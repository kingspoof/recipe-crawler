"""Site discovery (`recipe-crawler scout`): find candidate recipe sites, probe them, propose config lines.

Candidates come from
  1. external links on pages the crawler already fetched (counted during `crawl`, no extra requests),
  2. a Web Data Commons schema.org Recipe domain list, downloaded by you (`--import-wdc FILE`).
Before probing, configured, excluded ([excluded] in config.toml) and obvious non-recipe domains are dropped.
`--probe N` probes at most N candidates politely (few samples, same User-Agent).

It NEVER enables a site: `--report` prints config lines for you to review (terms of use, data quality) and
paste into config.toml yourself.
"""

from __future__ import annotations

import asyncio
import csv
import gzip
import io
import json
import logging
import math
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlsplit

from .config import Settings

log = logging.getLogger("scout")

FAILED = ("blocked", "no_recipes", "unreachable", "bad_data")
GOOD = ("ok", "links")

# --------------------------------------------------------------------------- domains


def bare(host: str) -> str:
    return host.strip().lower().rstrip(".").removeprefix("www.")


def _suffixes(host: str) -> Iterable[str]:
    """"a.b.example.com" -> "a.b.example.com", "b.example.com", "example.com", "com"."""
    parts = bare(host).split(".")
    for i in range(len(parts)):
        yield ".".join(parts[i:])


def matches(host: str, domains: set[str]) -> bool:
    """host is one of `domains` (bare) or a subdomain of one."""
    return any(s in domains for s in _suffixes(host))


# Social media, CDNs, shops, trackers, link shorteners, platforms, big sites: never recipe-site candidates.
# Matched as the domain itself or any subdomain (so "cdn.shopify.com" is dropped).
NON_RECIPE_DOMAINS = {
    # social / video / messaging
    "facebook.com", "fb.com", "fb.me", "instagram.com", "twitter.com", "x.com", "t.co", "pinterest.com",
    "pin.it", "youtube.com", "youtu.be", "tiktok.com", "linkedin.com", "reddit.com", "snapchat.com",
    "whatsapp.com", "wa.me", "t.me", "telegram.me", "telegram.org", "vk.com", "ok.ru", "weibo.com", "line.me",
    "threads.net", "bsky.app", "mastodon.social", "flickr.com", "vimeo.com", "dailymotion.com", "twitch.tv",
    "discord.gg", "discord.com", "messenger.com", "bilibili.com", "douyin.com", "kakao.com", "naver.me",
    "flipboard.com", "yummly.com", "bloglovin.com", "patreon.com",
    # search / big tech / reference
    "google.com", "goo.gl", "g.co", "bing.com", "yahoo.com", "yandex.ru", "baidu.com", "apple.com", "microsoft.com",
    "wikipedia.org", "wikimedia.org", "wikidata.org", "archive.org", "github.com", "mozilla.org", "w3.org",
    "schema.org", "creativecommons.org", "openstreetmap.org",
    # CDNs / assets / analytics / ads
    "cloudfront.net", "akamaihd.net", "akamaized.net", "cloudflare.com", "gstatic.com", "googleapis.com",
    "googleusercontent.com", "googletagmanager.com", "google-analytics.com", "doubleclick.net",
    "googlesyndication.com", "jsdelivr.net", "unpkg.com", "fbcdn.net", "wp.com", "gravatar.com", "amazonaws.com",
    "azureedge.net", "fastly.net", "imgix.net", "cloudinary.com", "addthis.com", "sharethis.com", "disqus.com",
    "mailchimp.com", "list-manage.com", "feedburner.com", "typekit.net", "fonts.com", "adobe.com", "hotjar.com",
    "outbrain.com", "taboola.com", "mediavine.com", "raptive.com", "adthrive.com", "criteo.com", "statcounter.com",
    "onetrust.com", "cookiebot.com", "recaptcha.net", "hubspot.com", "issuu.com", "scribd.com", "dropbox.com",
    # shops / payments / affiliate
    "amazon.com", "amzn.to", "amzn.eu", "ebay.com", "etsy.com", "aliexpress.com", "alibaba.com", "walmart.com",
    "target.com", "shopify.com", "myshopify.com", "paypal.com", "paypal.me", "stripe.com", "booking.com",
    "tripadvisor.com", "rakuten.co.jp", "shopee.com", "lazada.com", "tokopedia.com", "flipkart.com",
    "instacart.com", "skimresources.com", "shareasale.com", "awin1.com", "rstyle.me", "ltk.com", "liketoknow.it",
    # link-in-bio / shorteners / hosting platforms (bare)
    "bit.ly", "tinyurl.com", "ow.ly", "buff.ly", "linktr.ee", "linkin.bio", "lnk.to", "wordpress.org",
    "blogger.com", "wix.com", "squarespace.com", "godaddy.com", "jetpack.com", "wpengine.com", "elementor.com",
    "tasty.co", "wprm.io", "app.link",
}
# Blog platforms: the platform itself is dropped, the blogs on it ("rezepte.blogspot.com") are kept.
NON_RECIPE_EXACT = {"blogspot.com", "wordpress.com", "wixsite.com", "weebly.com", "tumblr.com", "substack.com",
                    "medium.com"}
# Brands with a domain in many TLDs (amazon.de, google.fr, ...): dropped when any label matches exactly.
NON_RECIPE_LABELS = {"google", "amazon", "facebook", "instagram", "youtube", "pinterest", "ebay", "yahoo",
                     "bing", "apple", "microsoft", "wikipedia", "twitter", "tiktok", "linkedin", "paypal"}


def is_non_recipe(host: str) -> bool:
    h = bare(host)
    if h in NON_RECIPE_EXACT or matches(h, NON_RECIPE_DOMAINS):
        return True
    labels = h.split(".")
    if labels[0] in ("cdn", "static", "img", "images", "assets", "media", "fonts", "api", "ads", "shop", "store"):
        return True
    return any(label in NON_RECIPE_LABELS for label in labels[:-1])


_HOST_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?)+$")


def clean_host(value: str) -> str | None:
    """A lowercase hostname from "Example.com", "https://www.example.com/x", ... or None."""
    v = value.strip().strip('"').lower()
    if "//" in v:
        v = urlsplit(v if "://" in v else "http:" + v).hostname or ""
    v = v.split("/")[0].split(":")[0].rstrip(".")
    if not _HOST_RE.match(v) or v.replace(".", "").isdigit() or not re.search(r"[a-z]", v.rsplit(".", 1)[-1]):
        return None
    return v


class Filter:
    """Drops configured, excluded and non-recipe hosts. Cached: called for every external link of every page."""

    def __init__(self, settings: Settings):
        self.configured = {bare(s.domain) for s in settings.sites}
        self.excluded = {bare(e.domain) for e in settings.excluded}
        self._cache: dict[str, str | None] = {}

    def reason(self, host: str) -> str | None:
        """Why this host is not a candidate, or None if it is one."""
        if host not in self._cache:
            if len(self._cache) > 200_000:
                self._cache.clear()
            r = None
            if matches(host, self.configured):
                r = "configured"
            elif matches(host, self.excluded):
                r = "excluded"
            elif is_non_recipe(host):
                r = "non-recipe"
            self._cache[host] = r
        return self._cache[host]

    def keep(self, host: str) -> bool:
        return self.reason(host) is None


# --------------------------------------------------------------------------- 1. links seen while crawling

# Only absolute (or protocol-relative) links can point to another site; just the host is captured.
_EXT_HREF_RE = re.compile(r"""<a\s[^>]*?href\s*=\s*["']?(?:https?:)?//([^/"'\s?#:>\\]+)""", re.IGNORECASE)


def external_hosts(page: str, own_domain: str) -> set[str]:
    """Distinct hosts of other sites linked from this page (lowercase, as written)."""
    own = bare(own_domain)
    out = set()
    for raw in _EXT_HREF_RE.findall(page):
        h = clean_host(raw)
        if h and bare(h) != own and not bare(h).endswith("." + own):
            out.add(h)
    return out


class LinkCollector:
    """Counts external hosts per crawled site in memory and writes them to candidate_links in batches.

    Each host counts once per page. Writing never raises: on a DB error the counts are kept for the next try.
    """

    MAX_PENDING_KEYS = 500_000

    def __init__(self, db, settings: Settings, flush_every: int = 2000):
        self.db = db
        self.filter = Filter(settings)
        self.flush_every = flush_every
        self.pending: Counter[tuple[str, str]] = Counter()
        self.pages = 0

    def add(self, from_domain: str, hosts: Iterable[str]) -> None:
        for h in hosts:
            if self.filter.keep(h):
                self.pending[h, from_domain] += 1
        self.pages += 1
        if self.pages >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        self.pages = 0
        if not self.pending:
            return
        rows, now = list(self.pending.items()), time.time()
        conn = self.db.conn
        if conn.in_transaction:  # someone else's transaction (see #4): never commit or roll back their writes
            log.warning("transaction already open; candidate links kept for the next try")
            return
        try:
            conn.execute("BEGIN")
            conn.executemany(
                """INSERT INTO candidate_links(host, from_domain, pages, first_seen, last_seen) VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(host, from_domain) DO UPDATE SET pages = pages + excluded.pages,
                   last_seen = excluded.last_seen""",
                ((h, d, n, now, now) for (h, d), n in rows),
            )
            conn.execute("COMMIT")
            self.pending.clear()
        except Exception as e:  # never let link counting break a crawl
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            log.warning("could not save %d candidate links (kept for the next try): %s", len(rows), e)
            if len(self.pending) > self.MAX_PENDING_KEYS:
                log.warning("dropping %d unsaved candidate links", len(self.pending))
                self.pending.clear()


def _upsert(conn, domain: str, host: str, source: str, **fields) -> bool:
    """Insert a candidate or update it (adds `source`, sets the given fields). Returns True if it was new."""
    row = conn.execute("SELECT source FROM site_candidates WHERE domain = ?", (domain,)).fetchone()
    if row is None:
        cols = ["domain", "host", "source", "added_at", *fields]
        conn.execute(f"INSERT INTO site_candidates({', '.join(cols)}) VALUES ({', '.join('?' * len(cols))})",
                     (domain, host, source, time.time(), *fields.values()))
        return True
    sources = row["source"].split(",")
    if source not in sources:
        sources.append(source)
    sets = ", ".join(f"{k} = ?" for k in fields)
    conn.execute(f"UPDATE site_candidates SET source = ?{', ' + sets if sets else ''} WHERE domain = ?",
                 (",".join(sources), *fields.values(), domain))
    return False


def from_links(db, settings: Settings, min_sites: int | None = None) -> tuple[int, int]:
    """Turn candidate_links into candidates: hosts linked from at least `min_sites` crawled sites.

    Returns (new, updated). Counts are recomputed from candidate_links, so running it again is safe.
    """
    min_sites = settings.scout.min_linked_sites if min_sites is None else min_sites
    flt = Filter(settings)  # again: config.toml may have changed since the links were counted
    pages: Counter[str] = Counter()
    sites: defaultdict[str, set[str]] = defaultdict(set)
    host_pages: defaultdict[str, Counter[str]] = defaultdict(Counter)
    for r in db.conn.execute("SELECT host, from_domain, pages FROM candidate_links"):
        if not flt.keep(r["host"]):
            continue
        d = bare(r["host"])
        pages[d] += r["pages"]
        sites[d].add(r["from_domain"])
        host_pages[d][r["host"]] += r["pages"]
    new = updated = 0
    db.conn.execute("BEGIN")
    try:
        for d, linked in sites.items():
            if len(linked) < min_sites:
                continue
            host = host_pages[d].most_common(1)[0][0]
            if _upsert(db.conn, d, host, "links", seen_count=pages[d], linked_sites=len(linked)):
                new += 1
            else:
                updated += 1
        db.conn.execute("COMMIT")
    except BaseException:
        db.conn.execute("ROLLBACK")
        raise
    return new, updated


# --------------------------------------------------------------------------- 2. Web Data Commons

_COUNT_NAMES = ("entit", "count", "quads", "items", "urls", "pages", "triples")
_DOMAIN_NAMES = ("domain", "host", "pld", "site", "tld")


def _int(v: str) -> int | None:
    v = v.strip().replace("_", "")
    return int(v) if v.isdigit() else None


def parse_wdc(text: str) -> list[tuple[str, int | None]]:
    """(host, entity count) pairs from a WDC domain list: CSV/TSV with or without header, or one domain per line.

    With a header, the domain column is the one named like domain/host/pld and the count column the one named
    like entities/count/quads. Without one, the first cell that looks like a host and the first number after it.
    """
    lines = [ln for ln in text.splitlines() if ln.strip() and not ln.lstrip().startswith("#")]
    if not lines:
        return []
    first = lines[0]
    if "\t" in first or "," in first or ";" in first:
        delim = "\t" if "\t" in first else ";" if first.count(";") > first.count(",") else ","
        rows = list(csv.reader(io.StringIO("\n".join(lines)), delimiter=delim))
    else:
        rows = [ln.split() for ln in lines]
    dom_col = cnt_col = None
    head = [c.strip().lower() for c in rows[0]]
    if not any(clean_host(c) for c in rows[0]):  # header row
        dom_col = next((i for i, c in enumerate(head) if any(n in c for n in _DOMAIN_NAMES)), None)
        cnt_col = next((i for n in _COUNT_NAMES for i, c in enumerate(head) if n in c and i != dom_col), None)
        rows = rows[1:]
    out = []
    for row in rows:
        if dom_col is not None:
            host = clean_host(row[dom_col]) if dom_col < len(row) else None
            count = _int(row[cnt_col]) if cnt_col is not None and cnt_col < len(row) else None
        else:
            idx = next((i for i, c in enumerate(row) if clean_host(c)), None)
            if idx is None:
                continue
            host = clean_host(row[idx])
            count = next((n for c in row[idx + 1:] if (n := _int(c)) is not None), None)
        if host:
            out.append((host, count))
    return out


def import_wdc(db, settings: Settings, path: Path) -> dict[str, int]:
    """Add the domains of a downloaded WDC Recipe domain list (.csv/.tsv/.txt, optionally .gz) as candidates."""
    raw = path.read_bytes()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    entries = parse_wdc(raw.decode("utf-8", errors="replace"))
    flt = Filter(settings)
    counts: Counter[str] = Counter()
    best: dict[str, tuple[str, int | None]] = {}
    for host, n in entries:
        why = flt.reason(host)
        if why:
            counts[why] += 1
            continue
        d = bare(host)
        if d not in best or (n or 0) > (best[d][1] or 0):
            best[d] = (host, n)
    db.conn.execute("BEGIN")
    try:
        for d, (host, n) in best.items():
            fields = {"wdc_entities": n} if n is not None else {}
            counts["new" if _upsert(db.conn, d, host, "wdc", **fields) else "updated"] += 1
        db.conn.execute("COMMIT")
    except BaseException:
        db.conn.execute("ROLLBACK")
        raise
    counts["read"] = len(entries)
    return dict(counts)


# --------------------------------------------------------------------------- gaps and guesses

# Regions as used in config.toml, by ISO country code.
COUNTRY_REGION = {
    **dict.fromkeys("AD AL AM AT AZ BA BE BG BY CH CY CZ DE DK EE ES FI FO FR GB GE GR HR HU IE IS IT LI LT LU "
                    "LV MC MD ME MK MT NL NO PL PT RO RS RU SE SI SK SM UA VA XK".split(), "Europe"),
    **dict.fromkeys("AE BH DZ EG IL IQ IR JO KW LB LY MA OM PS QA SA SY TN TR YE".split(),
                    "Middle East & North Africa"),
    **dict.fromkeys("KG KZ TJ TM UZ MN AF".split(), "Central Asia"),
    **dict.fromkeys("CN HK JP KR MO TW KP".split(), "East Asia"),
    **dict.fromkeys("BD BT IN LK MV NP PK".split(), "South Asia"),
    **dict.fromkeys("BN ID KH LA MM MY PH SG TH TL VN".split(), "Southeast Asia"),
    **dict.fromkeys("AO BF BI BJ BW CD CF CG CI CM CV DJ ER ET GA GH GM GN GQ GW KE KM LR LS MG ML MR MU MW "
                    "MZ NA NE NG RE RW SC SD SL SN SO SS ST SZ TD TG TZ UG ZA ZM ZW".split(), "Sub-Saharan Africa"),
    **dict.fromkeys("AR BO BR BZ CL CO CR CU DO EC GT GY HN HT JM MX NI PA PE PR PY SR SV TT UY VE BB BS LC "
                    "GD VC AG DM KN".split(), "Latin America"),
    **dict.fromkeys("US CA".split(), "North America"),
    **dict.fromkeys("AU NZ FJ PG WS TO".split(), "Oceania"),
}
# ccTLDs mostly used as generic TLDs: they say nothing about the country.
_GENERIC_CCTLDS = {"io", "co", "tv", "me", "ai", "fm", "ly", "to", "cc", "ws", "gg", "la", "nu", "tk", "ml", "ga",
                   "cf", "gq", "su", "am", "vc", "sh", "ac"}
_TLD_COUNTRY_EXTRA = {"uk": "GB"}
# Languages spoken (for recipe sites) in essentially one country.
LANGUAGE_COUNTRY = {
    "ja": "JP", "ko": "KR", "th": "TH", "vi": "VN", "id": "ID", "he": "IL", "fa": "IR", "tr": "TR", "el": "GR",
    "hu": "HU", "cs": "CZ", "sk": "SK", "pl": "PL", "ro": "RO", "bg": "BG", "hr": "HR", "sl": "SI", "sr": "RS",
    "uk": "UA", "lt": "LT", "lv": "LV", "et": "EE", "fi": "FI", "sv": "SE", "da": "DK", "nb": "NO", "no": "NO",
    "nn": "NO", "is": "IS", "ka": "GE", "kk": "KZ", "uz": "UZ", "si": "LK", "ne": "NP", "km": "KH", "lo": "LA",
    "my": "MM", "am": "ET", "mn": "MN", "hy": "AM", "az": "AZ", "sq": "AL", "mk": "MK", "tl": "PH", "fil": "PH",
}


@dataclass
class Guess:
    country: str  # "" if unknown
    region: str
    why: str  # shown in the report, so you know what to check


def guess_location(host: str, language: str | None) -> Guess:
    tld = bare(host).rsplit(".", 1)[-1]
    cc = _TLD_COUNTRY_EXTRA.get(tld) or (tld.upper() if len(tld) == 2 and tld not in _GENERIC_CCTLDS else "")
    if cc and cc in COUNTRY_REGION:
        return Guess(cc, COUNTRY_REGION[cc], f"country from .{tld}")
    lc = LANGUAGE_COUNTRY.get((language or "").lower())
    if lc:
        return Guess(lc, COUNTRY_REGION.get(lc, ""), f"country from language {language}")
    return Guess("", "", "country and region unknown")


class Gaps:
    """How thin the dataset is per language and region, from the recipes table and max_recipes_per_region."""

    LANG_SCALE = 20_000  # a language with this many recipes gets half the weight of an empty one

    def __init__(self, db, settings: Settings):
        self.cap = settings.max_recipes_per_region
        self.region: Counter[str] = Counter()
        self.language: Counter[str] = Counter()
        # One scan of the (large) recipes table for both counts.
        for region, language, n in db.conn.execute(
                "SELECT site_region, language, COUNT(*) FROM recipes GROUP BY site_region, language"):
            self.region[region] += n
            self.language[language] += n
        self.sites_per_language = Counter(s.language for s in settings.sites if s.enabled)

    def weight(self, language: str | None, region: str) -> float:
        """1 = big gap, 0 = region full. Unknown language or region counts as a medium gap."""
        if region and self.cap and self.region.get(region, 0) >= self.cap:
            return 0.0
        w = 1 / (1 + self.language.get(language, 0) / self.LANG_SCALE) if language else 0.5
        if region and self.cap:
            w *= 1 - 0.5 * self.region.get(region, 0) / self.cap
        return w

    def note(self, language: str | None, region: str) -> str:
        parts = []
        if language:
            parts.append(f"{language}: {self.language.get(language, 0):,} recipes, "
                         f"{self.sites_per_language.get(language, 0)} sites")
        if region:
            n = self.region.get(region, 0)
            parts.append(f"{region}: {n:,}" + (f" ({n / self.cap:.0%} of cap)" if self.cap else ""))
        return "; ".join(parts) or "language unknown"


def _popularity(row) -> float:
    return math.log1p(row["wdc_entities"] or 0) + 2 * math.log1p(row["linked_sites"] or 0)


# --------------------------------------------------------------------------- 3. probing


def to_probe(db, settings: Settings, n: int) -> list:
    """Up to n candidates to probe: never probed, or failed long enough ago. Gaps and popularity first."""
    flt = Filter(settings)
    gaps = Gaps(db, settings)
    stale = time.time() - settings.scout.reprobe_after_days * 86400
    rows = db.conn.execute(
        f"""SELECT * FROM site_candidates
            WHERE status = 'new' OR (status IN ({', '.join('?' * len(FAILED))}) AND probed_at < ?)""",
        (*FAILED, stale),
    ).fetchall()
    rows = [r for r in rows if flt.keep(r["host"])]

    def priority(r) -> tuple:
        g = guess_location(r["host"], r["language"])
        return (r["status"] == "new", gaps.weight(r["language"], g.region) * (1 + _popularity(r)))

    rows.sort(key=priority, reverse=True)
    return rows[:n]


def save_probe(db, domain: str, report: dict) -> None:
    status = report["status"]
    notes = report.get("problem") or ""
    if status == "ok" and report.get("hit_rate", 1) < 0.6:
        notes = (notes + "; " if notes else "") + "low hit rate: consider a url_pattern"
    db.conn.execute(
        """UPDATE site_candidates SET status = ?, hit_rate = ?, est_urls = ?, language = ?, mode = ?,
           url_pattern = ?, probed_at = ?, notes = ?, host = COALESCE(?, host) WHERE domain = ?""",
        (status, report.get("hit_rate"), report.get("est_urls"), report.get("language"), report.get("mode"),
         report.get("url_pattern"), time.time(), notes or None,
         report.get("host") if status in GOOD else None, domain),
    )


async def probe_candidates(db, settings: Settings, n: int) -> Counter[str]:
    """Probe at most n candidates, `scout.parallel` at a time; each result is saved as soon as it is in."""
    from .probe import probe_site  # probe -> crawler -> db; imported here to keep `scout` light

    rows = to_probe(db, settings, n)
    sem = asyncio.Semaphore(max(1, settings.scout.parallel))
    results: Counter[str] = Counter()

    async def one(row) -> None:
        async with sem:
            try:
                report = await probe_site(settings, row["host"], settings.scout.samples, max_sitemaps=3)
            except Exception as e:  # one broken site must not end the run
                log.warning("%s: probe crashed: %s", row["host"], e)
                report = {"status": "unreachable", "problem": f"probe crashed: {type(e).__name__}: {e}"}
            save_probe(db, row["domain"], report)
            results[report["status"]] += 1
            log.info("%-40s %-12s hit rate %s  %s", row["host"], report["status"],
                     report.get("recipe_hit_rate", "-"), report.get("problem") or "")

    log.info("probing %d candidate(s), %d at a time", len(rows), settings.scout.parallel)
    await asyncio.gather(*(one(r) for r in rows))
    return results


# --------------------------------------------------------------------------- 4. report


def _toml(s: str) -> str:
    return json.dumps(s, ensure_ascii=False)  # a JSON string is a valid TOML basic string


def config_line(row, guess: Guess) -> str:
    parts = [f"domain = {_toml(row['host'])}", f"country = {_toml(guess.country)}",
             f"language = {_toml(row['language'] or '')}", f"region = {_toml(guess.region)}"]
    line = "{ " + ", ".join(parts)
    if row["status"] == "links":
        if not row["url_pattern"]:
            return "# " + line + ", follow_links = true, url_pattern = ??? },  # CHECK: needs a url_pattern"
        line += f", follow_links = true, url_pattern = {_toml(row['url_pattern'])}"
    return line + " },"


def report(db, settings: Settings, limit: int = 50) -> list[str]:
    """Ready-to-paste config.toml lines for the good candidates, plus [excluded] lines for the failed ones."""
    flt = Filter(settings)
    gaps = Gaps(db, settings)
    good = []
    for r in db.conn.execute(f"SELECT * FROM site_candidates WHERE status IN ({', '.join('?' * len(GOOD))})", GOOD):
        if not flt.keep(r["host"]):
            continue  # added to config.toml or [excluded] since the probe
        g = guess_location(r["host"], r["language"])
        weight = gaps.weight(r["language"], g.region)
        expected = (r["est_urls"] or 500) * (r["hit_rate"] or 0)  # links mode: size unknown, assume 500
        good.append((expected * weight, expected, r, g))
    good.sort(key=lambda t: t[0], reverse=True)
    today = time.strftime("%Y-%m-%d")
    out = [
        f"# scout report {today}: {len(good)} candidate(s), best first (expected recipes x gap priority).",
        "# NOT enabled. Before pasting a line into [seeds] in config.toml: read the site's terms of use, look at",
        "# a few recipes, and check country / language / region (they are guesses).",
        "",
    ]
    for _, expected, r, g in good[:limit]:
        size = f"~{r['est_urls']:,} URLs" if r["est_urls"] else "size unknown (links mode)"
        src = ", ".join(filter(None, [
            f"linked from {r['linked_sites']} sites" if r["linked_sites"] else "",
            f"WDC {r['wdc_entities']:,} recipes" if r["wdc_entities"] else ""]))
        out.append(f"# {r['host']}: {r['status']}, hit rate {r['hit_rate'] or 0:.0%}, {size}, ~{expected:,.0f} recipes"
                   f"{'; ' + src if src else ''}")
        out.append(f"#   gap: {gaps.note(r['language'], g.region)} | {g.why}"
                   + (f" | {r['notes']}" if r["notes"] else ""))
        out.append(config_line(r, g))
        out.append("")
    if len(good) > limit:
        out.append(f"# ... {len(good) - limit} more (--limit)")
        out.append("")
    failed = [r for r in db.conn.execute(
        "SELECT * FROM site_candidates WHERE status IN ('blocked', 'no_recipes', 'bad_data') ORDER BY status, domain")
        if flt.keep(r["host"])]
    if failed:
        out.append(f"# {len(failed)} rejected candidate(s). Paste into [excluded] so they are never probed again")
        out.append("# (unreachable ones are retried after reprobe_after_days instead):")
        for r in failed:
            date = time.strftime("%Y-%m-%d", time.localtime(r["probed_at"])) if r["probed_at"] else today
            out.append(f"  {{ domain = {_toml(r['domain'])}, status = {_toml(r['status'])}, "
                       f"note = {_toml(r['notes'] or '')}, date = {_toml(date)} }},")
    return out


def summary(db) -> list[str]:
    rows = db.conn.execute("SELECT status, COUNT(*) AS n FROM site_candidates GROUP BY status ORDER BY n DESC").fetchall()
    links = db.conn.execute("SELECT COUNT(DISTINCT host) FROM candidate_links").fetchone()[0]
    total = sum(r["n"] for r in rows)
    parts = ", ".join(f"{r['status']}: {r['n']}" for r in rows)
    return [f"site candidates: {total}" + (f" ({parts})" if parts else ""),
            f"external hosts seen while crawling: {links:,}"]
