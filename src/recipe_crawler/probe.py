"""Probe a site: is it allowed by robots.txt, where do its URLs come from, do sample pages contain recipes.

Used by `recipe-crawler probe` (any domain, printed for a human) and by `scout --probe` (candidates, stored).
A probe reads robots.txt, at most `max_sitemaps` sitemaps (or the start page) and `samples` pages.
"""

from __future__ import annotations

import random
import re
from collections import Counter
from urllib.parse import urlsplit

from .config import Settings, Site
from .crawler import Crawler, DomainWorker
from .db import DB
from .extract import Recipe, extract_links, extract_recipes
from .net import MAX_SITEMAP_BYTES
from .sitemaps import FALLBACK_SITEMAPS, iter_sitemaps, keep_page_url, select_sitemaps

# A recipe whose ingredients are mostly these is a template/placeholder, not data.
_PLACEHOLDER_RE = re.compile(
    r"^\W*$|^\W*(ingredients?|zutaten|ingrédients?|ingredientes?|ingredienti|składniki|lorem.*|placeholder|"
    r"test|n/?a|none|null|undefined|\d+)\W*$",
    re.IGNORECASE,
)
_BLOCKED_HTTP = {401, 402, 403, 429, 451}


def recipe_problem(r: Recipe) -> str | None:
    """Why a recipe's structured data is unusable (how many past exclusions were found), or None."""
    ings = [i.strip() for i in r.ingredients if i and i.strip()]
    if len(ings) <= 1:
        return "0-1 ingredient lines"
    if sum(1 for i in ings if _PLACEHOLDER_RE.match(i)) * 2 >= len(ings):
        return "placeholder ingredients"
    if len({i.lower() for i in ings}) == 1:
        return "identical ingredient lines"
    return None


def suggest_url_pattern(host: str, urls: list[str]) -> str | None:
    """A url_pattern from the recipe pages found: their shared first path segment, e.g. ^https://host/rezept/."""
    segs = {urlsplit(u).path.strip("/").split("/")[0] for u in urls}
    if len(segs) != 1 or len(urls) < 2:
        return None
    seg = segs.pop()
    if not seg or len(seg) > 40:
        return None
    return f"^https://{re.escape(host)}/{re.escape(seg)}/"


def classify(report: dict) -> str:
    """Candidate status from a probe report: ok | links | blocked | no_recipes | unreachable | bad_data."""
    problem = report.get("problem") or ""
    if report.get("setup_failed"):
        return "blocked" if "bot blocking" in problem else "unreachable"
    if "disallows" in problem:
        return "blocked"
    samples = report.get("samples", [])
    hits = [t for t in samples if t.get("recipe")]
    if not hits:
        statuses = [t.get("status") for t in samples]
        if report.get("start_page_status") in _BLOCKED_HTTP:
            return "blocked"
        if samples and all(s in _BLOCKED_HTTP for s in statuses):
            return "blocked"
        if (samples and all(s == "network error" for s in statuses)) or report.get("start_page_status") == "network error":
            return "unreachable"
        return "no_recipes"
    if sum(1 for t in hits if t.get("data_problem")) * 2 >= len(hits):
        return "bad_data"
    return "links" if report.get("mode") == "links" else "ok"


async def probe_site(settings: Settings, domain: str, samples: int, max_sitemaps: int) -> dict:
    """Probe one domain; the report's "status" is the classify() result."""
    report = await _probe(settings, domain, samples, max_sitemaps)
    report["status"] = classify(report)
    report["ok"] = report["status"] in ("ok", "links")
    return report


async def _probe(settings: Settings, domain: str, samples: int, max_sitemaps: int) -> dict:
    try:
        site = settings.site(domain)
    except KeyError:
        site = Site(domain=domain, country="", language="")
    db = DB(":memory:")  # type: ignore[arg-type]
    crawler = Crawler(settings, db)
    crawler.links = None  # a probe must not record links
    w = DomainWorker(crawler, site)
    w.throttle.base_delay = w.throttle.delay = min(w.throttle.delay, 1.0)
    report: dict = {"domain": domain, "ok": False, "mode": "sitemaps"}
    try:
        if not await w.setup():
            report["setup_failed"] = True
            report["problem"] = w.setup_problem or "robots.txt unreachable (5xx / network error / blocked)"
            return report
        robots = w.robots
        assert robots is not None
        report["robots_sitemaps"] = robots.sitemaps
        report["crawl_delay"] = robots.crawl_delay
        if not robots.allowed(site.base_url + "/"):
            report["problem"] = "robots.txt disallows our user agent for the whole site"
            return report
        roots = site.sitemaps or select_sitemaps(site, robots.sitemaps) or [
            site.base_url + p for p in FALLBACK_SITEMAPS
        ]

        async def fetch(url):
            return await w._get(url, MAX_SITEMAP_BYTES) if robots.allowed(url) else None

        sitemaps_seen: list[tuple[str, int]] = []
        urls: list[str] = []
        walk: dict = {}
        async for sm, found in iter_sitemaps(site, roots, fetch, max_sitemaps=max_sitemaps, stats=walk):
            sitemaps_seen.append((sm, len(found)))
            urls.extend(u for u in found if robots.allowed(u))
        report["sitemaps_sampled"] = sitemaps_seen
        report["urls_in_sampled_sitemaps"] = len(urls)
        if urls:
            # Rough size: URLs per sampled urlset x (sampled + still queued sitemaps). Recipe sitemaps are
            # usually listed together, so this is the right order of magnitude, not more.
            report["est_urls"] = round(len(urls) / len(sitemaps_seen) * (len(sitemaps_seen) + walk.get("unread", 0)))
        else:
            # No usable sitemap: sample links from the start page instead.
            report["mode"] = "links"
            for start in site.start_urls or [site.base_url + "/"]:
                r = await w._get(start)
                report.setdefault("start_page_status", r.status if r is not None else "network error")
                if r is not None and r.status == 200:
                    pattern = site.url_pattern
                    urls.extend(
                        u for u in extract_links(r.text(), r.url)
                        if robots.allowed(u) and (pattern.search(u) if pattern else keep_page_url(site, u))
                    )
            if not urls:
                report["problem"] = "no URLs found in sitemaps or on the start page"
                return report
        tested = []
        for url in random.sample(urls, min(samples, len(urls))):
            r = await w._get(url)
            if r is None or r.status != 200:
                tested.append({"url": url, "status": r.status if r else "network error"})
                continue
            recipes = extract_recipes(r.text(), url)
            tested.append({
                "url": url, "final_url": r.url, "status": r.status,
                "recipe": recipes[0].name if recipes else None,
                "ingredients": len(recipes[0].ingredients) if recipes else 0,
                "steps": len(recipes[0].instructions) if recipes else 0,
                "language": recipes[0].language if recipes else None,
                "data_problem": recipe_problem(recipes[0]) if recipes else None,
            })
        report["samples"] = tested
        hits = [t for t in tested if t.get("recipe")]
        report["recipe_hit_rate"] = f"{len(hits)}/{len(tested)}"
        report["hit_rate"] = len(hits) / len(tested) if tested else 0.0
        langs = Counter((t["language"] or "").replace("_", "-").split("-")[0].lower() for t in hits if t["language"])
        report["language"] = langs.most_common(1)[0][0] if langs else None
        # The host the recipe pages are really on (e.g. "example.com" redirects to "www.example.com").
        hosts = Counter((urlsplit(t["final_url"]).hostname or "").lower() for t in hits)
        report["host"] = hosts.most_common(1)[0][0] if hosts else domain
        report["url_pattern"] = suggest_url_pattern(report["host"], [t["final_url"] for t in hits])
        if hits and report["mode"] == "links" and not site.follow_links:
            report["problem"] = ("no recipe sitemaps; add this site with follow_links = true and a "
                                 "url_pattern matching the recipe URLs above")
        bad = [t for t in hits if t.get("data_problem")]
        if bad and len(bad) * 2 >= len(hits):
            report["problem"] = f"bad structured data: {bad[0]['data_problem']} ({len(bad)}/{len(hits)} recipes)"
    finally:
        await crawler.fetcher.aclose()
        db.close()
    return report
