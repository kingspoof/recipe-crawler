"""Command line interface.

    uv run recipe-crawler probe www.example.com      # test whether a site is crawlable
    uv run recipe-crawler crawl                      # discover + crawl all enabled sites
    uv run recipe-crawler crawl -d www.example.com   # just one site
    uv run recipe-crawler stats
    uv run recipe-crawler export recipes.jsonl --dedupe
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import random
import signal
import sys
from collections import Counter
from urllib.parse import urlsplit

from .config import Settings, Site, load_config
from .crawler import Crawler, DomainWorker
from .db import DB
from .extract import extract_links, extract_recipes
from .net import MAX_SITEMAP_BYTES
from .sitemaps import FALLBACK_SITEMAPS, iter_sitemaps, select_sitemaps, keep_page_url


def _selected_sites(settings: Settings, domains: list[str] | None) -> list[Site]:
    if domains:
        return [settings.site(d.lower()) for d in domains]
    return [s for s in settings.sites if s.enabled]


# --------------------------------------------------------------------------- crawl / discover


def cmd_crawl(settings: Settings, args: argparse.Namespace, discover_only: bool = False) -> None:
    if args.target:
        settings.target_total = args.target
    db = DB(settings.db_path)
    crawler = Crawler(settings, db)
    sites = _selected_sites(settings, args.domain)

    def stop(*_: object) -> None:
        if crawler.stopping:
            sys.exit(1)
        logging.warning("Stopping after in-flight requests finish (Ctrl+C again to force)...")
        crawler.stopping = True

    loop = asyncio.new_event_loop()
    loop.add_signal_handler(signal.SIGINT, stop)
    loop.add_signal_handler(signal.SIGTERM, stop)
    try:
        loop.run_until_complete(crawler.run(sites, discover_only, getattr(args, "force", False)))
    finally:
        loop.close()
        logging.info("Done. %d recipes in database.", db.recipe_count())
        db.close()


# --------------------------------------------------------------------------- probe


async def _probe(settings: Settings, domain: str, samples: int, max_sitemaps: int) -> dict:
    try:
        site = settings.site(domain)
    except KeyError:
        site = Site(domain=domain, country="", language="")
    db = DB(":memory:")  # type: ignore[arg-type]
    crawler = Crawler(settings, db)
    w = DomainWorker(crawler, site)
    w.throttle.base_delay = w.throttle.delay = min(w.throttle.delay, 1.0)
    report: dict = {"domain": domain, "ok": False}
    try:
        if not await w.setup():
            report["problem"] = "robots.txt unreachable (5xx / network error / blocked)"
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
        async for sm, found in iter_sitemaps(site, roots, fetch, max_sitemaps=max_sitemaps):
            sitemaps_seen.append((sm, len(found)))
            urls.extend(u for u in found if robots.allowed(u))
        report["sitemaps_sampled"] = sitemaps_seen
        report["urls_in_sampled_sitemaps"] = len(urls)
        if not urls:
            # No usable sitemap: sample links from the start page instead.
            report["mode"] = "links"
            for start in site.start_urls or [site.base_url + "/"]:
                r = await w._get(start)
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
                "url": url, "status": r.status,
                "recipe": recipes[0].name if recipes else None,
                "ingredients": len(recipes[0].ingredients) if recipes else 0,
                "steps": len(recipes[0].instructions) if recipes else 0,
                "language": recipes[0].language if recipes else None,
            })
        report["samples"] = tested
        hits = sum(1 for t in tested if t.get("recipe"))
        report["recipe_hit_rate"] = f"{hits}/{len(tested)}"
        report["ok"] = hits > 0
        if hits and report.get("mode") == "links" and not site.follow_links:
            report["problem"] = ("no recipe sitemaps; add this site with follow_links = true and a "
                                 "url_pattern matching the recipe URLs above")
    finally:
        await crawler.fetcher.aclose()
        db.close()
    return report


def cmd_probe(settings: Settings, args: argparse.Namespace) -> None:
    async def main():
        sem = asyncio.Semaphore(16)

        async def one(d):
            async with sem:
                return await _probe(settings, d.lower(), args.samples, args.max_sitemaps)

        return await asyncio.gather(*(one(d) for d in args.domains))

    reports = asyncio.run(main())
    if args.json:
        print(json.dumps(reports, ensure_ascii=False, indent=2))
        return
    for rep in reports:
        flag = "OK  " if rep["ok"] else "FAIL"
        print(f"\n[{flag}] {rep['domain']}  hit rate {rep.get('recipe_hit_rate', '-')}"
              f"  crawl-delay {rep.get('crawl_delay')}  discovery: {rep.get('mode', 'sitemaps')}")
        if rep.get("problem"):
            print(f"   problem: {rep['problem']}")
        for sm, n in rep.get("sitemaps_sampled", [])[:15]:
            print(f"   sitemap {sm}  ({n} urls kept)")
        for t in rep.get("samples", []):
            label = (f"{t['recipe']!r} ({t['ingredients']} ingr, {t['steps']} steps, {t['language']})"
                     if t.get("recipe") else f"no recipe (HTTP {t['status']})")
            print(f"   {urlsplit(t['url']).path[:70]:<70} {label}")


# --------------------------------------------------------------------------- stats / export


def cmd_stats(settings: Settings, args: argparse.Namespace) -> None:
    db = DB(settings.db_path)
    rows = db.stats()
    print(f"{'domain':<34}{'recipes':>9}{'pending':>10}{'done':>9}{'no_rec':>9}{'errors':>8}{'blocked':>9}")
    for r in rows:
        print(f"{r['domain']:<34}{r['recipes']:>9}{r['pending']:>10}{r['done']:>9}{r['no_recipe']:>9}"
              f"{r['errors']:>8}{r['blocked']:>9}")
    print(f"\nTOTAL recipes: {db.recipe_count()}")
    for col in ("site_region", "site_country", "language"):
        parts = ", ".join(f"{r['key']}: {r['n']}" for r in db.stats_by(col)[:30])
        print(f"by {col}: {parts}")
    db.close()


def _fingerprint(rec: dict) -> str:
    key = rec["name"].lower().strip() + "|" + "|".join(sorted(i.lower().strip() for i in rec["ingredients"]))
    return hashlib.sha1(key.encode()).hexdigest()


def cmd_export(settings: Settings, args: argparse.Namespace) -> None:
    db = DB(settings.db_path)
    seen: set[str] = set()
    n = dupes = 0
    langs: Counter[str] = Counter()
    with open(args.output, "w", encoding="utf-8") as f:
        for rec in db.iter_recipes(include_raw=args.raw):
            if args.require_instructions and not rec["instructions"]:
                continue
            if args.dedupe:
                fp = _fingerprint(rec)
                if fp in seen:
                    dupes += 1
                    continue
                seen.add(fp)
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1
            langs[(rec["language"] or "?")[:2].lower()] += 1
    print(f"Wrote {n} recipes to {args.output} ({dupes} duplicates skipped)")
    print("languages:", ", ".join(f"{k}: {v}" for k, v in langs.most_common(25)))
    db.close()


# --------------------------------------------------------------------------- main


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="recipe-crawler", description="Polite schema.org recipe crawler")
    p.add_argument("-c", "--config", default="config.toml")
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("probe", help="check whether sites expose crawlable schema.org recipes")
    sp.add_argument("domains", nargs="+")
    sp.add_argument("--samples", type=int, default=5, help="random pages to test per site")
    sp.add_argument("--max-sitemaps", type=int, default=3, help="sitemaps to read per site")
    sp.add_argument("--json", action="store_true")

    for name, help_ in (("discover", "only read sitemaps and fill the URL queue"),
                        ("crawl", "discover (if needed) and crawl recipes")):
        sp = sub.add_parser(name, help=help_)
        sp.add_argument("-d", "--domain", action="append", help="limit to this domain (repeatable)")
        sp.add_argument("--target", type=int, help="stop after this many recipes in total")
        if name == "discover":
            sp.add_argument("--force", action="store_true", help="re-read sitemaps even if done before")

    sub.add_parser("stats", help="show crawl progress")

    sp = sub.add_parser("export", help="export recipes as JSON Lines")
    sp.add_argument("output")
    sp.add_argument("--dedupe", action="store_true", help="drop recipes with same name + ingredients")
    sp.add_argument("--raw", action="store_true", help="include the original JSON-LD")
    sp.add_argument("--require-instructions", action="store_true")

    args = p.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    if args.cmd == "probe":
        logging.getLogger("crawler").setLevel(logging.ERROR)
    settings = load_config(args.config)

    if args.cmd == "probe":
        cmd_probe(settings, args)
    elif args.cmd == "discover":
        cmd_crawl(settings, args, discover_only=True)
    elif args.cmd == "crawl":
        cmd_crawl(settings, args)
    elif args.cmd == "stats":
        cmd_stats(settings, args)
    elif args.cmd == "export":
        cmd_export(settings, args)
