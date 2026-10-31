"""`recipe-crawler probe`: check whether a site exposes crawlable schema.org recipes, without saving anything."""

from __future__ import annotations

import random

from .config import Settings, Site
from .crawler import Crawler, DomainWorker
from .db import DB
from .extract import extract_links, extract_recipes
from .net import MAX_SITEMAP_BYTES
from .sitemaps import FALLBACK_SITEMAPS, iter_sitemaps, keep_page_url, select_sitemaps


async def probe_site(settings: Settings, domain: str, samples: int, max_sitemaps: int) -> dict:
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
