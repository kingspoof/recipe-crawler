"""The crawl loop: one polite sequential worker per domain, many domains in parallel."""

from __future__ import annotations

import asyncio
import itertools
import logging
import time
from collections import defaultdict

import httpx

from .config import Settings, Site
from .db import DB
from .extract import extract_links, extract_recipes
from .net import MAX_SITEMAP_BYTES, Fetcher, Response, Robots, Throttle, TooLarge, fetch_robots
from .sitemaps import FALLBACK_SITEMAPS, iter_sitemaps, select_sitemaps, keep_followed_link, keep_page_url

log = logging.getLogger("crawler")

BATCH = 200
# Stop crawling a domain for this run after this many consecutive failures (blocked, down, ...).
MAX_CONSECUTIVE_FAILURES = 25
# Stop crawling a domain if, after this many pages, almost none contained a recipe.
YIELD_CHECK_AFTER = 300
MIN_RECIPE_YIELD = 0.05


class DomainWorker:
    def __init__(self, crawler: "Crawler", site: Site):
        self.c = crawler
        self.site = site
        self.robots: Robots | None = None
        self.throttle = Throttle(site.delay or crawler.settings.default_delay)
        self.consecutive_failures = 0
        self.pages = 0
        self.recipes = 0
        self.log = logging.getLogger(f"crawler.{site.domain}")

    @property
    def db(self) -> DB:
        return self.c.db

    async def _get(self, url: str, max_bytes: int | None = None) -> Response | None:
        """Polite GET: waits for the throttle, retries nothing, returns None on network errors."""
        await self.throttle.wait()
        try:
            r = await self.c.fetcher.get(url, max_bytes) if max_bytes else await self.c.fetcher.get(url)
        except (httpx.HTTPError, httpx.InvalidURL, TooLarge) as e:  # InvalidURL is not an HTTPError
            self.throttle.done()
            self.consecutive_failures += 1
            self.log.debug("GET %s failed: %s", url, e)
            return None
        if r.status in (429, 503):
            self.throttle.backoff(r.retry_after())
            self.consecutive_failures += 1
            self.log.warning("HTTP %s, backing off to %.0fs delay", r.status, self.throttle.delay)
        else:
            self.throttle.done(r.elapsed)
        return r

    async def setup(self) -> bool:
        await self.throttle.wait()
        self.robots, problem = await fetch_robots(self.c.fetcher, self.site.base_url)
        self.throttle.done()
        if problem:
            self.log.warning("skipping domain: %s", problem)
            return False
        if self.robots.crawl_delay:
            self.throttle = Throttle(max(self.throttle.base_delay, min(self.robots.crawl_delay, 60)))
        return True

    async def discover(self) -> None:
        assert self.robots is not None
        if self.site.sitemaps:
            roots = self.site.sitemaps
        else:
            roots = select_sitemaps(self.site, self.robots.sitemaps) or [
                self.site.base_url + p for p in FALLBACK_SITEMAPS
            ]
        sitemaps_read = urls_found = 0
        if self.site.follow_links:
            starts = self.site.start_urls or [self.site.base_url + "/"]
            urls_found += self.db.add_urls(self.site.domain, [u for u in starts if self.robots.allowed(u)])
        self.log.info("discovering URLs from %d sitemap root(s)", len(roots))

        failed: list[str] = []

        async def fetch(url: str) -> Response | None:
            if not self.robots.allowed(url):
                return None
            r = await self._get(url, MAX_SITEMAP_BYTES)
            if r is None or r.status == 429 or r.status >= 500:  # temporary: try again next run
                failed.append(url)
            return r

        async for sm, urls in iter_sitemaps(self.site, roots, fetch):
            sitemaps_read += 1
            urls = [u for u in urls if self.robots.allowed(u)]
            added = self.db.add_urls(self.site.domain, urls)
            urls_found += added
            if sitemaps_read % 25 == 0:
                self.log.info("... %d sitemaps read, %d URLs queued", sitemaps_read, urls_found)
            if self.c.stopping:
                return  # not marked as discovered, so it resumes next time
        if self.c.stopping:
            return
        if failed:
            # Not marked as discovered, so the next run reads the sitemaps again (the URLs already queued
            # are ignored then). Marking it would hide the missing sitemaps until `discover --force`.
            self.log.warning("discovery incomplete: %d sitemap(s) failed (e.g. %s), %d sitemaps read, %d URLs "
                             "queued; retrying next run", len(failed), failed[0], sitemaps_read, urls_found)
            return
        self.db.set_discovered(self.site.domain, sitemaps_read, urls_found)
        self.log.info("discovery done: %d sitemaps, %d URLs queued", sitemaps_read, urls_found)

    def _cap(self) -> int:
        return self.site.max_recipes or self.c.settings.max_recipes_per_site

    async def crawl(self) -> None:
        assert self.robots is not None
        s = self.c.settings
        have = self.db.recipe_count(self.site.domain)
        while not self.c.stopping:
            if have + self.recipes >= self._cap():
                self.log.info("reached per-site cap of %d recipes", self._cap())
                return
            if self.c.region_full(self.site.region):
                self.log.info("region %r reached its cap of %d recipes", self.site.region, s.max_recipes_per_region)
                return
            batch = self.db.next_batch(self.site.domain, BATCH, s.max_attempts)
            if not batch:
                self.log.info("frontier exhausted")
                return
            for url in batch:
                if self.c.stopping or have + self.recipes >= self._cap() or self.c.region_full(self.site.region):
                    return
                if self.consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                    self.log.warning("too many consecutive failures, pausing this domain for this run")
                    return
                if (not self.site.follow_links and self.pages >= YIELD_CHECK_AFTER
                        and self.recipes / self.pages < MIN_RECIPE_YIELD):
                    self.log.warning(
                        "only %d recipes in %d pages; set url_pattern/sitemap_pattern for this site "
                        "(see `probe`). Pausing domain.", self.recipes, self.pages)
                    return
                try:
                    await self._process(url)
                except Exception as e:  # a page that crashes the parser must not stall the domain on every run
                    self.log.warning("%s crashed the worker: %s: %s", url, type(e).__name__, e)
                    self.db.mark(url, "error", error=f"crash {type(e).__name__}: {e}"[:300],
                                 max_attempts=s.max_attempts)

    async def _process(self, url: str) -> None:
        s = self.c.settings
        if not self.robots.allowed(url):
            self.db.mark(url, "blocked")
            return
        if not self.site.follow_links and not keep_page_url(self.site, url):
            # queued before the site got its current url_pattern: skip without fetching
            self.db.mark(url, "no_recipe", error="filtered by url_pattern")
            return
        r = await self._get(url)
        self.pages += 1
        if r is None:
            self.db.mark(url, "error", error="network", max_attempts=s.max_attempts)
            return
        if r.status in (429, 503):
            return  # leave it pending; the throttle already backed off
        if r.status in (404, 410):
            self.db.mark(url, "gone", r.status)
            self.consecutive_failures = 0
            return
        if r.status != 200:
            self.db.mark(url, "error", r.status, f"HTTP {r.status}", s.max_attempts)
            self.consecutive_failures += 1
            return
        self.consecutive_failures = 0
        self.throttle.relax()
        if "html" not in r.content_type and r.content_type:
            self.db.mark(url, "no_recipe", r.status, f"content-type {r.content_type}")
            return
        page = r.text()
        if self.site.follow_links:
            links = await asyncio.to_thread(extract_links, page, r.url)
            self.db.add_urls(self.site.domain, (
                u for u in links if keep_followed_link(self.site, u) and self.robots.allowed(u)))
        recipes = await asyncio.to_thread(extract_recipes, page, url)
        if not recipes:
            self.db.mark(url, "no_recipe", r.status)
            return
        for i, recipe in enumerate(recipes):
            if i:
                recipe.url = f"{url}#recipe-{i + 1}"
            if self.db.save_recipe(recipe, self.site):
                self.recipes += 1
                self.c.total += 1
                self.c.region_total[self.site.region] += 1
        self.db.mark(url, "done", r.status)
        if self.c.total >= s.target_total:
            log.info("target of %d recipes reached", s.target_total)
            self.c.stopping = True

    async def run(self, discover_only: bool = False, force_discover: bool = False) -> None:
        try:
            if not await self.setup():
                return
            if force_discover:
                self.db.reset_discovery(self.site.domain)
            if not self.db.is_discovered(self.site.domain, self.c.settings.rediscover_after_days):
                await self.discover()
            if not discover_only:
                await self.crawl()
        except Exception:
            self.log.exception("worker crashed")


class Crawler:
    def __init__(self, settings: Settings, db: DB):
        self.settings = settings
        self.db = db
        self.fetcher = Fetcher(settings.user_agent, settings.timeout)
        self.stopping = False
        self.total = db.recipe_count()
        self.region_total: defaultdict[str, int] = defaultdict(int, db.region_counts())
        self.workers: list[DomainWorker] = []

    def region_full(self, region: str) -> bool:
        cap = self.settings.max_recipes_per_region
        return bool(cap and region and self.region_total[region] >= cap)

    async def _report(self) -> None:
        start_total, start = self.total, time.monotonic()
        while True:
            await asyncio.sleep(30)
            elapsed = time.monotonic() - start
            rate = (self.total - start_total) / elapsed * 3600 if elapsed else 0
            active = sum(1 for w in self.workers if w.pages or w.recipes)
            log.info("TOTAL %d recipes | %.0f/hour this run | %d domains active", self.total, rate, active)

    async def run(self, sites: list[Site], discover_only: bool = False, force_discover: bool = False) -> None:
        sem = asyncio.Semaphore(self.settings.max_parallel_domains)

        async def guarded(w: DomainWorker) -> None:
            async with sem:
                if not self.stopping:
                    await w.run(discover_only, force_discover)

        # Start sites round-robin across regions: the config lists Europe first, and with more sites than
        # max_parallel_domains the sites at the end of the list would otherwise wait for a free slot.
        by_region: defaultdict[str, list[Site]] = defaultdict(list)
        for s in sites:
            by_region[s.region].append(s)
        order = [s for group in itertools.zip_longest(*by_region.values()) for s in group if s]
        self.workers = [DomainWorker(self, s) for s in order]
        reporter = asyncio.create_task(self._report())
        try:
            await asyncio.gather(*(guarded(w) for w in self.workers))
        finally:
            reporter.cancel()
            await self.fetcher.aclose()
