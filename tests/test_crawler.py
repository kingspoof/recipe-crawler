import asyncio
import json

import httpx
import pytest

from recipe_crawler.config import Settings, Site
from recipe_crawler.crawler import Crawler, DomainWorker
from recipe_crawler.db import DB
from recipe_crawler.net import Response

ROBOTS_OK = "User-agent: *\nAllow: /\n"


def recipe_page(name="Soup", ingredients=("1 onion",)):
    node = {"@context": "https://schema.org", "@type": "Recipe", "name": name,
            "recipeIngredient": list(ingredients), "recipeInstructions": ["Cook."]}
    return f'<html><head><script type="application/ld+json">{json.dumps(node)}</script></head></html>'


class FakeFetcher:
    """routes: url -> (status, body[, headers]) or an exception instance to raise, or a callable(url)."""

    robots_token = "RecipeResearchCrawler"

    def __init__(self, routes):
        self.routes = routes
        self.requests: list[str] = []

    async def get(self, url, max_bytes=None, follow_redirects=False):
        self.requests.append(url)
        route = self.routes.get(url, (404, ""))
        if callable(route):
            route = route(url)
        if isinstance(route, BaseException):
            raise route
        status, body, *rest = route
        headers = httpx.Headers(rest[0] if rest else {"content-type": "text/html"})
        return Response(url, status, headers, body.encode() if isinstance(body, str) else body, 0.0)

    async def aclose(self):
        pass


def make(tmp_path, routes, **site_kw):
    settings = Settings(user_agent="RecipeResearchCrawler/0.1 (+test)", db_path=tmp_path / "r.db", default_delay=0)
    db = DB(tmp_path / "r.db")
    crawler = Crawler(settings, db)
    crawler.fetcher = FakeFetcher(routes)
    site = Site(domain="x.test", country="CH", language="de", delay=0, **site_kw)
    return crawler, DomainWorker(crawler, site), db


def status(db, url):
    return tuple(db.conn.execute("SELECT status, attempts FROM urls WHERE url = ?", (url,)).fetchone())


def test_crashing_page_is_marked_error_and_domain_continues(tmp_path, monkeypatch):
    """Regression: an exception in one page killed the worker, the URL stayed pending with attempts=0."""
    routes = {"https://x.test/robots.txt": (200, ROBOTS_OK, {"content-type": "text/plain"}),
              "https://x.test/bad": (200, recipe_page("bad")),
              "https://x.test/good": (200, recipe_page("good"))}
    crawler, w, db = make(tmp_path, routes)
    db.set_discovered("x.test", 0, 0)
    db.add_urls("x.test", ["https://x.test/bad", "https://x.test/good"])

    from recipe_crawler import crawler as crawler_mod
    real = crawler_mod.extract_recipes

    def extract(page, url):
        if url.endswith("/bad"):
            raise RecursionError("deeply nested JSON-LD")
        return real(page, url)

    monkeypatch.setattr(crawler_mod, "extract_recipes", extract)
    asyncio.run(w.run())
    assert status(db, "https://x.test/bad") == ("failed", 3)  # retried within the run, then given up
    assert status(db, "https://x.test/good") == ("done", 0)
    err = db.conn.execute("SELECT error FROM urls WHERE url = 'https://x.test/bad'").fetchone()[0]
    assert err.startswith("crash RecursionError")


def test_invalid_url_is_a_network_error(tmp_path):
    routes = {"https://x.test/robots.txt": (200, ROBOTS_OK, {"content-type": "text/plain"}),
              "https://x.test/a b": httpx.InvalidURL("bad url")}
    crawler, w, db = make(tmp_path, routes)
    db.set_discovered("x.test", 0, 0)
    db.add_urls("x.test", ["https://x.test/a b"])
    asyncio.run(w.run())
    assert status(db, "https://x.test/a b") == ("failed", 3)


SITEMAP_INDEX = """<?xml version="1.0"?><sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
<sitemap><loc>https://x.test/sm-1.xml</loc></sitemap><sitemap><loc>https://x.test/sm-2.xml</loc></sitemap>
</sitemapindex>"""


def urlset(*urls):
    locs = "".join(f"<url><loc>{u}</loc></url>" for u in urls)
    return f'<?xml version="1.0"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{locs}</urlset>'


def test_discovery_with_a_failed_sitemap_is_not_marked_done(tmp_path):
    """Regression: a child sitemap that got a 503 was skipped and the domain was still marked discovered."""
    xml = {"content-type": "application/xml"}
    routes = {"https://x.test/robots.txt": (200, ROBOTS_OK + "Sitemap: https://x.test/index.xml\n",
                                            {"content-type": "text/plain"}),
              "https://x.test/index.xml": (200, SITEMAP_INDEX, xml),
              "https://x.test/sm-1.xml": (200, urlset("https://x.test/r/1"), xml),
              "https://x.test/sm-2.xml": (503, "", xml)}
    crawler, w, db = make(tmp_path, routes)
    asyncio.run(w.run(discover_only=True))
    assert not db.is_discovered("x.test")
    assert db.conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0] == 1  # what was found is kept

    routes["https://x.test/sm-2.xml"] = (200, urlset("https://x.test/r/2"), xml)
    crawler, w, db = make(tmp_path, routes)
    asyncio.run(w.run(discover_only=True))
    assert db.is_discovered("x.test")
    assert db.conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0] == 2


def test_rediscover_after_days(tmp_path):
    db = DB(tmp_path / "r.db")
    db.set_discovered("x.test", 1, 1)
    assert db.is_discovered("x.test", 30) and db.is_discovered("x.test", 0)
    db.conn.execute("UPDATE domains SET discovered_at = discovered_at - 31 * 86400")
    assert not db.is_discovered("x.test", 30)
    assert db.is_discovered("x.test", 0)  # 0 = never again


TEXT = {"content-type": "text/plain"}


def test_robots_refetched_after_24h(tmp_path):
    routes = {"https://x.test/robots.txt": (200, ROBOTS_OK, TEXT),
              "https://x.test/1": (200, recipe_page("one")),
              "https://x.test/2": (200, recipe_page("two"))}
    crawler, w, db = make(tmp_path, routes)
    now = [0.0]
    w.clock = lambda: now[0]
    db.set_discovered("x.test", 0, 0)
    db.add_urls("x.test", ["https://x.test/1"])
    asyncio.run(w.run())
    assert status(db, "https://x.test/1") == ("done", 0)

    routes["https://x.test/robots.txt"] = (200, "User-agent: *\nDisallow: /\n", TEXT)
    db.add_urls("x.test", ["https://x.test/2"])
    now[0] = 23 * 3600
    asyncio.run(w.crawl())  # still the cached robots.txt
    assert status(db, "https://x.test/2") == ("done", 0)
    db.add_urls("x.test", ["https://x.test/3"])
    now[0] = 25 * 3600
    asyncio.run(w.crawl())
    assert status(db, "https://x.test/3") == ("blocked", 0)
    assert crawler.fetcher.requests.count("https://x.test/robots.txt") == 2


def test_redirects_are_checked_against_robots(tmp_path):
    routes = {"https://x.test/robots.txt": (200, ROBOTS_OK, TEXT),
              "https://x.test/a": (301, "", {"location": "/recipe/a"}),
              "https://x.test/recipe/a": (200, recipe_page("a")),
              "https://x.test/b": (302, "", {"location": "https://other.test/private/b"}),
              "https://other.test/robots.txt": (200, "User-agent: *\nDisallow: /private/\n", TEXT),
              "https://x.test/loop": (302, "", {"location": "/loop"})}
    crawler, w, db = make(tmp_path, routes)
    db.set_discovered("x.test", 0, 0)
    db.add_urls("x.test", ["https://x.test/a", "https://x.test/b", "https://x.test/loop"])
    asyncio.run(w.run())
    assert status(db, "https://x.test/a") == ("done", 0)
    assert db.recipe_count() == 1
    assert status(db, "https://x.test/b") == ("blocked", 0)
    assert "https://other.test/private/b" not in crawler.fetcher.requests
    assert status(db, "https://x.test/loop") == ("blocked", 0)
    assert crawler.fetcher.requests.count("https://x.test/loop") == 6  # 1 + 5 redirects, then given up


def test_child_sitemap_on_other_host_uses_that_hosts_robots(tmp_path):
    xml = {"content-type": "application/xml"}
    index = SITEMAP_INDEX.replace("https://x.test/sm-2.xml", "https://cdn.test/sm-2.xml")
    routes = {"https://x.test/robots.txt": (200, ROBOTS_OK + "Sitemap: https://x.test/index.xml\n", TEXT),
              "https://cdn.test/robots.txt": (200, "User-agent: *\nDisallow: /\n", TEXT),
              "https://x.test/index.xml": (200, index, xml),
              "https://x.test/sm-1.xml": (200, urlset("https://x.test/r/1"), xml),
              "https://cdn.test/sm-2.xml": (200, urlset("https://x.test/r/2"), xml)}
    crawler, w, db = make(tmp_path, routes)
    asyncio.run(w.run(discover_only=True))
    assert "https://cdn.test/sm-2.xml" not in crawler.fetcher.requests
    assert [r[0] for r in db.conn.execute("SELECT url FROM urls")] == ["https://x.test/r/1"]
