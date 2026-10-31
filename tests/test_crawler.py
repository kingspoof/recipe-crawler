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
