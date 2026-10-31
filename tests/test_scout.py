import asyncio
import gzip
import json
import time
import tomllib
from pathlib import Path

import httpx
import pytest

from recipe_crawler import net, scout
from recipe_crawler.config import Excluded, Settings, Site, load_config
from recipe_crawler.crawler import Crawler
from recipe_crawler.db import DB
from recipe_crawler.extract import Recipe
from recipe_crawler.probe import classify, probe_site, recipe_problem, suggest_url_pattern


def settings(tmp_path, **kw) -> Settings:
    s = Settings(user_agent="RecipeResearchCrawler/0.1 (+test@example.org)", db_path=tmp_path / "r.db",
                 default_delay=0.0, **kw)
    s.sites = [Site("www.marmiton.org", "FR", "fr", "Europe"), Site("www.chefkoch.de", "DE", "de", "Europe")]
    s.excluded = [Excluded("allrecipes.com", "unknown")]
    return s


# --------------------------------------------------------------------------- domains / filter


def test_filter(tmp_path):
    f = scout.Filter(settings(tmp_path))
    assert f.reason("marmiton.org") == "configured"
    assert f.reason("WWW.Marmiton.org") == "configured"
    assert f.reason("static.chefkoch.de") == "configured"
    assert f.reason("www.allrecipes.com") == "excluded"
    for h in ("www.facebook.com", "pin.it", "i.pinimg.com.cdn.cloudflare.com", "amazon.de", "www.google.fr",
              "cdn.example.com", "blogspot.com", "fonts.googleapis.com"):
        assert f.reason(h) == "non-recipe", h
    for h in ("www.kuechengoetter.de", "rezepte.blogspot.com", "applepie.com", "myblog.wordpress.com"):
        assert f.keep(h), h


def test_clean_host():
    assert scout.clean_host("https://WWW.Example.com/x?y") == "www.example.com"
    assert scout.clean_host(" example.co.uk ") == "example.co.uk"
    assert scout.clean_host("example.com:8080") == "example.com"
    for bad in ("localhost", "1.2.3.4", "not a host", "", "foo.123"):
        assert scout.clean_host(bad) is None, bad


def test_external_hosts():
    page = """<a href="https://www.other.fr/r/1">x</a> <a href='//cdn.thing.net/a.js'>
              <a href="/local">l</a> <a href="https://www.marmiton.org/x">own</a>
              <a href="https://img.marmiton.org/x">own sub</a> <a class=x href=http://Third.com>t</a>
              <a href="mailto:a@b.com">m</a>"""
    assert scout.external_hosts(page, "www.marmiton.org") == {"www.other.fr", "cdn.thing.net", "third.com"}


# --------------------------------------------------------------------------- links -> candidates


def test_collector_and_from_links(tmp_path):
    s = settings(tmp_path)
    db = DB(s.db_path)
    c = scout.LinkCollector(db, s, flush_every=3)
    c.add("www.marmiton.org", {"www.blog-a.fr", "www.facebook.com", "www.chefkoch.de"})
    c.add("www.marmiton.org", {"www.blog-a.fr", "solo.fr"})
    c.add("www.chefkoch.de", {"blog-a.fr"})  # 3rd page: flushed
    assert not c.pending
    c.flush()
    rows = {(r["host"], r["from_domain"]): r["pages"] for r in db.conn.execute("SELECT * FROM candidate_links")}
    assert rows == {("www.blog-a.fr", "www.marmiton.org"): 2, ("solo.fr", "www.marmiton.org"): 1,
                    ("blog-a.fr", "www.chefkoch.de"): 1}
    assert scout.from_links(db, s) == (1, 0)  # solo.fr is linked from only 1 site
    row = db.conn.execute("SELECT * FROM site_candidates").fetchone()
    assert (row["domain"], row["host"], row["seen_count"], row["linked_sites"], row["status"]) == \
        ("blog-a.fr", "www.blog-a.fr", 3, 2, "new")
    assert scout.from_links(db, s) == (0, 1)  # idempotent
    assert db.conn.execute("SELECT seen_count FROM site_candidates").fetchone()[0] == 3
    assert scout.from_links(db, s, min_sites=1) == (1, 1)
    db.close()


def test_collector_keeps_counts_when_db_is_locked(tmp_path):
    s = settings(tmp_path)
    db, other = DB(s.db_path), DB(s.db_path)
    db.conn.execute("PRAGMA busy_timeout = 0")
    other.conn.execute("BEGIN IMMEDIATE")
    c = scout.LinkCollector(db, s)
    c.add("www.marmiton.org", {"blog.fr"})
    c.flush()
    assert c.pending and not db.conn.in_transaction
    other.conn.execute("COMMIT")
    c.flush()
    assert not c.pending
    assert db.conn.execute("SELECT pages FROM candidate_links").fetchone()[0] == 1
    db.close(), other.close()


def test_collector_leaves_foreign_transaction_alone(tmp_path):
    s = settings(tmp_path)
    db = DB(s.db_path)
    db.conn.execute("BEGIN")
    db.conn.execute("INSERT INTO domains(domain) VALUES ('x')")
    c = scout.LinkCollector(db, s)
    c.add("www.marmiton.org", {"blog.fr"})
    c.flush()
    assert db.conn.in_transaction and c.pending
    db.conn.execute("COMMIT")
    assert db.conn.execute("SELECT COUNT(*) FROM domains").fetchone()[0] == 1
    db.close()


# --------------------------------------------------------------------------- WDC import


@pytest.mark.parametrize("text, expected", [
    ("domain,entities\nwww.a.com,120\nb.de,7\n", [("www.a.com", 120), ("b.de", 7)]),
    ("pld\t#quads\t#entities\na.com\t900\t30\n", [("a.com", 30)]),  # entities preferred
    ("a.com 12\nb.com  3\n", [("a.com", 12), ("b.com", 3)]),  # whitespace-separated
    ("a.com\nhttps://www.b.fr/\n# comment\n\n", [("a.com", None), ("www.b.fr", None)]),
    ("12;a.com;5\n", [("a.com", 5)]),
])
def test_parse_wdc(text, expected):
    assert scout.parse_wdc(text) == expected


def test_import_wdc(tmp_path):
    s = settings(tmp_path)
    db = DB(s.db_path)
    f = tmp_path / "Recipe_domains.csv.gz"
    f.write_bytes(gzip.compress(b"domain,entities\nwww.newsite.it,500\nnewsite.it,20\nmarmiton.org,9\n"
                                b"allrecipes.com,9\nyoutube.com,9\n"))
    counts = scout.import_wdc(db, s, f)
    assert counts == {"read": 5, "new": 1, "configured": 1, "excluded": 1, "non-recipe": 1}
    row = db.conn.execute("SELECT * FROM site_candidates").fetchone()
    assert (row["domain"], row["host"], row["wdc_entities"], row["source"]) == ("newsite.it", "www.newsite.it", 500, "wdc")
    db.conn.execute("INSERT INTO candidate_links VALUES ('newsite.it', 'www.marmiton.org', 3, 0, 0)")
    scout.from_links(db, s, min_sites=1)
    assert db.conn.execute("SELECT source FROM site_candidates").fetchone()[0] == "wdc,links"
    db.close()


# --------------------------------------------------------------------------- probe helpers


def recipe(ings):
    return Recipe(url="u", name="n", ingredients=ings, instructions=["x"])


def test_recipe_problem():
    assert recipe_problem(recipe(["200 g flour", "2 eggs"])) is None
    assert recipe_problem(recipe(["flour, eggs, milk"])) == "0-1 ingredient lines"
    assert recipe_problem(recipe(["Ingredients", "-", "1 egg"])) == "placeholder ingredients"
    assert recipe_problem(recipe(["salt", "Salt", "salt "])) == "identical ingredient lines"


def test_suggest_url_pattern():
    assert suggest_url_pattern("www.x.de", ["https://www.x.de/rezept/1", "https://www.x.de/rezept/2-a"]) == \
        r"^https://www\.x\.de/rezept/"
    assert suggest_url_pattern("x.de", ["https://x.de/a/1", "https://x.de/b/2"]) is None
    assert suggest_url_pattern("x.de", ["https://x.de/a/1"]) is None


@pytest.mark.parametrize("report, status", [
    ({"setup_failed": True, "problem": "robots.txt returned HTTP 403 (bot blocking)"}, "blocked"),
    ({"setup_failed": True, "problem": "robots.txt unreachable: ConnectError"}, "unreachable"),
    ({"problem": "robots.txt disallows our user agent for the whole site"}, "blocked"),
    ({"samples": [{"status": 403}, {"status": 403}]}, "blocked"),
    ({"samples": [{"status": "network error"}]}, "unreachable"),
    ({"mode": "links", "start_page_status": 403}, "blocked"),
    ({"samples": [{"status": 200, "recipe": None}]}, "no_recipes"),
    ({"samples": [{"status": 200, "recipe": "a", "data_problem": "0-1 ingredient lines"},
                  {"status": 200, "recipe": "b"}]}, "bad_data"),
    ({"samples": [{"status": 200, "recipe": "a"}, {"status": 200, "recipe": None}]}, "ok"),
    ({"mode": "links", "samples": [{"status": 200, "recipe": "a"}]}, "links"),
])
def test_classify(report, status):
    assert classify(report) == status


def test_guess_location():
    assert scout.guess_location("www.kochen.de", "de") == scout.Guess("DE", "Europe", "country from .de")
    assert scout.guess_location("food.co.uk", "en").country == "GB"
    assert scout.guess_location("x.com", "ja") == scout.Guess("JP", "East Asia", "country from language ja")
    assert scout.guess_location("x.io", "en").country == ""


# --------------------------------------------------------------------------- probe + crawl over fake HTTP

RECIPE = {"@context": "https://schema.org", "@type": "Recipe", "name": "Kuchen", "inLanguage": "de",
          "recipeIngredient": ["200 g Mehl", "2 Eier"], "recipeInstructions": "Backen."}


def recipe_page(links=""):
    return f'<html lang="de"><script type="application/ld+json">{json.dumps(RECIPE)}</script>{links}</html>'


def fake_site(request: httpx.Request) -> httpx.Response:
    host, path = request.url.host, request.url.path
    if host == "blocked.com":
        return httpx.Response(403)
    if host == "down.com":
        raise httpx.ConnectError("no route")
    if path == "/robots.txt":
        return httpx.Response(200, text=f"User-agent: *\nDisallow: /private\nSitemap: https://{host}/sitemap.xml\n")
    if path == "/sitemap.xml":
        urls = "".join(f"<url><loc>https://{host}/rezept/{i}</loc></url>" for i in range(10))
        return httpx.Response(200, text=f'<?xml version="1.0"?><urlset>{urls}</urlset>')
    if path.startswith("/rezept/"):
        return httpx.Response(200, text=recipe_page('<a href="https://www.kochblog.at/x">x</a>'),
                              headers={"content-type": "text/html"})
    return httpx.Response(404)


@pytest.fixture
def fake_http(monkeypatch):
    requests = []

    def handler(request):
        requests.append(str(request.url))
        return fake_site(request)

    real = httpx.AsyncClient
    monkeypatch.setattr(net.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw))
    return requests


def test_probe_site(tmp_path, fake_http):
    s = settings(tmp_path)
    rep = asyncio.run(probe_site(s, "www.newsite.de", samples=3, max_sitemaps=3))
    assert rep["status"] == "ok" and rep["ok"]
    assert rep["hit_rate"] == 1.0 and rep["est_urls"] == 10 and rep["language"] == "de"
    assert rep["url_pattern"] == r"^https://www\.newsite\.de/rezept/"
    assert asyncio.run(probe_site(s, "blocked.com", 3, 3))["status"] == "blocked"
    assert asyncio.run(probe_site(s, "down.com", 3, 3))["status"] == "unreachable"
    assert not any("/private" in u for u in fake_http)


def test_probe_candidates_and_report(tmp_path, fake_http):
    s = settings(tmp_path)
    db = DB(s.db_path)
    now = time.time()
    for host in ("www.newsite.de", "blocked.com", "down.com"):
        scout._upsert(db.conn, scout.bare(host), host, "wdc", wdc_entities=100)
    # failed recently: not probed again; failed long ago: probed again
    scout._upsert(db.conn, "old.de", "old.de", "wdc", status="no_recipes", probed_at=now - 40 * 86400)
    scout._upsert(db.conn, "recent.de", "recent.de", "wdc", status="no_recipes", probed_at=now - 86400)
    results = asyncio.run(scout.probe_candidates(db, s, 10))
    assert results == {"ok": 2, "blocked": 1, "unreachable": 1}  # old.de serves the fake site too
    st = dict(db.conn.execute("SELECT domain, status FROM site_candidates"))
    assert st["recent.de"] == "no_recipes"
    assert asyncio.run(scout.probe_candidates(db, s, 10)) == {}  # nothing left to probe

    lines = scout.report(db, s)
    text = "\n".join(lines)
    assert "# NOT enabled" in text
    seeds = [ln for ln in lines if ln.startswith("{ domain")]
    parsed = tomllib.loads("site = [\n" + "\n".join(seeds) + "\n]")["site"]
    assert {p["domain"] for p in parsed} == {"www.newsite.de", "old.de"}
    assert parsed[0]["country"] == "DE" and parsed[0]["language"] == "de" and parsed[0]["region"] == "Europe"
    excluded = [ln for ln in lines if ln.startswith("  { domain")]
    parsed = tomllib.loads("site = [\n" + "\n".join(excluded) + "\n]")["site"]
    assert [(p["domain"], p["status"]) for p in parsed] == [("blocked.com", "blocked"), ("recent.de", "no_recipes")]
    # once added to the config, a candidate disappears from the report
    s.sites.append(Site("www.newsite.de", "DE", "de", "Europe"))
    assert "www.newsite.de" not in "\n".join(scout.report(db, s))
    db.close()


def test_report_links_mode_line():
    row = {"host": "x.de", "language": "de", "status": "links", "url_pattern": None}
    assert scout.config_line(row, scout.Guess("DE", "Europe", "")).startswith("# {")
    row["url_pattern"] = r"^https://x\.de/r/"
    line = scout.config_line(row, scout.Guess("DE", "Europe", ""))
    assert tomllib.loads("s = [" + line + "]")["s"][0]["url_pattern"] == r"^https://x\.de/r/"


def test_crawl_collects_links(tmp_path, fake_http):
    s = settings(tmp_path)
    s.sites = [Site("www.newsite.de", "DE", "de", "Europe")]
    db = DB(s.db_path)
    asyncio.run(Crawler(s, db).run(s.sites))
    assert db.recipe_count() == 10
    rows = db.conn.execute("SELECT host, from_domain, pages FROM candidate_links").fetchall()
    assert [tuple(r) for r in rows] == [("www.kochblog.at", "www.newsite.de", 10)]  # flushed at the end
    db.close()


# --------------------------------------------------------------------------- config


def test_config_excluded(tmp_path):
    cfg = Path(__file__).parent.parent / "config.toml"
    s = load_config(cfg, require_contact=False)
    assert len(s.excluded) > 100 and {e.status for e in s.excluded} <= set(scout.FAILED) | {"unknown"}
    bad = tmp_path / "c.toml"
    bad.write_text('[seeds]\nsite = [{ domain = "www.a.com", country = "", language = "" }]\n'
                   '[excluded]\nsite = [{ domain = "a.com", status = "blocked" }]\n')
    with pytest.raises(SystemExit, match="both"):
        load_config(bad, require_contact=False)
    bad.write_text('[excluded]\nsite = [{ domain = "a.com", status = "nope" }]\n')
    with pytest.raises(SystemExit, match="status"):
        load_config(bad, require_contact=False)
