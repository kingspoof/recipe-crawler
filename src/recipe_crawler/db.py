"""SQLite storage: the URL frontier and the extracted recipes.

Everything is persisted, so the crawler can be stopped (Ctrl+C) and resumed.
"""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator

from .config import Site
from .extract import Recipe

SCHEMA = """
CREATE TABLE IF NOT EXISTS urls (
    url         TEXT PRIMARY KEY,
    domain      TEXT NOT NULL,
    -- pending | done | no_recipe | error | failed | blocked | gone
    status      TEXT NOT NULL DEFAULT 'pending',
    attempts    INTEGER NOT NULL DEFAULT 0,
    http_status INTEGER,
    error       TEXT,
    updated_at  REAL
);
CREATE INDEX IF NOT EXISTS urls_domain_status ON urls(domain, status);

CREATE TABLE IF NOT EXISTS domains (
    domain        TEXT PRIMARY KEY,
    discovered_at REAL,
    sitemaps_read INTEGER,
    urls_found    INTEGER
);

CREATE TABLE IF NOT EXISTS recipes (
    id             INTEGER PRIMARY KEY,  -- stable, referenced by recipe_ingredients.recipe_id
    url            TEXT NOT NULL UNIQUE,
    domain         TEXT NOT NULL,
    site_country   TEXT,
    site_region    TEXT,
    language       TEXT,
    name           TEXT NOT NULL,
    description    TEXT,
    ingredients    TEXT NOT NULL,  -- JSON list of strings
    instructions   TEXT NOT NULL,  -- JSON list of strings
    cuisine        TEXT,           -- JSON list
    category       TEXT,           -- JSON list
    keywords       TEXT,           -- JSON list
    prep_time_min  INTEGER,
    cook_time_min  INTEGER,
    total_time_min INTEGER,
    recipe_yield   TEXT,
    image          TEXT,
    author         TEXT,
    date_published TEXT,
    rating_value   REAL,
    rating_count   INTEGER,
    nutrition      TEXT,           -- JSON object
    raw            TEXT,           -- original JSON-LD node
    crawled_at     REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS recipes_domain ON recipes(domain);
"""

_JSON_FIELDS = ("ingredients", "instructions", "cuisine", "category", "keywords", "nutrition")

# Seconds a connection waits for another one's write lock. `map` and `crawl` run at the same time, and a
# `map` chunk or a big `add_urls` can hold the lock for a few seconds.
BUSY_TIMEOUT = 60.0


@contextmanager
def transaction(conn: sqlite3.Connection, immediate: bool = True) -> Iterator[None]:
    """One transaction on an autocommit connection; rolled back on any error, so the connection never stays
    inside a half-done transaction (which would silently swallow every later write until it is closed).

    immediate: take the write lock right away (BEGIN IMMEDIATE). A deferred BEGIN that reads first and writes
    later fails with SQLITE_BUSY at the first write if another connection committed in between, and the busy
    timeout does not help there.
    """
    if conn.in_transaction:
        raise RuntimeError("transaction() does not nest: a transaction is already open")
    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield
    except BaseException:
        if conn.in_transaction:  # some errors (e.g. SQLITE_FULL) already rolled back
            conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")


class DB:
    def __init__(self, path: Path, timeout: float = BUSY_TIMEOUT):
        if str(path) != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=timeout)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    # ------------------------------------------------------------------ frontier

    def add_urls(self, domain: str, urls: Iterable[str]) -> int:
        now = time.time()
        before = self.conn.total_changes
        with transaction(self.conn):
            self.conn.executemany(
                "INSERT OR IGNORE INTO urls(url, domain, updated_at) VALUES (?, ?, ?)",
                ((u, domain, now) for u in urls),
            )
        return self.conn.total_changes - before

    def next_batch(self, domain: str, limit: int, max_attempts: int) -> list[str]:
        rows = self.conn.execute(
            """SELECT url FROM urls
               WHERE domain = ? AND (status = 'pending' OR (status = 'error' AND attempts < ?))
               ORDER BY attempts, rowid LIMIT ?""",
            (domain, max_attempts, limit),
        ).fetchall()
        return [r["url"] for r in rows]

    def mark(self, url: str, status: str, http_status: int | None = None, error: str | None = None,
             max_attempts: int = 3) -> None:
        if status == "error":
            self.conn.execute(
                """UPDATE urls SET attempts = attempts + 1, http_status = ?, error = ?, updated_at = ?,
                   status = CASE WHEN attempts + 1 >= ? THEN 'failed' ELSE 'error' END
                   WHERE url = ?""",
                (http_status, error, time.time(), max_attempts, url),
            )
        else:
            self.conn.execute(
                "UPDATE urls SET status = ?, http_status = ?, error = ?, updated_at = ? WHERE url = ?",
                (status, http_status, error, time.time(), url),
            )

    def is_discovered(self, domain: str) -> bool:
        row = self.conn.execute("SELECT discovered_at FROM domains WHERE domain = ?", (domain,)).fetchone()
        return bool(row and row["discovered_at"])

    def set_discovered(self, domain: str, sitemaps_read: int, urls_found: int) -> None:
        self.conn.execute(
            """INSERT INTO domains(domain, discovered_at, sitemaps_read, urls_found) VALUES (?, ?, ?, ?)
               ON CONFLICT(domain) DO UPDATE SET discovered_at = excluded.discovered_at,
               sitemaps_read = excluded.sitemaps_read, urls_found = excluded.urls_found""",
            (domain, time.time(), sitemaps_read, urls_found),
        )

    def reset_discovery(self, domain: str) -> None:
        self.conn.execute("DELETE FROM domains WHERE domain = ?", (domain,))

    # ------------------------------------------------------------------ recipes

    def save_recipe(self, r: Recipe, site: Site) -> bool:
        """Insert a recipe; returns False if a recipe for this URL already exists."""
        # The configured site language wins: many pages mislabel themselves (template defaults like "en").
        # The page's own label is only used for multilingual sites configured with language = "".
        # Primary subtag only ("de-DE" -> "de"); the site's country is stored separately.
        lang = (site.language or r.language or "").replace("_", "-").split("-")[0].lower() or None
        cur = self.conn.execute(
            """INSERT OR IGNORE INTO recipes(url, domain, site_country, site_region, language, name, description,
               ingredients, instructions, cuisine, category, keywords, prep_time_min, cook_time_min,
               total_time_min, recipe_yield, image, author, date_published, rating_value, rating_count,
               nutrition, raw, crawled_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                r.url, site.domain, site.country, site.region, lang, r.name, r.description,
                json.dumps(r.ingredients, ensure_ascii=False),
                json.dumps(r.instructions, ensure_ascii=False),
                json.dumps(r.cuisine, ensure_ascii=False),
                json.dumps(r.category, ensure_ascii=False),
                json.dumps(r.keywords, ensure_ascii=False),
                r.prep_time_min, r.cook_time_min, r.total_time_min, r.recipe_yield, r.image, r.author,
                r.date_published, r.rating_value, r.rating_count,
                json.dumps(r.nutrition, ensure_ascii=False) if r.nutrition else None,
                json.dumps(r.raw, ensure_ascii=False) if r.raw else None,
                time.time(),
            ),
        )
        return cur.rowcount > 0

    def recipe_count(self, domain: str | None = None) -> int:
        if domain:
            return self.conn.execute("SELECT COUNT(*) FROM recipes WHERE domain = ?", (domain,)).fetchone()[0]
        return self.conn.execute("SELECT COUNT(*) FROM recipes").fetchone()[0]

    def region_counts(self) -> dict[str, int]:
        return dict(self.conn.execute("SELECT site_region, COUNT(*) FROM recipes GROUP BY site_region"))

    def iter_recipes(self, include_raw: bool = False) -> Iterator[dict[str, Any]]:
        cols = "*" if include_raw else "url, domain, site_country, site_region, language, name, description, ingredients, " \
            "instructions, cuisine, category, keywords, prep_time_min, cook_time_min, total_time_min, " \
            "recipe_yield, image, author, date_published, rating_value, rating_count, nutrition, crawled_at"
        for row in self.conn.execute(f"SELECT {cols} FROM recipes ORDER BY id"):
            d = dict(row)
            for f in _JSON_FIELDS + (("raw",) if include_raw else ()):
                if d.get(f):
                    d[f] = json.loads(d[f])
            yield d

    # ------------------------------------------------------------------ stats

    def stats(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            """SELECT u.domain,
                      SUM(u.status = 'pending') AS pending,
                      SUM(u.status = 'done') AS done,
                      SUM(u.status = 'no_recipe') AS no_recipe,
                      SUM(u.status IN ('error', 'failed')) AS errors,
                      SUM(u.status = 'blocked') AS blocked,
                      (SELECT COUNT(*) FROM recipes r WHERE r.domain = u.domain) AS recipes
               FROM urls u GROUP BY u.domain ORDER BY recipes DESC"""
        ).fetchall()

    def stats_by(self, column: str) -> list[sqlite3.Row]:
        assert column in ("site_country", "site_region", "language")
        return self.conn.execute(
            f"SELECT {column} AS key, COUNT(*) AS n FROM recipes GROUP BY {column} ORDER BY n DESC"
        ).fetchall()
