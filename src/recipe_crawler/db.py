"""SQLite storage: the URL frontier and the extracted recipes.

Everything is persisted, so the crawler can be stopped (Ctrl+C) and resumed.
"""

from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import time
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator

from . import schema
from .config import Site
from .extract import Recipe


EXPORT_COLUMNS = (
    "url, domain, site_country, site_region, language, name, description, ingredients, instructions, cuisine, "
    "category, keywords, prep_time_min, cook_time_min, total_time_min, recipe_yield, image, author, "
    "date_published, rating_value, rating_count, nutrition, crawled_at"
)
_JSON_FIELDS = ("ingredients", "instructions", "cuisine", "category", "keywords", "nutrition")

# Seconds a connection waits for another one's write lock. `map` and `crawl` run at the same time, and a
# `map` chunk or a big `add_urls` can hold the lock for a few seconds.
BUSY_TIMEOUT = 60.0
JOURNAL_SIZE_LIMIT = 64 * 1024 * 1024


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


log = logging.getLogger("db")


class Busy(SystemExit):
    pass


def lock_file(path: Path, exclusive: bool, wait: bool = False) -> int:
    """flock on `path` (created if missing); returns the fd, which holds the lock until closed."""
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, (fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH) | (0 if wait else fcntl.LOCK_NB))
    except BlockingIOError:
        os.close(fd)
        raise Busy(f"{path} is locked by another recipe-crawler process") from None
    return fd


def single_instance(db_path: Path, name: str) -> int:
    """At most one `crawl`, one `process`, ... per database at a time (two `process` runs would each think
    they have the whole quota). Returns the lock's fd: keep it open for the whole command."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    try:
        return lock_file(Path(f"{db_path}.{name}.lock"), exclusive=True)
    except Busy:
        raise Busy(f"another `recipe-crawler {name}` is already running on {db_path}") from None


class DB:
    """readonly: for commands that only read (stats, export, dry runs). Opened with mode=ro, so nothing, not even
    a migration, can write; a missing database is an error instead of an empty new file.

    Otherwise the schema is migrated to the current version on open (schema.py); `backup` is called first if
    a migration needs a full backup."""

    def __init__(self, path: Path, timeout: float = BUSY_TIMEOUT, readonly: bool = False,
                 backup: Callable[[], object] | None = None):
        memory = str(path) == ":memory:"
        if readonly and not memory:
            if not Path(path).exists():
                raise SystemExit(f"No database at {path}: run `recipe-crawler crawl` first.")
            self.conn = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True,
                                        isolation_level=None, check_same_thread=False, timeout=timeout)
        else:
            if not memory:
                Path(path).parent.mkdir(parents=True, exist_ok=True)
            self.conn = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=timeout)
        self.conn.row_factory = sqlite3.Row
        self._lock_fd: int | None = None
        if readonly:
            v = schema.version(self.conn)
            if v != schema.CURRENT:
                log.warning("database is at schema version %d, this code at %d: a write command migrates it",
                            v, schema.CURRENT)
            return
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        # After a checkpoint, the -wal file is truncated to this size instead of keeping its peak size
        # (`map --full` used to leave GBs behind).
        self.conn.execute(f"PRAGMA journal_size_limit={JOURNAL_SIZE_LIMIT}")
        if memory:
            schema.migrate(self.conn, backup)
            return
        # Every writing process holds a shared lock on recipes.db.lock; a migration needs it exclusively, so
        # it never changes the schema under a running `crawl` that still uses the old one.
        lock = Path(f"{path}.lock")
        if schema.pending(self.conn):
            try:
                self._lock_fd = lock_file(lock, exclusive=True)
            except Busy:
                self.conn.close()
                raise Busy(f"The database needs a schema migration (to version {schema.CURRENT}), but another "
                           "recipe-crawler process is using it: stop it, then run this again.") from None
            try:
                schema.migrate(self.conn, backup)
            except BaseException:
                self.close()
                raise
            fcntl.flock(self._lock_fd, fcntl.LOCK_SH)  # downgrade: other commands may start now
        else:
            self._lock_fd = lock_file(lock, exclusive=False, wait=True)  # waits only for a running migration

    def close(self) -> None:
        self.conn.close()
        if self._lock_fd is not None:
            os.close(self._lock_fd)
            self._lock_fd = None

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

    def is_discovered(self, domain: str, max_age_days: float = 0) -> bool:
        """True if the sitemaps were read completely, and (max_age_days > 0) not longer ago than that."""
        row = self.conn.execute("SELECT discovered_at FROM domains WHERE domain = ?", (domain,)).fetchone()
        if not (row and row["discovered_at"]):
            return False
        return max_age_days <= 0 or row["discovered_at"] > time.time() - max_age_days * 86400

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
        # Listed, not "*": the real table still has legacy columns from earlier versions, which must not end up
        # in the export.
        cols = EXPORT_COLUMNS + (", raw" if include_raw else "")
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
