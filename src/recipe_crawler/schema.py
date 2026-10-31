"""Schema versions of recipes.db, in one place.

The version is stored in `PRAGMA user_version`. Each migration runs in its own BEGIN IMMEDIATE transaction
together with the version bump, so a crash leaves the database at the old or the new version, never between.

- A database newer than this code is refused for writing (an old checkout must not touch a migrated DB).
- Read-only commands never migrate (see DB(readonly=True)).
- Migrations marked `backup` get a full backup first (`recipe-crawler backup --full`), if there is data.
- Version 1 is the baseline: the layout the crawler and ingredient_processing.ipynb created, plus
  ingredient_lines.original. On the existing database it only adds what is missing, so nothing is reprocessed.

To change the schema: append a Migration with the next version. Never edit or reorder old ones.
Every migration must be idempotent (check before it adds: a column, a table, an index), so that it also
works on a database where the change was already made by hand or by an older checkout.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from typing import Callable

log = logging.getLogger("schema")

CRAWLER_TABLES = """
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


INGREDIENT_TABLES = """
CREATE TABLE IF NOT EXISTS ingredient_lines (
    id INTEGER PRIMARY KEY,
    line TEXT NOT NULL UNIQUE,
    n INTEGER NOT NULL,
    status INTEGER NOT NULL DEFAULT 0,
    items TEXT,
    model TEXT,
    done_at REAL
);
CREATE INDEX IF NOT EXISTS ingredient_lines_todo ON ingredient_lines(status, n DESC);

CREATE TABLE IF NOT EXISTS ingredient_names (
    name TEXT PRIMARY KEY,
    canonical TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS recipe_ingredients_state (
    recipe_id INTEGER PRIMARY KEY,  -- recipes.id
    mapped_at REAL NOT NULL,
    pending INTEGER NOT NULL        -- lines of the recipe that were not done yet when it was mapped
);
"""

RECIPE_INGREDIENTS = """
CREATE TABLE IF NOT EXISTS recipe_ingredients (
    recipe_id INTEGER NOT NULL,  -- recipes.id
    pos INTEGER NOT NULL,        -- position of the line in the recipe
    line_id INTEGER NOT NULL,    -- ingredient_lines.id
    name TEXT NOT NULL,
    amount REAL,
    unit TEXT
);
"""


class SchemaTooNew(SystemExit):
    pass


def statements(script: str) -> list[str]:
    """The statements of a script (no triggers here, so splitting on ';' is safe). executescript() would
    COMMIT first, which breaks the one-transaction-per-migration rule."""
    return [s.strip() for s in script.split(";") if s.strip()]


def columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)).fetchone() is not None


def _v1_baseline(conn: sqlite3.Connection) -> None:
    for stmt in statements(CRAWLER_TABLES + INGREDIENT_TABLES + RECIPE_INGREDIENTS):
        conn.execute(stmt)
    if "original" not in columns(conn, "ingredient_lines"):
        conn.execute("ALTER TABLE ingredient_lines ADD COLUMN original TEXT")  # schema only, instant on 2M rows
        log.info("migration: added ingredient_lines.original")
    # Created only if recipe_ingredients has no index on these columns yet (map --full names them differently).
    for column in ("recipe_id", "name"):
        if not _has_index_on(conn, "recipe_ingredients", column):
            conn.execute(f"CREATE INDEX recipe_ingredients_{column.split('_')[0]} ON recipe_ingredients({column})")


def _has_index_on(conn: sqlite3.Connection, table: str, column: str) -> bool:
    for _, name, *_ in conn.execute(f"PRAGMA index_list({table})"):
        if [r[2] for r in conn.execute(f"PRAGMA index_info({name})")] == [column]:
            return True
    return False


def _v2_incomplete_discoveries(conn: sqlite3.Connection) -> None:
    # schema only (no table rewrite), instant on the real database: no backup needed
    if "incomplete_discoveries" not in columns(conn, "domains"):
        conn.execute("ALTER TABLE domains ADD COLUMN incomplete_discoveries INTEGER NOT NULL DEFAULT 0")


@dataclass(frozen=True)
class Migration:
    version: int
    description: str
    apply: Callable[[sqlite3.Connection], None]
    backup: bool = True  # take a full backup first (if the database has data)


MIGRATIONS = [
    Migration(1, "baseline (crawler + ingredient tables, ingredient_lines.original)", _v1_baseline, backup=False),
    Migration(2, "domains.incomplete_discoveries", _v2_incomplete_discoveries, backup=False),
]
CURRENT = len(MIGRATIONS)
assert [m.version for m in MIGRATIONS] == list(range(1, CURRENT + 1))


def version(conn: sqlite3.Connection) -> int:
    return conn.execute("PRAGMA user_version").fetchone()[0]


def check_not_newer(conn: sqlite3.Connection, where: str = "the database") -> int:
    v = version(conn)
    if v > CURRENT:
        raise SchemaTooNew(f"{where} is at schema version {v}, this code only knows {CURRENT}: update the code "
                           "(git pull) instead of running an old version against a migrated database.")
    return v


def pending(conn: sqlite3.Connection) -> list[Migration]:
    return MIGRATIONS[check_not_newer(conn):]


def has_data(conn: sqlite3.Connection) -> bool:
    return any(has_table(conn, t) and conn.execute(f"SELECT 1 FROM {t} LIMIT 1").fetchone()
               for t in ("recipes", "ingredient_lines", "urls"))


def migrate(conn: sqlite3.Connection, backup: Callable[[], object] | None = None) -> int:
    """Brings the database to CURRENT. `backup` is called once before the first migration that needs one;
    without it, such a migration is refused. Returns the number of migrations applied."""
    from .db import transaction

    todo = pending(conn)
    if not todo:
        return 0
    if any(m.backup for m in todo) and has_data(conn):
        if backup is None:
            raise SystemExit(f"Schema migration to version {CURRENT} needs a full backup first: set [backup] dir "
                             "in config.toml (outside iCloud).")
        log.info("full backup before migrating to schema version %d", CURRENT)
        backup()
    for m in todo:
        with transaction(conn):
            m.apply(conn)
            conn.execute(f"PRAGMA user_version = {m.version}")
        log.info("schema version %d: %s", m.version, m.description)
    return len(todo)
