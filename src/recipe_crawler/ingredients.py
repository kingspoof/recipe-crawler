"""Ingredient lines -> English name, amount, unit (ported from ingredient_processing.ipynb).

1. `lines`: every distinct ingredient line (lowercase, whitespace collapsed) goes into ingredient_lines, with how
   often it occurs and its most common original spelling.
2. `process`: the to-do lines go to Gemini (gemini.py), most common first; every answer is saved right away.
3. `map`: recipe_ingredients gets one row per ingredient of every recipe, with units cleaned up locally.

Tables (all created / migrated here, additively, on the first write command):
- ingredient_lines: notebook schema + `original` (nullable).
  status 0 = to do, 1 = done, -1 = model left it out max_attempts times, -2 = skipped (too long).
  items: JSON [[name, amount, unit], ...] exactly as the model returned them; cleanup happens in `map`.
- recipe_ingredients: notebook schema, recipe_id = recipes.id.
- recipe_ingredients_state: one row per mapped recipe: when it was mapped and how many of its lines were still
  not done then. Incremental `map` only looks at recipes without a row or with pending > 0, and rebuilds those
  that have a line done after their mapped_at.
- ingredient_names: name -> canonical, applied in `map` (filled by a future `names` step).
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from collections import Counter
from contextlib import contextmanager
from typing import Iterator

from .config import IngredientSettings
from .gemini import Line, UsageLog, fingerprint, next_reset, quota_day

log = logging.getLogger("ingredients")

SCHEMA = """
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
RECIPE_INGREDIENTS_INDEXES = """
CREATE INDEX IF NOT EXISTS recipe_ingredients_recipe ON recipe_ingredients(recipe_id);
CREATE INDEX IF NOT EXISTS recipe_ingredients_name ON recipe_ingredients(name);
"""


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Additive and idempotent: never drops anything, never touches done lines."""
    conn.executescript(SCHEMA + RECIPE_INGREDIENTS + RECIPE_INGREDIENTS_INDEXES)
    if "original" not in _columns(conn, "ingredient_lines"):
        conn.execute("ALTER TABLE ingredient_lines ADD COLUMN original TEXT")  # schema only, instant on 2M rows
        log.info("migration: added ingredient_lines.original")


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)).fetchone() is not None


@contextmanager
def _tx(conn: sqlite3.Connection) -> Iterator[None]:
    """One transaction (DB opens the connection in autocommit mode)."""
    conn.execute("BEGIN")
    try:
        yield
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")


def _spelling(line: object) -> str:
    return " ".join(str(line).split())


def norm(line: object) -> str:
    """The key lines are deduplicated on: lowercase, whitespace collapsed."""
    return _spelling(line).lower()


# --------------------------------------------------------------------------- step 1: lines


def collect_lines(conn: sqlite3.Connection, cfg: IngredientSettings) -> None:
    """Adds new lines, updates counts and original spellings. Status, items etc. of existing lines stay."""
    ensure_schema(conn)
    t = time.time()
    counts: Counter[str] = Counter()
    spellings: Counter[str] = Counter()  # only spellings that differ from the lowercase key
    for (ingredients,) in conn.execute("SELECT ingredients FROM recipes"):
        for raw in json.loads(ingredients):
            if line := _spelling(raw):
                key = line.lower()
                counts[key] += 1
                if line != key:
                    spellings[line] += 1
    # Most common spelling per key; the lowercase key itself counts as a spelling too (-> original NULL).
    best: dict[str, tuple[int, str]] = {}
    other: Counter[str] = Counter()  # per key: how often it is written in some non-lowercase spelling
    for line, n in spellings.items():
        key = line.lower()
        other[key] += n
        if n > best.get(key, (0, ""))[0]:
            best[key] = (n, line)
    del spellings
    rows = []
    for key, n in counts.items():
        b = best.get(key)
        rows.append((key, n, b[1] if b and b[0] > n - other[key] else None))  # beats the lowercase spelling
    del counts, best, other
    with _tx(conn):
        conn.executemany(
            """INSERT INTO ingredient_lines (line, n, original) VALUES (?, ?, ?)
               ON CONFLICT(line) DO UPDATE SET n = excluded.n, original = excluded.original""",
            rows,
        )
        conn.execute("UPDATE ingredient_lines SET status = -2 WHERE status = 0 AND length(line) > ?",
                     (cfg.max_line_chars,))
    log.info("%s distinct lines in %.0f s", f"{len(rows):,}", time.time() - t)
    for row in progress(conn):
        log.info("%s", row)


# --------------------------------------------------------------------------- step 2: process (scheduler in gemini.py)


def todo_lines(conn: sqlite3.Connection) -> list[Line]:
    """To-do lines, most common first, with the spelling to send. Works before the migration too (dry run)."""
    text = "COALESCE(original, line)" if "original" in _columns(conn, "ingredient_lines") else "line"
    return [Line(id, t) for id, t in conn.execute(
        f"SELECT id, {text} FROM ingredient_lines WHERE status = 0 ORDER BY n DESC")]


def make_save(conn: sqlite3.Connection):
    def save(answered: list[tuple[int, list]], failed: list[int], model: str) -> None:
        now = time.time()
        with _tx(conn):
            conn.executemany(
                "UPDATE ingredient_lines SET status = 1, items = ?, model = ?, done_at = ? WHERE id = ?",
                [(json.dumps(items, ensure_ascii=False), model, now, id) for id, items in answered],
            )
            conn.executemany("UPDATE ingredient_lines SET status = -1 WHERE id = ?", [(id,) for id in failed])
    return save


# --------------------------------------------------------------------------- step 3: map

# unit -> (factor for the amount, new unit). Everything not listed stays as the model returned it.
# Not mapped on purpose: a bare "t" (tsp or tbsp) and "c" (cup?).
UNIT_FIX = {
    "dag": (10, "g"), "dkg": (10, "g"), "gr": (1, "g"), "gram": (1, "g"), "grams": (1, "g"), "gramm": (1, "g"),
    "kilogram": (1, "kg"), "cl": (10, "ml"), "dl": (100, "ml"), "liter": (1, "l"), "litre": (1, "l"),
    "spoon": (1, "tbsp"), "tablespoon": (1, "tbsp"), "ss": (1, "tbsp"), "el": (1, "tbsp"), "msk": (1, "tbsp"),
    "teaspoon": (1, "tsp"), "tl": (1, "tsp"), "tsk": (1, "tsp"), "krm": (1, "ml"), "cc": (1, "ml"),
    "pieces": (1, "piece"), "pcs": (1, "piece"), "pc": (1, "piece"), "null": (1, None), "none": (1, None),
    # seen in the mapped data of the first run
    "ek": (1, "tbsp"), "pl": (1, "tbsp"), "cs": (1, "tbsp"), "c.s.": (1, "tbsp"), "tbs": (1, "tbsp"),
    "tbl": (1, "tbsp"), "tk": (1, "tsp"), "hrst": (1, "handful"), "ks": (1, "piece"), "stk": (1, "piece"),
    "st": (1, "piece"),
}
# Amounts above this (after the unit conversion) are typos like "500000 g aubergine": amount dropped, row kept.
MAX_AMOUNT = {"g": 50_000, "ml": 50_000}
MAX_AMOUNT_ANY = 100_000


def clean(name: object, amount: object, unit: object) -> tuple[str, float | None, str | None] | None:
    name = " ".join(name.split()).lower() if isinstance(name, str) else None
    if not name or name in ("null", "none") or len(name) > 60:
        return None
    if not isinstance(amount, (int, float)) or isinstance(amount, bool) or amount < 0:
        amount = None
    unit = " ".join(unit.split()).lower() if isinstance(unit, str) and unit.strip() else None
    if unit in UNIT_FIX:
        factor, unit = UNIT_FIX[unit]
        amount = amount * factor if amount is not None else None
    if amount is not None and (amount > MAX_AMOUNT_ANY or amount > MAX_AMOUNT.get(unit, MAX_AMOUNT_ANY)):  # type: ignore[arg-type]
        amount = None
    return name, amount, unit  # type: ignore[return-value]


def map_ingredients(conn: sqlite3.Connection, cfg: IngredientSettings, full: bool = False) -> dict[str, int]:
    """Builds recipe_ingredients. Incremental unless full (or there is no map state yet, e.g. the first run)."""
    ensure_schema(conn)
    started = time.time()  # mapped_at: lines done while this runs are picked up again next time
    if not full and conn.execute("SELECT 1 FROM recipe_ingredients_state LIMIT 1").fetchone() is None:
        log.info("no map state yet: full rebuild")
        full = True
    canonical = dict(conn.execute("SELECT name, canonical FROM ingredient_names"))
    stats = Counter({"recipes": 0, "rebuilt": 0, "rows": 0, "complete": 0})

    def rows_for(recipe_id: int, lines: list[str], done: dict[str, tuple[int, list, float]]) -> tuple[list, int]:
        rows, pending = [], 0
        for pos, line in enumerate(lines):
            if line in done:
                line_id, entries, _ = done[line]
                for entry in entries:
                    if isinstance(entry, list) and len(entry) == 3 and (c := clean(*entry)):
                        rows.append((recipe_id, pos, line_id, canonical.get(c[0], c[0]), c[1], c[2]))
            elif line and len(line) <= cfg.max_line_chars:  # to do, or failed (can be reset to 0 by hand)
                pending += 1
        return rows, pending

    insert = "INSERT INTO recipe_ingredients VALUES (?, ?, ?, ?, ?, ?)"
    insert_state = "INSERT OR REPLACE INTO recipe_ingredients_state VALUES (?, ?, ?)"
    with _tx(conn):
        if full:
            done = {line: (id, json.loads(items), done_at or 0) for id, line, items, done_at in conn.execute(
                "SELECT id, line, items, done_at FROM ingredient_lines WHERE status = 1")}
            conn.execute("DROP TABLE IF EXISTS recipe_ingredients")
            conn.execute(RECIPE_INGREDIENTS)
            conn.execute("DELETE FROM recipe_ingredients_state")
            rows, states = [], []
            for recipe_id, ingredients in conn.execute("SELECT id, ingredients FROM recipes"):
                r, pending = rows_for(recipe_id, [norm(x) for x in json.loads(ingredients)], done)
                rows += r
                states.append((recipe_id, started, pending))
                stats["recipes"] += 1
                stats["complete"] += pending == 0
                if len(rows) > 200_000:
                    conn.executemany(insert, rows)
                    stats["rows"] += len(rows)
                    rows = []
            conn.executemany(insert, rows)
            conn.executemany(insert_state, states)
            stats["rows"] += len(rows)
            stats["rebuilt"] = stats["recipes"]
            del done, states
            for statement in RECIPE_INGREDIENTS_INDEXES.strip().split(";")[:-1]:  # no executescript: it commits
                conn.execute(statement)
        else:
            candidates = conn.execute(
                """SELECT r.id, s.mapped_at FROM recipes r LEFT JOIN recipe_ingredients_state s ON s.recipe_id = r.id
                   WHERE s.recipe_id IS NULL OR s.pending > 0""").fetchall()
            for i in range(0, len(candidates), 1000):
                _map_chunk(conn, candidates[i:i + 1000], rows_for, started, stats, insert, insert_state)
    log.info("%s rows for %s rebuilt recipes in %.0f s (%s recipes checked)", f"{stats['rows']:,}",
             f"{stats['rebuilt']:,}", time.time() - started, f"{stats['recipes']:,}")
    return dict(stats)


def _map_chunk(conn, chunk, rows_for, started, stats, insert, insert_state) -> None:
    mapped_at = dict(chunk)
    recipes = {id: [norm(x) for x in json.loads(ing)] for id, ing in conn.execute(
        f"SELECT id, ingredients FROM recipes WHERE id IN ({','.join('?' * len(chunk))})", list(mapped_at))}
    needed = list({line for lines in recipes.values() for line in lines if line})
    done: dict[str, tuple[int, list, float]] = {}
    for i in range(0, len(needed), 500):
        part = needed[i:i + 500]
        # status checked here, not in SQL: "status = 1" makes SQLite use the status index and scan 2M rows
        for id, line, status, items, done_at in conn.execute(
                f"SELECT id, line, status, items, done_at FROM ingredient_lines WHERE line IN "
                f"({','.join('?' * len(part))})", part):
            if status == 1:
                done[line] = (id, json.loads(items), done_at or 0)
    for recipe_id, lines in recipes.items():
        stats["recipes"] += 1
        since = mapped_at[recipe_id]
        if since is not None and not any(line in done and done[line][2] > since for line in lines):
            continue  # nothing new for this recipe
        rows, pending = rows_for(recipe_id, lines, done)
        conn.execute("DELETE FROM recipe_ingredients WHERE recipe_id = ?", (recipe_id,))
        conn.executemany(insert, rows)
        conn.execute(insert_state, (recipe_id, started, pending))
        stats["rebuilt"] += 1
        stats["rows"] += len(rows)
        stats["complete"] += pending == 0


# --------------------------------------------------------------------------- names (next step)


def names_report(conn: sqlite3.Connection) -> list[str]:
    """Stub of the name standardization step: shows what there is to merge. Merging itself is not built yet."""
    ensure_schema(conn)
    distinct, rows = conn.execute("SELECT COUNT(DISTINCT name), COUNT(*) FROM recipe_ingredients").fetchone()
    mapped = conn.execute("SELECT COUNT(*) FROM ingredient_names").fetchone()[0]
    top = conn.execute("SELECT name, COUNT(*) FROM recipe_ingredients GROUP BY 1 ORDER BY 2 DESC LIMIT 20").fetchall()
    return [
        f"{distinct:,} distinct names in {rows:,} recipe_ingredients rows, {mapped:,} with a canonical name",
        "most common: " + ", ".join(f"{n} ({c:,})" for n, c in top),
        "Merging names into canonical ones (ingredient_names) is not implemented yet: fill the table by hand or "
        "with a future `names --merge`, then run `map --full`.",
    ]


# --------------------------------------------------------------------------- stats


def progress(conn: sqlite3.Connection) -> list[str]:
    """The progress table of notebook cell 7."""
    if not _has_table(conn, "ingredient_lines"):
        return ["no ingredient_lines yet: run `recipe-crawler lines`"]
    rows = {s: (c, n or 0) for s, c, n in conn.execute(
        "SELECT status, COUNT(*), SUM(n) FROM ingredient_lines GROUP BY 1")}
    all_lines = max(sum(c for c, _ in rows.values()), 1)
    all_uses = max(sum(n for _, n in rows.values()), 1)
    out = []
    for status, name in [(1, "done"), (0, "to do"), (-1, "failed"), (-2, "skipped (too long)")]:
        c, n = rows.get(status, (0, 0))
        out.append(f"{name:20} {c:>10,} distinct lines ({c / all_lines:6.1%})   "
                   f"covering {n:>10,} recipe lines ({n / all_uses:6.1%})")
    return out


def quota_report(cfg: IngredientSettings, keys: list[str]) -> list[str]:
    usage = UsageLog(cfg.usage_log)
    used = usage.used_today(keys)
    reset = next_reset()
    left = reset - reset.now(reset.tzinfo)
    out = [f"quota day {quota_day()} (America/Los_Angeles), next reset {reset:%a %H:%M} local, "
           f"in {left.seconds // 3600}h {left.seconds % 3600 // 60:02}m"]
    lanes = {(fingerprint(k), m) for k in keys for m in cfg.models} | set(used)
    for key, model in sorted(lanes, key=str):
        out.append(f"  {key}/{model}: {used.get((key, model), 0)}/{cfg.rpd or '-'} requests today"
                   + (" (429 per day: used up)" if (key, model) in usage.exhausted else ""))
    if not keys:
        out.append(f"  (no keys file at {cfg.keys_file}: old records are shown by line index)")
    return out
