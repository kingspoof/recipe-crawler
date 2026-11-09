"""Ingredient lines -> English name, amount, unit (ported from ingredient_processing.ipynb).

1. `lines`: every distinct ingredient line (lowercase, whitespace collapsed) goes into ingredient_lines, with how
   often it occurs and its most common original spelling.
2. `process`: the to-do lines go to Gemini (gemini.py), most common first; every answer is saved right away.
3. `map`: recipe_ingredients gets one row per ingredient of every recipe, with units cleaned up locally.

Tables (all created / migrated here, additively, on the first write command):
- ingredient_lines: notebook schema + `original` (nullable).
  status 0 = to do, 1 = done, -1 = model left it out max_attempts times, -2 = skipped (too long),
  -3 = gave up after repeated server errors on this line alone (safe to reset to 0).
  items: JSON [[name, amount, unit], ...] exactly as the model returned them; cleanup happens in `map`.
- recipe_ingredients: notebook schema, recipe_id = recipes.id.
- recipe_ingredients_state: one row per mapped recipe: when it was mapped and how many of its lines were still
  not done then. Incremental `map` only looks at recipes without a row or with pending > 0, and rebuilds those
  that have a line done after their mapped_at.
- ingredient_names: name -> canonical, filled by `names` (step 4) and applied in `map`. canonical '' = not food:
  `map` leaves the name out. A canonical can itself be a name with another canonical: chains are followed.

4. `names`: every distinct name (from the answers in ingredient_lines) that has no canonical yet goes to Gemini,
   which merges synonyms, regional names and spellings and drops filler words. The result is applied to
   recipe_ingredients right away (UPDATE / DELETE by name), so no `map --full` is needed.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import sqlite3
import time
from collections import Counter
from typing import Any, Callable
from .config import IngredientSettings
from . import schema
from .db import transaction as _tx
from .schema import RECIPE_INGREDIENTS
from .schema import columns as _columns
from .schema import has_table as _has_table
from .gemini import (GAVE_UP, PROMPT, Client, GeminiClient, Lane, Line, Task, UsageLog, fingerprint, next_reset,
                     quota_day, spool_read)

log = logging.getLogger("ingredients")

FULL_CHUNK = 5000  # recipes per transaction in `map --full`: the write lock is held for a moment at a time


def ensure_schema(conn: sqlite3.Connection) -> None:
    """Migrates to the current schema version (DB() already does this for write commands). See schema.py."""
    schema.migrate(conn)


def _spelling(line: object) -> str:
    return " ".join(str(line).split())


def norm(line: object) -> str:
    """The key lines are deduplicated on: lowercase, whitespace collapsed."""
    return _spelling(line).lower()


# --------------------------------------------------------------------------- step 1: lines


LINES_CHUNK = 50_000  # upserts per transaction in `lines`


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
    for i in range(0, len(rows), LINES_CHUNK):  # short transactions: `crawl` keeps writing in between
        with _tx(conn):
            conn.executemany(
                """INSERT INTO ingredient_lines (line, n, original) VALUES (?, ?, ?)
                   ON CONFLICT(line) DO UPDATE SET n = excluded.n, original = excluded.original""",
                rows[i:i + LINES_CHUNK],
            )
    with _tx(conn):
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


SAVE_BUSY_TIMEOUT_MS = 10_000  # per save try (3 tries): a stop must not wait minutes for a locked database


def make_save(conn: sqlite3.Connection):
    """The `process` connection only saves answers, so its busy timeout is shortened here."""
    conn.execute(f"PRAGMA busy_timeout = {SAVE_BUSY_TIMEOUT_MS}")

    def save(answered: list[tuple[int, list]], failed: list[int], model: str, gave_up: list[int] = ()) -> None:  # type: ignore[assignment]
        now = time.time()
        with _tx(conn):
            conn.executemany(
                "UPDATE ingredient_lines SET status = 1, items = ?, model = ?, done_at = ? WHERE id = ?",
                [(json.dumps(items, ensure_ascii=False), model, now, id) for id, items in answered],
            )
            conn.executemany("UPDATE ingredient_lines SET status = -1 WHERE id = ?", [(id,) for id in failed])
            conn.executemany(f"UPDATE ingredient_lines SET status = {GAVE_UP} WHERE id = ?", [(id,) for id in gave_up])
    return save


def build_lanes(cfg: IngredientSettings, keys: list[str], usage: UsageLog, used: Counter[tuple[str, str]],
                client: Callable[[str], Client] = GeminiClient) -> list[Lane]:
    """One lane per key x model, with today's usage; lanes that got a 429 PerDay today start exhausted."""
    lanes = [Lane(k, client(k), m, used[fingerprint(k), m], cfg.rpd) for m in cfg.models for k in keys]
    for lane in lanes:
        lane.exhausted = (lane.key, lane.model) in usage.exhausted
    return lanes


def dry_run(conn: sqlite3.Connection, cfg: IngredientSettings, keys: list[str], used: Counter[tuple[str, str]],
            show_prompt: bool = False) -> list[str]:
    """What `process` would send. Reads only: no migrations, no requests, no usage records."""
    if not _has_table(conn, "ingredient_lines"):
        raise SystemExit("No ingredient_lines table yet: run `recipe-crawler lines` first.")
    todo = todo_lines(conn)
    out = [f"{len(todo):,} lines to do = {-(-len(todo) // cfg.batch_size):,} requests of {cfg.batch_size}"]
    for k in keys:
        for m in cfg.models:
            out.append(f"  lane {fingerprint(k)}/{m}: {used[fingerprint(k), m]}/{cfg.rpd or '-'} used today")
    first = [line.text for line in todo[:cfg.batch_size]]
    out.append(f"first request ({len(first)} lines, prompt {len(PROMPT):,} chars):")
    out += [f"  [{i}, {json.dumps(text, ensure_ascii=False)}]" for i, text in enumerate(first[:20])]
    if len(first) > 20:
        out.append(f"  ... {len(first) - 20} more")
    if show_prompt:
        out.append(PROMPT)
    return out


def spool_path(cfg: IngredientSettings):
    return cfg.usage_log.parent / "unsaved_answers.jsonl"


def apply_spool(conn: sqlite3.Connection, cfg: IngredientSettings) -> int:
    """Saves the answers a previous `process` could not write to the database, then removes the file."""
    path = spool_path(cfg)
    records = spool_read(path)
    if not records and not path.exists():
        return 0
    save = make_save(conn)
    for answered, failed, model, gave_up in records:
        save(answered, failed, model, gave_up)
    path.unlink()
    n = sum(len(r[0]) for r in records)
    log.info("applied %s answers from %s", f"{n:,}", path)
    return n


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
    canonical = resolve(dict(conn.execute("SELECT name, canonical FROM ingredient_names")))
    stats = Counter({"recipes": 0, "rebuilt": 0, "rows": 0, "complete": 0})

    def rows_for(recipe_id: int, lines: list[str], done: dict[str, tuple[int, list, float]]) -> tuple[list, int]:
        rows, pending = [], 0
        for pos, line in enumerate(lines):
            if line in done:
                line_id, entries, _ = done[line]
                for entry in entries:
                    if isinstance(entry, list) and len(entry) == 3 and (c := clean(*entry)):
                        if name := canonical.get(c[0], c[0]):  # '' = not food
                            rows.append((recipe_id, pos, line_id, name, c[1], c[2]))
            elif line and len(line) <= cfg.max_line_chars:  # to do, or failed (can be reset to 0 by hand)
                pending += 1
        return rows, pending

    insert = "INSERT INTO recipe_ingredients VALUES (?, ?, ?, ?, ?, ?)"
    insert_state = "INSERT OR REPLACE INTO recipe_ingredients_state VALUES (?, ?, ?)"
    if full:
        _map_full(conn, rows_for, started, stats)
    else:
        candidates = conn.execute(
            """SELECT r.id, s.mapped_at FROM recipes r LEFT JOIN recipe_ingredients_state s ON s.recipe_id = r.id
               WHERE s.recipe_id IS NULL OR s.pending > 0""").fetchall()
        for i in range(0, len(candidates), 1000):
            with _tx(conn):  # one transaction per chunk: `crawl` and `process` can write in between
                _map_chunk(conn, candidates[i:i + 1000], rows_for, started, stats, insert, insert_state)
    log.info("%s rows for %s rebuilt recipes in %.0f s (%s recipes checked)", f"{stats['rows']:,}",
             f"{stats['rebuilt']:,}", time.time() - started, f"{stats['recipes']:,}")
    return dict(stats)


def _map_full(conn, rows_for, started, stats) -> None:
    """Builds recipe_ingredients_new (+ state) chunk by chunk, then swaps both in with one short transaction.
    Until the swap, readers see the old table; a crash leaves the old table untouched."""
    done = {line: (id, json.loads(items), done_at or 0) for id, line, items, done_at in conn.execute(
        "SELECT id, line, items, done_at FROM ingredient_lines WHERE status = 1")}
    suffix = time.time_ns()  # index names must be unique while the old table still exists
    with _tx(conn):
        conn.execute("DROP TABLE IF EXISTS recipe_ingredients_new")  # left over from a crashed run
        conn.execute("DROP TABLE IF EXISTS recipe_ingredients_state_new")
        conn.execute(RECIPE_INGREDIENTS.replace("IF NOT EXISTS recipe_ingredients", "recipe_ingredients_new"))
        conn.execute("CREATE TABLE recipe_ingredients_state_new "
                     "(recipe_id INTEGER PRIMARY KEY, mapped_at REAL NOT NULL, pending INTEGER NOT NULL)")
        # Indexes first, so each chunk keeps them up to date (no minute-long CREATE INDEX holding the lock).
        conn.execute(f"CREATE INDEX recipe_ingredients_recipe_{suffix} ON recipe_ingredients_new(recipe_id)")
        conn.execute(f"CREATE INDEX recipe_ingredients_name_{suffix} ON recipe_ingredients_new(name)")
    last = -1
    while True:
        chunk = conn.execute("SELECT id, ingredients FROM recipes WHERE id > ? ORDER BY id LIMIT ?",
                             (last, FULL_CHUNK)).fetchall()
        if not chunk:
            break
        last = chunk[-1][0]
        rows, states = [], []
        for recipe_id, ingredients in chunk:
            r, pending = rows_for(recipe_id, [norm(x) for x in json.loads(ingredients)], done)
            rows += r
            states.append((recipe_id, started, pending))
            stats["complete"] += pending == 0
        with _tx(conn):
            conn.executemany("INSERT INTO recipe_ingredients_new VALUES (?, ?, ?, ?, ?, ?)", rows)
            conn.executemany("INSERT INTO recipe_ingredients_state_new VALUES (?, ?, ?)", states)
        stats["recipes"] += len(chunk)
        stats["rows"] += len(rows)
    stats["rebuilt"] = stats["recipes"]
    with _tx(conn):
        conn.execute("DROP TABLE recipe_ingredients")
        conn.execute("DROP TABLE recipe_ingredients_state")
        conn.execute("ALTER TABLE recipe_ingredients_new RENAME TO recipe_ingredients")
        conn.execute("ALTER TABLE recipe_ingredients_state_new RENAME TO recipe_ingredients_state")


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


# --------------------------------------------------------------------------- step 4: names

NAMES_BATCH = 500  # names per request: ~12 output tokens each; big batches keep a group of variants together
NAMES_ROUNDS = 3  # new canonical names the model invents are sent again, until nothing new comes back

NAMES_PROMPT = """You standardize ingredient names from recipes, so that one ingredient always has one name.

The input is a JSON array of [index, name]. The names are already English, lowercase and singular (a few are not
translated yet: translate them, e.g. "air" is Malay for water). For every name return {"i": index, "c": standard name}:
- c: the name American recipes use for this ingredient, lowercase, singular, without brand, correctly spelled
  (turmeric, never "tumeric"; vanillin, never "vanilin"), also when a misspelled variant is in the input.
  Return the name unchanged if it is already the standard name.
- Synonyms, regional names and spelling variants get the same name. Use these:
  scallion (green onion, spring onion), cilantro (coriander leaf, fresh coriander, chinese parsley),
  powdered sugar (icing sugar, confectioners' sugar), superfine sugar (caster sugar), all-purpose flour (plain flour),
  cornstarch (corn starch), baking soda (bicarbonate of soda),
  eggplant (aubergine), zucchini (courgette), bell pepper (capsicum, sweet pepper), arugula (rocket),
  chickpea (garbanzo bean), shrimp (prawn), ground beef (minced beef, beef mince), panko (panko breadcrumb).
- Drop words that do not change what you buy: size (small, medium, large, big), temperature (warm, cold, lukewarm,
  tepid, room temperature), freshness and quality (fresh, good, ripe, best, homemade, store-bought), "of choice".
- Keep words that make it a different product: variety or color (red onion, green bell pepper, brown sugar,
  basmati rice), part
  (egg yolk, chicken breast, lemon zest), kind (olive oil, whole milk, smoked salmon, ground cinnamon vs cinnamon
  stick), processing that makes it a different product (dried shrimp, salted fish, smoked salmon, fermented
  shrimp paste, pickled ginger, canned tuna, cured ham), named varieties and origins (manzano pepper, turkish pepper, korean chili flakes, sweet paprika,
  smoked paprika). Never generalize to a broader ingredient: superfine sugar is not sugar, olive oil is not oil.
- A name with several ingredients ("salt and pepper", "parsley and green onion"): keep it unchanged, never pick one.
- If a name can mean different ingredients, keep it unchanged: "coriander" (leaf or seed), "pepper" (black pepper
  or bell pepper), "red pepper" (red bell pepper or chili flakes), "cream", "corn flour" (cornmeal or cornstarch).
- Names in the same input that mean the same ingredient must get exactly the same name.
- Not food (equipment, packaging, utensils, things that are not eaten: "ziplock bag", "toothpick", "kettle grill",
  "baking paper", "skewer"): c = null. Water, ice and salt are food, and so is everything that goes into the
  dish or flavors it: rice paper, tea bag, spice bag, bouquet garni, and edible decorations: chocolate pen,
  sprinkles, edible glitter. A lone number or letter ("1", "a") is not food. When in doubt, it is food.
Every index must appear in the output.

Example input:  [[0, "spring onion"], [1, "green onion"], [2, "icing sugar"], [3, "medium tomato"],
                 [4, "coriander leaf"], [5, "aluminium foil"], [6, "olive oil"], [7, "lukewarm water"],
                 [8, "coriander"], [9, "panko bread crumb"], [10, "brown sugar"], [11, "aubergine"],
                 [12, "green pepper"], [13, "rice paper"], [14, "tumeric"], [15, "turmeric"], [16, "salt and pepper"],
                 [17, "dried shrimp"], [18, "air"]]
Example output: [{"i":0,"c":"scallion"},{"i":1,"c":"scallion"},{"i":2,"c":"powdered sugar"},{"i":3,"c":"tomato"},{"i":4,"c":"cilantro"},{"i":5,"c":null},{"i":6,"c":"olive oil"},{"i":7,"c":"water"},{"i":8,"c":"coriander"},{"i":9,"c":"panko"},{"i":10,"c":"brown sugar"},{"i":11,"c":"eggplant"},{"i":12,"c":"green bell pepper"},{"i":13,"c":"rice paper"},{"i":14,"c":"turmeric"},{"i":15,"c":"turmeric"},{"i":16,"c":"salt and pepper"},{"i":17,"c":"dried shrimp"},{"i":18,"c":"water"}]

Input:
"""

NAMES_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {"i": {"type": "integer"}, "c": {"type": ["string", "null"]}},
        "required": ["i", "c"],
    },
}


def parse_names(n: int, result: Any) -> dict[int, str]:
    """{index: canonical} for every name answered; '' = not food. A name that is not a usable name (too long,
    not a string) counts as left out."""
    out: dict[int, str] = {}
    for item in result if isinstance(result, list) else []:
        i = item.get("i") if isinstance(item, dict) else None
        if not isinstance(i, int) or isinstance(i, bool) or not 0 <= i < n or i in out:
            continue
        c = item.get("c")
        if c is None:
            out[i] = ""
        elif isinstance(c, str) and (c := " ".join(c.split()).lower()) and len(c) <= 60 and c not in ("null", "none"):
            out[i] = c
    return out


NAMES = Task("names", NAMES_PROMPT, NAMES_SCHEMA, parse_names)


def resolve(mapping: dict[str, str]) -> dict[str, str]:
    """Follows chains (a -> b, b -> c gives a -> c). A cycle stops at the name where it closes."""
    out: dict[str, str] = {}
    for name in mapping:
        seen = {name}
        target = mapping[name]
        while target and target in mapping and mapping[target] != target and target not in seen:
            seen.add(target)
            target = mapping[target]
        out[name] = target
    return out


def name_counts(conn: sqlite3.Connection) -> Counter[str]:
    """Every name in the answers (after clean(), before ingredient_names), with how many recipe lines use it."""
    counts: Counter[str] = Counter()
    for items, n in conn.execute("SELECT items, n FROM ingredient_lines WHERE status = 1"):
        for entry in json.loads(items):
            if isinstance(entry, list) and len(entry) == 3 and (c := clean(*entry)):
                counts[c[0]] += n
    return counts


def todo_names(conn: sqlite3.Connection, counts: Counter[str] | None = None) -> list[Line]:
    """Names without a canonical yet: from the answers, plus canonical names that are not names themselves.
    Sorted by the last word, so variants of one ingredient ("icing sugar", "powdered sugar") share a request."""
    counts = Counter(counts if counts is not None else name_counts(conn))
    known = dict(conn.execute("SELECT name, canonical FROM ingredient_names"))
    for name, canonical in known.items():
        if canonical and canonical not in known:
            counts[canonical] += counts.get(name, 0) or 1
    todo = [(name, n) for name, n in counts.items() if name not in known]
    todo.sort(key=lambda x: (x[0].split()[-1], -x[1], x[0]))
    return [Line(name, name) for name, _ in todo]


def names_settings(cfg: IngredientSettings) -> IngredientSettings:
    return dataclasses.replace(cfg, batch_size=NAMES_BATCH)


def names_spool_path(cfg: IngredientSettings):
    return cfg.usage_log.parent / "unsaved_names.jsonl"


def make_save_names(conn: sqlite3.Connection):
    """Saves canonical names. Names the model left out stay to do (sent again next run), nothing is marked."""
    conn.execute(f"PRAGMA busy_timeout = {SAVE_BUSY_TIMEOUT_MS}")

    def save(answered: list[tuple[str, str]], failed: list[str], model: str, gave_up: list[str] = ()) -> None:  # type: ignore[assignment]
        with _tx(conn):
            conn.executemany("INSERT OR REPLACE INTO ingredient_names (name, canonical) VALUES (?, ?)", answered)
    return save


def apply_names_spool(conn: sqlite3.Connection, cfg: IngredientSettings) -> int:
    path = names_spool_path(cfg)
    records = spool_read(path)
    if not records and not path.exists():
        return 0
    save = make_save_names(conn)
    for answered, _, model, _ in records:
        save(answered, [], model)
    path.unlink()
    n = sum(len(r[0]) for r in records)
    log.info("applied %s names from %s", f"{n:,}", path)
    return n


APPLY_CHUNK = 200  # names per transaction when renaming recipe_ingredients rows


def apply_names(conn: sqlite3.Connection) -> dict[str, int]:
    """Renames (or, for '' = not food, deletes) recipe_ingredients rows by name, through the name index.
    Short transactions, so `crawl` and `process` can write in between."""
    ensure_schema(conn)
    t = time.time()
    canonical = resolve(dict(conn.execute("SELECT name, canonical FROM ingredient_names")))
    present = {name for (name,) in conn.execute("SELECT DISTINCT name FROM recipe_ingredients")}
    changes = [(name, c) for name, c in canonical.items() if c != name and name in present]
    stats = Counter({"names": 0, "renamed": 0, "deleted": 0})
    for i in range(0, len(changes), APPLY_CHUNK):
        with _tx(conn):
            for name, c in changes[i:i + APPLY_CHUNK]:
                if c:
                    stats["renamed"] += conn.execute(
                        "UPDATE recipe_ingredients SET name = ? WHERE name = ?", (c, name)).rowcount
                else:
                    stats["deleted"] += conn.execute(
                        "DELETE FROM recipe_ingredients WHERE name = ?", (name,)).rowcount
                stats["names"] += 1
    log.info("recipe_ingredients: %s names changed, %s rows renamed, %s rows deleted (not food) in %.0f s",
             f"{stats['names']:,}", f"{stats['renamed']:,}", f"{stats['deleted']:,}", time.time() - t)
    return dict(stats)


def names_dry_run(conn: sqlite3.Connection, cfg: IngredientSettings, show_prompt: bool = False) -> list[str]:
    """What `names` would send. Reads only."""
    if not _has_table(conn, "ingredient_lines"):
        raise SystemExit("No ingredient_lines table yet: run `recipe-crawler lines` first.")
    todo = todo_names(conn)
    out = [f"{len(todo):,} names to do = {-(-len(todo) // NAMES_BATCH):,} requests of {NAMES_BATCH} (first round)"]
    first = [line.text for line in todo[:NAMES_BATCH]]
    out.append(f"first request ({len(first)} names, prompt {len(NAMES_PROMPT):,} chars):")
    out += [f"  [{i}, {json.dumps(text, ensure_ascii=False)}]" for i, text in enumerate(first[:30])]
    if len(first) > 30:
        out.append(f"  ... {len(first) - 30} more")
    if show_prompt:
        out.append(NAMES_PROMPT)
    return out


def names_report(conn: sqlite3.Connection, limit: int = 25) -> list[str]:
    """How far `names` is, and the biggest merges (by recipe lines)."""
    if not _has_table(conn, "ingredient_lines"):
        return ["no ingredient_lines yet: run `recipe-crawler lines`"]
    counts = name_counts(conn)
    canonical = resolve(dict(conn.execute("SELECT name, canonical FROM ingredient_names")))
    done = [name for name in counts if name in canonical]
    merged = [name for name in done if canonical[name] and canonical[name] != name]
    dropped = [name for name in done if not canonical[name]]
    groups: dict[str, list[str]] = {}
    for name in merged:
        groups.setdefault(canonical[name], []).append(name)
    total = max(sum(counts.values()), 1)
    out = [
        f"{len(counts):,} distinct names in the answers: {len(done):,} with a canonical name "
        f"({sum(counts[n] for n in done) / total:.1%} of the recipe lines), {len(counts) - len(done):,} to do",
        f"  merged into another name: {len(merged):,} names, {sum(counts[n] for n in merged):,} recipe lines",
        f"  not food (left out):      {len(dropped):,} names, {sum(counts[n] for n in dropped):,} recipe lines",
        f"  -> {len({canonical.get(n, n) for n in counts} - {''}):,} distinct names after merging",
        "biggest merges:",
    ]
    by_lines = sorted(groups.items(), key=lambda g: -sum(counts[n] for n in g[1]))
    for target, names in by_lines[:limit]:
        names.sort(key=lambda n: -counts[n])
        shown = ", ".join(f"{n} ({counts[n]:,})" for n in names[:6]) + (f", +{len(names) - 6}" if len(names) > 6 else "")
        out.append(f"  {target} <- {shown}")
    if dropped:
        dropped.sort(key=lambda n: -counts[n])
        out.append("not food: " + ", ".join(f"{n} ({counts[n]:,})" for n in dropped[:15]))
    return out


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
    for status, name in [(1, "done"), (0, "to do"), (-1, "failed"), (GAVE_UP, "gave up (server errors)"),
                         (-2, "skipped (too long)")]:
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
