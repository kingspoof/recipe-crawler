"""Diet, food group and allergens per ingredient (ingredient_info), with Gemini.

`classify` sends every name in recipe_ingredients that has no row in ingredient_info yet (most used first) and
saves the answers right away. It shares the quota, lanes and usage log with `process` and `names`.

- diet: vegan (no animal products) | vegetarian (dairy, egg, honey) | pescatarian (fish, seafood) | meat |
  unknown (the usual versions differ: stock, broth, bouillon). Judged on the usual store-bought version.
- food_group: one of FOOD_GROUPS.
- allergens: the 14 EU allergens (Regulation 1169/2011, Annex II) the usual version contains.

Recipes: recipe_diets() / recipe_allergens() (also as SQL in RECIPE_DIET_SQL / RECIPE_ALLERGENS_SQL, for
notebooks). Not a view on purpose: `map --full` swaps recipe_ingredients, and SQLite refuses to rename a table
while a view refers to a dropped one.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import sqlite3
import time
from collections import Counter
from typing import Any

from .config import IngredientSettings
from .db import transaction as _tx
from .gemini import Line, Task, spool_read
from .ingredients import SAVE_BUSY_TIMEOUT_MS, ensure_schema
from .schema import has_table as _has_table

log = logging.getLogger("classify")

BATCH = 400  # names per request: ~25 output tokens each

DIETS = ("vegan", "vegetarian", "pescatarian", "meat", "unknown")
FOOD_GROUPS = ("vegetable", "fruit", "grain", "legume", "nut_seed", "dairy", "egg", "meat", "poultry", "seafood",
               "fat_oil", "herb_spice", "sweetener", "sauce_condiment", "beverage", "alcohol", "other")
ALLERGENS = ("gluten", "crustacean", "egg", "fish", "peanut", "soy", "milk", "tree_nut", "celery", "mustard",
             "sesame", "sulphite", "lupin", "mollusc")

PROMPT = f"""You classify recipe ingredients.

The input is a JSON array of [index, ingredient]; the names are English, lowercase and singular.
For every ingredient return {{"i": index, "d": diet, "g": food group, "a": [allergens]}}, judged on the usual
store-bought version of the ingredient:
- d: "vegan" = no animal products (sugar, margarine, dried pasta, bread, wine count as vegan).
  "vegetarian" = contains dairy, egg or honey but no meat or fish (butter, cheese of any kind, mayonnaise, pesto).
  "pescatarian" = contains fish or seafood but no other meat (anchovy, fish sauce, oyster sauce, worcestershire
  sauce, dashi, shrimp paste).
  "meat" = contains meat, poultry or slaughter products (bacon, lard, suet, gelatin, chicken stock, beef broth).
  "unknown" = only when the usual versions differ: unspecified stock, broth, bouillon cube, "meat substitute",
  and dishes or mixes whose recipe varies (sandwich, stew base, curry sauce) unless the name says what is in it
  ("chicken curry sauce" is meat, "lemon butter sauce" is vegetarian).
- g: the main group, one of {", ".join(FOOD_GROUPS)}.
  meat = red meat and meat products (bacon, sausage, ham); poultry = chicken, turkey, duck; seafood = fish and
  seafood; legume = beans, lentils, peas, chickpeas, tofu, soy milk; nut_seed = nuts and seeds, also peanut and
  sesame; herb_spice = herbs, spices, salt, pepper, spice mixes; sauce_condiment = sauces, pastes, vinegar,
  stock, mustard, ketchup; grain = flour, rice, pasta, bread, oats, starch; other = everything else (yeast,
  baking powder, gelatin, food coloring, water).
- a: the EU allergens the usual version contains, from: {", ".join(ALLERGENS)}. [] if none.
  gluten = wheat, rye, barley, spelt, oats and products (flour, pasta, bread, soy sauce, beer); milk = all dairy
  (butter, cheese, cream, yogurt, whey); tree_nut = almond, hazelnut, walnut, cashew, pecan, pistachio,
  macadamia, brazil nut (not peanut, not coconut); crustacean = shrimp, crab, lobster; mollusc = squid, octopus,
  clam, mussel, oyster, snail; sulphite = wine, dried fruit with sulphite, vinegar from wine.
Every index must appear in the output.

Example input:  [[0, "butter"], [1, "chicken stock"], [2, "fish sauce"], [3, "flour"], [4, "stock"],
                 [5, "gelatin"], [6, "honey"], [7, "soy sauce"], [8, "tofu"], [9, "parmesan"], [10, "shrimp"],
                 [11, "almond"], [12, "salt"], [13, "pesto"]]
Example output: [{{"i":0,"d":"vegetarian","g":"dairy","a":["milk"]}},{{"i":1,"d":"meat","g":"sauce_condiment","a":["celery"]}},{{"i":2,"d":"pescatarian","g":"sauce_condiment","a":["fish"]}},{{"i":3,"d":"vegan","g":"grain","a":["gluten"]}},{{"i":4,"d":"unknown","g":"sauce_condiment","a":["celery"]}},{{"i":5,"d":"meat","g":"other","a":[]}},{{"i":6,"d":"vegetarian","g":"sweetener","a":[]}},{{"i":7,"d":"vegan","g":"sauce_condiment","a":["soy","gluten"]}},{{"i":8,"d":"vegan","g":"legume","a":["soy"]}},{{"i":9,"d":"vegetarian","g":"dairy","a":["milk"]}},{{"i":10,"d":"pescatarian","g":"seafood","a":["crustacean"]}},{{"i":11,"d":"vegan","g":"nut_seed","a":["tree_nut"]}},{{"i":12,"d":"vegan","g":"herb_spice","a":[]}},{{"i":13,"d":"vegetarian","g":"sauce_condiment","a":["milk","tree_nut"]}}]

Input:
"""

SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "i": {"type": "integer"},
            "d": {"type": "string", "enum": list(DIETS)},
            "g": {"type": "string", "enum": list(FOOD_GROUPS)},
            "a": {"type": "array", "items": {"type": "string", "enum": list(ALLERGENS)}},
        },
        "required": ["i", "d", "g", "a"],
    },
}


def parse(n: int, result: Any) -> dict[int, tuple[str, str, list[str]]]:
    """{index: (diet, food group, allergens)}; an answer with an unknown diet or group counts as left out,
    unknown allergens are dropped."""
    out: dict[int, tuple[str, str, list[str]]] = {}
    for item in result if isinstance(result, list) else []:
        i = item.get("i") if isinstance(item, dict) else None
        if not isinstance(i, int) or isinstance(i, bool) or not 0 <= i < n or i in out:
            continue
        diet, group, allergens = item.get("d"), item.get("g"), item.get("a")
        if diet in DIETS and group in FOOD_GROUPS:
            found = set(allergens) if isinstance(allergens, list) else set()
            out[i] = (diet, group, [a for a in ALLERGENS if a in found])
    return out


TASK = Task("classify", PROMPT, SCHEMA, parse)


def settings(cfg: IngredientSettings) -> IngredientSettings:
    return dataclasses.replace(cfg, batch_size=BATCH)


def spool_path(cfg: IngredientSettings):
    return cfg.usage_log.parent / "unsaved_classify.jsonl"


def todo(conn: sqlite3.Connection) -> list[Line]:
    """Names in recipe_ingredients without ingredient_info, most used first."""
    known = {name for (name,) in conn.execute("SELECT name FROM ingredient_info")} \
        if _has_table(conn, "ingredient_info") else set()
    rows = conn.execute("SELECT name, COUNT(*) FROM recipe_ingredients GROUP BY name").fetchall()
    rows.sort(key=lambda r: (-r[1], r[0]))
    return [Line(name, name) for name, _ in rows if name not in known]


def make_save(conn: sqlite3.Connection):
    """Saves the answers. Names the model left out stay to do (sent again next run)."""
    conn.execute(f"PRAGMA busy_timeout = {SAVE_BUSY_TIMEOUT_MS}")

    def save(answered: list[tuple[str, Any]], failed: list[str], model: str, gave_up: list[str] = ()) -> None:  # type: ignore[assignment]
        now = time.time()
        with _tx(conn):
            conn.executemany(
                "INSERT OR REPLACE INTO ingredient_info (name, diet, food_group, allergens, model, done_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                [(name, d, g, json.dumps(list(a)), model, now) for name, (d, g, a) in answered])
    return save


def apply_spool(conn: sqlite3.Connection, cfg: IngredientSettings) -> int:
    path = spool_path(cfg)
    records = spool_read(path)
    if not records and not path.exists():
        return 0
    save = make_save(conn)
    for answered, _, model, _ in records:
        save(answered, [], model)
    path.unlink()
    n = sum(len(r[0]) for r in records)
    log.info("applied %s answers from %s", f"{n:,}", path)
    return n


def dry_run(conn: sqlite3.Connection, show_prompt: bool = False) -> list[str]:
    if not _has_table(conn, "recipe_ingredients"):
        raise SystemExit("No recipe_ingredients yet: run `recipe-crawler map` first.")
    names = todo(conn)
    out = [f"{len(names):,} ingredients to classify = {-(-len(names) // BATCH):,} requests of {BATCH}"]
    out += [f"  [{i}, {json.dumps(line.text, ensure_ascii=False)}]" for i, line in enumerate(names[:20])]
    if len(names) > 20:
        out.append(f"  ... {len(names) - 20:,} more")
    if show_prompt:
        out.append(PROMPT)
    return out


# --------------------------------------------------------------------------- recipes

# A recipe is as restrictive as its least restrictive ingredient. An ingredient without ingredient_info, or with
# diet unknown, makes the recipe unknown unless it already contains meat; so do lines not processed yet.
RECIPE_DIET_SQL = """
SELECT ri.recipe_id,
       CASE WHEN MAX(i.diet = 'meat') THEN 'meat'
            WHEN MAX(i.diet IS NULL OR i.diet = 'unknown') OR MAX(COALESCE(s.pending, 0) > 0) THEN 'unknown'
            WHEN MAX(i.diet = 'pescatarian') THEN 'pescatarian'
            WHEN MAX(i.diet = 'vegetarian') THEN 'vegetarian'
            ELSE 'vegan' END AS diet
FROM recipe_ingredients ri
LEFT JOIN ingredient_info i ON i.name = ri.name
LEFT JOIN recipe_ingredients_state s ON s.recipe_id = ri.recipe_id
GROUP BY ri.recipe_id
"""

RECIPE_ALLERGENS_SQL = """
SELECT ri.recipe_id, group_concat(DISTINCT a.value) AS allergens
FROM recipe_ingredients ri
JOIN ingredient_info i ON i.name = ri.name, json_each(i.allergens) a
GROUP BY ri.recipe_id
"""


def recipe_diets(conn: sqlite3.Connection) -> dict[int, str]:
    """recipe id -> diet, for every recipe with at least one mapped ingredient."""
    return dict(conn.execute(RECIPE_DIET_SQL))


def recipe_allergens(conn: sqlite3.Connection) -> dict[int, set[str]]:
    """recipe id -> allergens, for every recipe with at least one."""
    return {id: set(a.split(",")) for id, a in conn.execute(RECIPE_ALLERGENS_SQL)}


def info(conn: sqlite3.Connection) -> dict[str, tuple[str, str, list[str]]]:
    """name -> (diet, food group, allergens); empty before the migration."""
    if not _has_table(conn, "ingredient_info"):
        return {}
    return {name: (d, g, json.loads(a)) for name, d, g, a in conn.execute(
        "SELECT name, diet, food_group, allergens FROM ingredient_info")}


def report(conn: sqlite3.Connection, examples: int = 8) -> list[str]:
    if not _has_table(conn, "ingredient_info") or not _has_table(conn, "recipe_ingredients"):
        return ["no ingredient_info yet: run `recipe-crawler classify`"]
    uses = dict(conn.execute("SELECT name, COUNT(*) FROM recipe_ingredients GROUP BY name"))
    known = info(conn)
    done = [n for n in uses if n in known]
    total = max(sum(uses.values()), 1)
    out = [f"{len(uses):,} ingredients: {len(done):,} classified "
           f"({sum(uses[n] for n in done) / total:.1%} of the recipe ingredient rows), {len(uses) - len(done):,} to do"]
    for title, k, values in (("diet", 0, DIETS), ("food group", 1, FOOD_GROUPS)):
        out.append(f"by {title} (ingredients / rows, most used):")
        for v in values:
            names = sorted((n for n in done if known[n][k] == v), key=lambda n: -uses[n])
            if names:
                out.append(f"  {v:16} {len(names):>7,} / {sum(uses[n] for n in names):>9,}  "
                           + ", ".join(names[:examples]))
    allergens = Counter(a for n in done for a in known[n][2] for _ in range(uses[n]))
    out.append("allergens (rows): " + ", ".join(f"{a} {c:,}" for a, c in allergens.most_common()))
    diets = Counter(recipe_diets(conn).values())
    n = max(sum(diets.values()), 1)
    out.append("recipes by diet: " + ", ".join(f"{d} {diets[d]:,} ({diets[d] / n:.1%})" for d in DIETS))
    return out
