# Task: ingredient processing CLI

Work on a new branch `ingredient-cli`, push it, and open a PR against main.

## Goal
Turn `ingredient_processing.ipynb` (read it fully: cells 1-8) into package code + CLI subcommands, as described in
TODO.md section 1. The notebook logic is proven on 2M lines; port it faithfully, then apply the fixes listed below.
Match the style of the existing code in src/recipe_crawler (argparse subcommands in cli.py, settings dataclass in
config.py loaded from config.toml, short comments, `from __future__ import annotations`). Do not touch the crawler
logic. Do not modify or delete the notebooks.

The real database (7 GB), gemini_keys.txt and the Gemini quota are only on the user's Mac. Build against small test
databases and a fake Gemini client. The user runs it for real locally afterwards.

## Facts about the real database (data.nosync/recipes.db)
- `recipes` has `id INTEGER PRIMARY KEY` (= old rowid, migrated 2026-10-30) and `url TEXT NOT NULL UNIQUE`; see
  db.py SCHEMA. Use `recipes.id`, never `rowid`. The real table also has legacy columns (ingredients_processed,
  ingredients_clean, detected_language, recipes_ingredients_processed) and a legacy `ingredients` table from older
  notebooks: ignore them, don't drop them.
- `ingredient_lines` (schema in notebook cell 1) has 2,065,251 rows: status 1 = 2,063,876 done, -2 = 1,374 skipped
  (>200 chars), -1 = 1 failed. `items` is JSON [[name, amount, unit], ...] as the model returned it,
  `model` = "gemini-3.5-flash-lite".
- `recipe_ingredients` (schema in notebook cell 8) has 6.07M rows for 604,741 recipes; `recipe_id` references
  recipes.id. Indexes recipe_ingredients_recipe(recipe_id), recipe_ingredients_name(name).
- Schema changes must be additive migrations (ALTER TABLE ADD COLUMN / CREATE ... IF NOT EXISTS), run automatically
  and idempotently, never requiring reprocessing of the 2M done lines.
- Usage log `data.nosync/gemini_usage.jsonl`, one JSON per request, e.g. {"day": "2026-10-30", "key": 3, "model":
  "gemini-3.5-flash-lite", "thinking": "minimal", "lines": 650, "ts": ..., "latency": ..., "retries": 0,
  "prompt_tokens": ..., "thoughts_tokens": 0, "output_tokens": ..., "cached_tokens": 0, "total_tokens": ...,
  "finish": "STOP"} (errors have "error", 429s also "details"). `"key"` is currently the 0-based line index in
  gemini_keys.txt (5 keys). New records must use a key fingerprint (first 8 hex chars of sha256 of the key). When
  counting today's usage, map old integer keys to the fingerprint of the key at that index in the current key file
  (best effort), so a partly used quota on the day of the switch is still respected.

## Commands to add
- `recipe-crawler lines`: notebook step 1. Collect distinct lines into ingredient_lines (insert new, update counts,
  mark too long as -2). Must not reset done lines.
- `recipe-crawler process`: notebook step 2 (Lane scheduler, quota per key x model, RPD/RPM, in-flight, stagger,
  429 PerDay vs short-term pauses, 5xx/network retries with backoff, retry queue, MAX_ATTEMPTS -> status -1, most
  common lines first). Runs until every lane has used today's quota (or nothing is left), then exits (no sleeping
  until the reset; the daily job restarts it). Options: --keys FILE (default gemini_keys.txt), --model (repeatable),
  --batch-size, --rpm, --max-requests (stop after N requests in total, for testing), --dry-run (print what it would
  send). Clean shutdown on SIGINT/SIGTERM: stop sending, wait for in-flight requests and save them; a second signal
  aborts.
- `recipe-crawler map`: notebook step 3, but incremental by default: only (re)build rows for recipes that have no
  rows yet or that contain a line done since the last map (track it with a small state table or a recipes column,
  your choice, documented). `--full` rebuilds everything like the notebook (drop + recreate + indexes). Must be fast
  on 600K recipes (batch inserts, one transaction).
- `recipe-crawler stats`: additionally print ingredient progress (the table from notebook cell 7) and today's
  requests per key fingerprint x model vs RPD, plus the time to the next reset (midnight America/Los_Angeles).
- Logging via the logging module (already configured in main), plus a global `--log-file` option so scheduled runs
  can be checked later.

Put the ingredient logic in a new module (e.g. src/recipe_crawler/ingredients.py, maybe gemini.py for the client and
lanes); keep cli.py thin. Settings in a new `[ingredients]` section of config.toml with the notebook defaults (models,
thinking_level "minimal", batch_size 650, max_output_tokens 32768, max_line_chars 200, max_attempts 2, rpd 500,
rpm 14, in_flight 200, stagger 5, usage_log "data.nosync/gemini_usage.jsonl", keys_file "gemini_keys.txt").
load_config currently exits if crawler.contact is empty (it is empty in the committed config): the ingredient
commands must not require crawler.contact; only crawl/probe/discover need it.

## Fixes to apply (found while checking the finished run)
1. UNIT_FIX additions (seen in the mapped data): "ek" -> tbsp (552 rows), "pl" (Czech polévková lžíce) -> tbsp
   (449), "hrst" -> handful (298), "tk" -> tsp (95), "cs" (c.s.) -> tbsp (49), "ks"/"stk"/"st" -> piece,
   "tbs"/"tbl" -> tbsp. Do NOT map a bare "t"/"c" blindly (t can be tsp or tbsp, c can be cup); leave them.
2. Amount sanity: drop the amount (NULL, keep the row) when it is absurd, e.g. g/ml > 50,000 or any amount > 100,000
   after unit conversion (40 rows like 500000 g aubergine).
3. Case: lines are deduplicated on a lowercase, whitespace-collapsed key, but the model should get the original
   spelling, because "5 t cream" vs "5 T cream" (tsp vs tbsp) is lost in lowercase. Add a nullable `original` TEXT
   column to ingredient_lines (additive), filled by `lines` with one original spelling per key (the most common one
   or the first seen), and send `original or line` in process. Add one line to the prompt: in English recipes
   lowercase "t" = tsp and uppercase "T" = tbsp. Keep the rest of the prompt and RESPONSE_SCHEMA exactly as in the
   notebook (validated at ~0.1-0.2% untranslated).
4. Name standardization (TODO.md "Names are not standardized"): add the `ingredient_names` table
   (name -> canonical) and apply it in `map` (canonical when present, else name). Add a `recipe-crawler names`
   subcommand stub documented as the next step, unless there is time to implement the Gemini-based merging cleanly;
   keep it optional and off by default.

## Tests (pytest, no network)
Fake Gemini client injected into the processing code (don't monkeypatch google.genai internals more than needed).
Cover: normal batch saved; model leaves out lines -> retried, then -1 after max_attempts; bad JSON; 503 retried with
backoff (waits injectable/zero in tests); 429 short-term pause vs 429 PerDay -> lane exhausted; RPD reached ->
process exits; old integer-key usage records counted against the right fingerprint; SIGINT-style stop saves in-flight
results; `lines` keeps done statuses and fills `original`; incremental `map` only touches affected recipes and
`--full` gives the same result; UNIT_FIX and the amount cap. Use temp SQLite databases with the real SCHEMA from
db.py plus the notebook tables. All existing tests must still pass (`uv run pytest`).

## Docs
Add a short "Ingredient processing" section to README.md (commands, keys file, quota notes from the TODO.md lessons)
and tick off / adjust TODO.md section 1.

## Report when done
The PR URL, the design (modules, tables/columns added, how incremental map tracks its state), anything that behaves
differently from the notebook, and exactly what the user should run locally first on the real database, in order,
starting with safe read-only commands like `stats` and `process --dry-run`.
