# TODO

## After the current ingredient run is complete (`ingredient_processing.ipynb`)

### 1. Ingredient processing as a terminal command, no notebooks

Move the logic from `ingredient_processing.ipynb` into the package as CLI subcommands next to
`crawl` / `probe` / `stats` / `export` in `src/recipe_crawler/cli.py`, settings in `config.toml`:

- `recipe-crawler lines`: collect new distinct ingredient lines into `ingredient_lines` (step 1 of the notebook).
- `recipe-crawler process`: send to-do lines to Gemini until every key has used up today's quota, then exit
  (step 2). Options for keys file, models, batch size, pace, max requests.
- `recipe-crawler map`: rebuild `recipe_ingredients` with the local unit cleanup (step 3). Ideally incremental
  (only recipes that are new or whose lines just got done) instead of a full rebuild.
- `recipe-crawler stats` should also show ingredient progress and today's quota per key.
- Clean shutdown on Ctrl-C / SIGTERM (finish requests in flight, save), like `crawl` already does.
- Logging to a file instead of print, so a scheduled run can be checked afterwards.
- Tests with a fake Gemini client (503, 429, bad JSON, missing lines, quota pauses), like the offline test used
  while building the notebook.

Lessons from the first run to keep:

- Prompt and schema from the notebook (`en` field, "never spoon", unit rules), validated with
  `gemini_quality_test.ipynb`: ~0.1% untranslated.
- 503 (overloaded) requests **do** count towards the 500/day (seen in AI Studio, 2026-10-29). ~20% of requests
  were 503 on the first evening.
- The 500/day is per project (confirmed 2026-10-29: a second project on an account whose first project was
  used up ran normally). But 11 projects on one account sending 4 requests each at the same time all got 429
  within minutes, so there seems to be an additional short-term (per minute) limit per account. Ramp up slowly
  and stagger the keys instead of starting all at once.
- Identify keys in the usage log by a fingerprint (e.g. first 8 chars of a sha256), not by line number in
  `gemini_keys.txt`, so editing the file does not mix up the counts.
- Most common lines first: the top ~52K distinct lines covered half of all recipe lines.
- Measured: ~15 input + ~28 output tokens per line, ~786 tokens prompt overhead, ~55 s per 650-line request,
  0 thinking tokens at `minimal`.

Quality fixes found in the first run (checked 2026-10-29 on ~496K done lines, ~0.2% untranslated):

- `UNIT_FIX` in step 3 is missing `pl` (Czech polévková lžíce) -> tbsp and `hrst` -> handful.
- Lowercasing the lines before sending loses `t` (teaspoon) vs `T` (tablespoon) in English recipes:
  "5 t cream" came back as cup. Send the original case (deduplicate on the lowercase key, but send one original
  spelling), or at least keep the case of a standalone `t`/`T`.
- Names are not standardized yet: "panko" / "panko breadcrumbs", "coriander" / "cilantro",
  "scallion" / "spring onion". Add a step that merges the distinct *names* (far fewer than lines, so cheap) into one
  canonical name each, e.g. a `ingredient_names` table name -> canonical, applied in `map`.

### 2. Crawl only as much per day as can be processed

- Capacity per day ≈ keys × 500 requests × (1 - 503 share) × 650 lines. Currently 2 keys: ~800 useful requests,
  ~520K lines per day.
- New recipes mostly bring new distinct lines, but common lines are already done. Measure how many *new* distinct
  lines an average new recipe adds (it drops as the dataset grows) and derive the daily recipe budget from that.
- Add a daily limit to the crawler (`max_recipes_per_day` in `config.toml`, or computed from the processing
  capacity), so the to-do lines never pile up.

### 3. Run it every day automatically

- One daily job: `crawl` (up to the budget) → `lines` → `process` (until the quota is used up) → `map`.
- Start after the quota reset (midnight Pacific = 08:00 / 09:00 Swiss time, depending on daylight saving time).
- Use the existing `deploy/recipe-crawler.service` (systemd, for a server) and/or a launchd job on the Mac.
- The dataset can then grow as large as wanted, with the processing keeping up every day.
