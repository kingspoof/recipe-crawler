# TODO

## After the current ingredient run is complete (`ingredient_processing.ipynb`)

### 1. Ingredient processing as a terminal command, no notebooks

Done (see README "Ingredient processing"): `lines`, `process`, `map` (incremental, `--full`), `stats`
(progress, quota per key fingerprint, next reset), clean shutdown, `--log-file`, tests with a fake client.
Also done from the first-run fixes: key fingerprints in the usage log (old line-index records are still
counted), `UNIT_FIX` additions (`ek`, `pl`, `hrst`, `tk`, `cs`, `ks`/`stk`/`st`, `tbs`/`tbl`), amount cap,
original spelling sent to the model (`ingredient_lines.original`) plus a t / T line in the prompt.

Still open:

- Names are not standardized yet: "panko" / "panko breadcrumbs", "coriander" / "cilantro",
  "scallion" / "spring onion". The `ingredient_names` table (name -> canonical) exists and `map` applies it;
  `recipe-crawler names` is only a stub. Next: merge the distinct *names* (far fewer than lines, so cheap) with
  Gemini into one canonical name each, then `map --full`.
- "5 t cream" and "5 T cream" still share one lowercase key, so they get one answer (from the most common
  spelling). If that matters, keep the case of a standalone `t` / `T` in the key (re-queues only those lines).
- Check the t / T prompt line with `gemini_quality_test.ipynb` before relying on it.

Lessons from the first run (still valid):

- Prompt and schema from the notebook (`en` field, "never spoon", unit rules), validated with
  `gemini_quality_test.ipynb`: ~0.1% untranslated.
- 503 (overloaded) requests **do** count towards the 500/day (seen in AI Studio, 2026-10-29). ~20% of requests
  were 503 on the first evening.
- The 500/day is per project (confirmed 2026-10-29). But 11 projects on one account sending 4 requests each at
  the same time all got 429 within minutes, so there seems to be an additional short-term (per minute) limit
  per account. Ramp up slowly and stagger the keys instead of starting all at once.
- Most common lines first: the top ~52K distinct lines covered half of all recipe lines.
- Measured: ~15 input + ~28 output tokens per line, ~786 tokens prompt overhead, ~55 s per 650-line request,
  0 thinking tokens at `minimal`.

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
