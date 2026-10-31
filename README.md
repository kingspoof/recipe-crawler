# recipe-crawler

A polite web crawler that collects recipes from ~260 recipe sites in 87 countries and 38+ languages.

Most recipe websites, in every country, embed a machine-readable [schema.org/Recipe](https://schema.org/Recipe)
block (Google requires it for recipe search results). So there is **one generic extractor** instead of a
parser per site, and adding a new site is usually one line of config.

## Setup

```bash
uv sync
# then edit config.toml and set crawler.contact to your (university) email
```

> **Note:** this folder is on the iCloud-synced Desktop. The virtualenv (`.venv.nosync`) and the database
> (`data.nosync/`) use the `.nosync` suffix so iCloud leaves them alone. Without that, iCloud breaks the venv
> and would try to sync a multi-GB SQLite file while it is being written.

## Usage

```bash
uv run recipe-crawler crawl                     # crawl all enabled sites (resumable, Ctrl+C to stop)
uv run recipe-crawler crawl -d www.marmiton.org # only one site (repeat -d for more)
uv run recipe-crawler stats                     # progress per site / region / country / language
uv run recipe-crawler export recipes.jsonl --dedupe --require-instructions
uv run recipe-crawler probe www.some-site.com   # test whether a new site works before adding it
```

For a multi-day crawl, run it in `tmux`/`screen` (or with `caffeinate -i` on a Mac so it doesn't sleep).
Stopping and restarting is safe: all state lives in SQLite.

## Ingredient processing

Every distinct ingredient line is turned into English name, amount and unit with Gemini, then mapped back to
the recipes (`recipe_ingredients`: recipe_id = `recipes.id`, pos, line_id, name, amount, unit).

```bash
uv run recipe-crawler lines            # collect new distinct lines into ingredient_lines (keeps done ones)
uv run recipe-crawler process --dry-run  # what would be sent, today's quota per key
uv run recipe-crawler process          # send to-do lines (most common first) until today's quota is used up
uv run recipe-crawler map              # incremental: new recipes and recipes with newly done lines
uv run recipe-crawler map --full       # drop and rebuild (needed after changing UNIT_FIX or ingredient_names)
uv run recipe-crawler stats            # also ingredient progress, requests today per key x model, next reset
uv run recipe-crawler --log-file data.nosync/ingredients.log process   # log to a file for scheduled runs
```

- **Keys**: `gemini_keys.txt` (not in git), one API key per line, **one key per Google Cloud project**: keys of
  the same project share the quota. Keys are identified by a fingerprint (first 8 hex chars of the sha256) in
  the usage log and the output, never by line number. Settings in `[ingredients]` of `config.toml`.
- **Quota**: 500 requests per day per project and model on the free tier, reset at midnight Pacific
  (08:00 / 09:00 Swiss time). `process` counts every request in `data.nosync/gemini_usage.jsonl` (the API
  does not say what is left) and exits once every key x model has used today's quota. 503 (overloaded) answers
  count towards the 500 too. Many keys of one account starting at once get 429 within minutes, so lanes start
  `stagger` seconds apart; a short-term 429 pauses a lane for 2, 4, 8 … 30 min, a per-day 429 ends it for today.
- **Stopping**: Ctrl+C / SIGTERM stops sending, waits for the requests in flight and saves them; a second
  Ctrl+C aborts. Every answer is saved right away, so restarting is always safe.
- **Server and network errors** are never counted against the lines. A network error (no answer) pauses the
  lane for 2, 4, 8 … 30 min. A 5xx pauses the lane too, unless other requests of the same model succeed in
  between: then the batch itself is the problem and is split in halves, down to single lines. A single line that
  still fails twice that way gets **status -3** ("gave up (server errors)" in `stats`). It is safe to retry:
  `sqlite3 data.nosync/recipes.db "UPDATE ingredient_lines SET status = 0 WHERE status = -3;"`
  (status -1 = the model left the line out twice; reset it the same way to try again).
- **Database errors**: a failed save is retried 3 times (10 s busy timeout each); if the database still can't be
  written (locked, disk full), the answers go to `data.nosync/unsaved_answers.jsonl`, no new requests are sent,
  later answers go straight to that file, and the next `process` saves them first. So if `process` stops with
  "database not writable" after about half a minute, nothing is lost: find what held the lock (`map --full`,
  `sqlite3` left open in a transaction, a full disk) and run `process` again.
- `names` is a stub for the next step (merging names like "cilantro" / "coriander" via `ingredient_names`).

## Database safety

`recipes.db` holds weeks of crawling and the Gemini answers paid for with quota. Set `[backup] dir` in
`config.toml` to a folder **outside iCloud** (ideally another disk) first; `backup` refuses to run without it.

```bash
uv run recipe-crawler backup          # daily: ingredient_lines, ingredient_names, domains (gzipped, small)
                                      #   + a full backup when the newest is older than 7 days
uv run recipe-crawler backup --full   # full backup now
uv run recipe-crawler check           # quick_check of the newest backups; exit 1 if broken or too old
```

- Both kinds are safe while `crawl` / `process` / `map` run (one consistent snapshot, written to `*.partial`,
  checked with `quick_check`, then renamed). Retention: 14 daily; full: the newest 2 plus the newest of each of
  the last 4 weeks. `backup` ends with a WAL checkpoint, so the `-wal` file shrinks back.
- Exclude the live database from Time Machine (a copy taken mid-write can be torn):
  `tmutil addexclusion -p "$PWD/data.nosync"` (and keep the backup folder included).
- **Restore a full backup**: stop every `recipe-crawler` process, move `recipes.db`, `recipes.db-wal` and
  `recipes.db-shm` aside, copy the backup to `data.nosync/recipes.db`.
- **Restore the daily tables** (e.g. `ingredient_lines` after a bad edit): `gunzip -k daily-….db.gz`, then in
  `sqlite3 data.nosync/recipes.db`: `ATTACH 'daily-….db' AS b; BEGIN; DELETE FROM ingredient_lines;
  INSERT INTO ingredient_lines (id, line, n, status, items, model, done_at, original)
  SELECT id, line, n, status, items, model, done_at, original FROM b.ingredient_lines; COMMIT;`
  Always name the columns: a backup from an older schema can have fewer (e.g. `domains` before schema v2 has no
  `incomplete_discoveries`), so for `domains` use
  `INSERT INTO domains (domain, discovered_at, sitemaps_read, urls_found) SELECT domain, discovered_at,
  sitemaps_read, urls_found FROM b.domains;`
- **Schema versions** live in `src/recipe_crawler/schema.py` (`PRAGMA user_version`). A migration runs on the
  first write command after an update, takes a full backup first when it changes data, and refuses to run while
  another `recipe-crawler` process uses the database. Read-only commands (`stats`, `export`, `names`,
  `process --dry-run`) open the database read-only and never migrate. Code from this version on refuses a
  database with a newer schema; older checkouts (before schema versioning) do not check, so don't run them
  against a migrated database.
- Only one `crawl`, `process`, `map`, `lines` or `backup` runs at a time per database.
- **Don't**: `cp` the database while anything runs, delete `-wal` / `-shm` files while anything runs, `VACUUM`
  during a crawl, or `DROP COLUMN` (rewrites all 7 GB).

## How it works

1. **robots.txt**: fetched per host, refreshed after 24 h and obeyed (including `Crawl-delay`); redirects are
   followed only if the target's robots.txt allows it. Sites that return 401/403 for
   robots.txt are treated as "bots not welcome" and skipped.
2. **Discovery**: reads the site's sitemaps (again after `rediscover_after_days`, and on the next run if a
   sitemap failed). If a sitemap index has recipe-named children
   (`recipe-sitemap.xml`, `receita-sitemap.xml`, `/recepti/…`) only those are followed. For sites without
   recipe sitemaps, `follow_links = true` mode follows links whose URL matches `url_pattern`.
3. **Crawling**: one sequential worker per site with ≥2 s between requests. It slows down when the
   server is slow, backs off exponentially on 429/503 (honouring `Retry-After`), and pauses a site after
   25 consecutive failures or if almost no pages contain recipes. Up to 100 sites run in parallel.
4. **Extraction**: JSON-LD (`@graph`, nested `HowToSection`s, string or list instructions, ISO-8601 durations,
   broken JSON) with a microdata fallback. A recipe needs at least a name and ingredients.
5. **Storage**: `data.nosync/recipes.db` (SQLite). The `recipes` table has clean columns (name, ingredients,
   instructions, cuisine, category, times, yield, rating, nutrition, language, site country/region) and the
   original JSON-LD in `raw`.

## Expected scale and time

With ~260 sites the crawl starts at roughly 100,000 pages/hour. Small blogs (a few hundred to a few thousand
recipes each) finish within hours. After that, the large community sites (food.com, chefkoch, 10000recipe,
tudogostoso, nefisyemektarifleri, kochbar, xiachufang, ptitchef, …) carry the crawl. Expect **several days**
to reach 1M. `max_recipes_per_site` (80k) keeps those big sites from dominating. Raise it, or add more sites,
if you hit the ceiling before 1M.

## Adding sites

```bash
uv run recipe-crawler probe www.newsite.com
```

The probe reports whether robots.txt allows crawling, which sitemaps it found, and whether random pages
contain recipes. If it reports `discovery: links`, add the site with `follow_links = true` and a `url_pattern`
matching the recipe URLs it printed. See the comments at the top of `config.toml` for all options.

## Be a good citizen (and keep your project safe)

- Put a real contact address in `config.toml`. Site operators can then email you instead of banning you.
- Don't lower `default_delay` below ~1 s, and don't try to get around sites that block bots (403/402,
  Cloudflare challenges). Several big sites were excluded for exactly that reason (see `config.toml`).
- Check each site's terms of use. Recipe *ingredient lists* generally aren't copyrightable, but descriptions,
  instructions and photos usually are. Using the data for research is normally fine. **Publishing the
  dataset** is a different question; ask your supervisor or university's legal office before you do.
- Store the source `url` with every recipe (already done) so your work stays traceable.

## Complementary existing datasets

Worth knowing about for comparison or to fill gaps:
- **Web Data Commons** extracts schema.org data (including `Recipe`) from Common Crawl. That is millions of
  multilingual recipes, with no crawling needed. It's a good way to find more candidate sites too.
- **RecipeNLG** (~2.2M) and **Recipe1M+** (~1M): large, but mostly English/US recipes.

## Tests

```bash
uv run pytest
```
