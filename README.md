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

## How it works

1. **robots.txt**: fetched per site and obeyed (including `Crawl-delay`). Sites that return 401/403 for
   robots.txt are treated as "bots not welcome" and skipped.
2. **Discovery**: reads the site's sitemaps. If a sitemap index has recipe-named children
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
