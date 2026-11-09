"""Command line interface.

    uv run recipe-crawler probe www.example.com      # test whether a site is crawlable
    uv run recipe-crawler crawl                      # discover + crawl all enabled sites
    uv run recipe-crawler crawl -d www.example.com   # just one site
    uv run recipe-crawler stats
    uv run recipe-crawler export recipes.jsonl --dedupe
    uv run recipe-crawler lines                      # ingredient processing: collect lines,
    uv run recipe-crawler process                    #   send them to Gemini until today's quota is used,
    uv run recipe-crawler map                        #   map the results back to the recipes
    uv run recipe-crawler scout --from-links --probe 20 --report   # find new sites, propose config lines
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import signal
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

from . import backup, classify, gemini, graph, ingredients, scout
from .config import Settings, Site, load_config
from .crawler import Crawler
from .db import DB, single_instance
from .probe import probe_site


def open_db(settings: Settings) -> DB:
    """For write commands: migrates the schema if needed, with a full backup first when a migration asks for
    one (refused if [backup] dir is not set)."""
    take = (lambda: backup.full(settings.db_path, settings.backup)) if settings.backup.dir else None
    return DB(settings.db_path, backup=take)


def _selected_sites(settings: Settings, domains: list[str] | None) -> list[Site]:
    if domains:
        return [settings.site(d.lower()) for d in domains]
    return [s for s in settings.sites if s.enabled]


# --------------------------------------------------------------------------- crawl / discover


def cmd_crawl(settings: Settings, args: argparse.Namespace, discover_only: bool = False) -> None:
    if args.target:
        settings.target_total = args.target
    db = open_db(settings)
    crawler = Crawler(settings, db)
    sites = _selected_sites(settings, args.domain)

    def stop(*_: object) -> None:
        if crawler.stopping:
            sys.exit(1)
        logging.warning("Stopping after in-flight requests finish (Ctrl+C again to force)...")
        crawler.stopping = True

    loop = asyncio.new_event_loop()
    loop.add_signal_handler(signal.SIGINT, stop)
    loop.add_signal_handler(signal.SIGTERM, stop)
    try:
        loop.run_until_complete(crawler.run(sites, discover_only, getattr(args, "force", False)))
    finally:
        loop.close()
        logging.info("Done. %d recipes in database.", db.recipe_count())
        db.close()


# --------------------------------------------------------------------------- probe


def cmd_probe(settings: Settings, args: argparse.Namespace) -> None:
    async def main():
        sem = asyncio.Semaphore(16)

        async def one(d):
            async with sem:
                return await probe_site(settings, d.lower(), args.samples, args.max_sitemaps)

        return await asyncio.gather(*(one(d) for d in args.domains))

    reports = asyncio.run(main())
    if args.json:
        print(json.dumps(reports, ensure_ascii=False, indent=2))
        return
    for rep in reports:
        flag = "OK  " if rep["ok"] else "FAIL"
        est = f"  ~{rep['est_urls']:,} URLs" if rep.get("est_urls") else ""
        print(f"\n[{flag}] {rep['domain']}  {rep['status']}  hit rate {rep.get('recipe_hit_rate', '-')}"
              f"  crawl-delay {rep.get('crawl_delay')}  discovery: {rep.get('mode', 'sitemaps')}{est}"
              f"  language: {rep.get('language') or '-'}")
        if rep.get("problem"):
            print(f"   problem: {rep['problem']}")
        for sm, n in rep.get("sitemaps_sampled", [])[:15]:
            print(f"   sitemap {sm}  ({n} urls kept)")
        for t in rep.get("samples", []):
            label = (f"{t['recipe']!r} ({t['ingredients']} ingr, {t['steps']} steps, {t['language']})"
                     if t.get("recipe") else f"no recipe (HTTP {t['status']})")
            if t.get("data_problem"):
                label += f"  BAD DATA: {t['data_problem']}"
            print(f"   {urlsplit(t['url']).path[:70]:<70} {label}")


# --------------------------------------------------------------------------- scout


def cmd_scout(settings: Settings, args: argparse.Namespace) -> None:
    db = open_db(settings)
    try:
        if args.import_wdc:
            counts = scout.import_wdc(db, settings, Path(args.import_wdc))
            print(f"WDC: {counts.get('read', 0):,} domains read, {counts.get('new', 0):,} new candidates, "
                  f"{counts.get('updated', 0):,} updated; skipped {counts.get('configured', 0):,} configured, "
                  f"{counts.get('excluded', 0):,} excluded, {counts.get('non-recipe', 0):,} non-recipe")
        if args.from_links:
            new, updated = scout.from_links(db, settings, args.min_sites)
            print(f"links: {new:,} new candidates, {updated:,} updated "
                  f"(linked from >= {args.min_sites or settings.scout.min_linked_sites} crawled sites)")
        if args.probe:
            results = asyncio.run(scout.probe_candidates(db, settings, args.probe))
            print("probed:", ", ".join(f"{k}: {v}" for k, v in results.most_common()) or "nothing to probe")
        if args.report:
            print("\n".join(scout.report(db, settings, args.limit)))
        print("\n".join(scout.summary(db)))
    finally:
        db.close()


# --------------------------------------------------------------------------- stats / export


def cmd_stats(settings: Settings, args: argparse.Namespace) -> None:
    db = DB(settings.db_path, readonly=True)
    rows = db.stats()
    print(f"{'domain':<34}{'recipes':>9}{'pending':>10}{'done':>9}{'no_rec':>9}{'errors':>8}{'blocked':>9}")
    for r in rows:
        print(f"{r['domain']:<34}{r['recipes']:>9}{r['pending']:>10}{r['done']:>9}{r['no_recipe']:>9}"
              f"{r['errors']:>8}{r['blocked']:>9}")
    print(f"\nTOTAL recipes: {db.recipe_count()}")
    for col in ("site_region", "site_country", "language"):
        parts = ", ".join(f"{r['key']}: {r['n']}" for r in db.stats_by(col)[:30])
        print(f"by {col}: {parts}")
    print("\n".join(scout.summary(db)))
    print("\ningredient lines:")
    print("\n".join(ingredients.progress(db.conn)))
    cfg = settings.ingredients
    keys = gemini.load_keys(cfg.keys_file) if cfg.keys_file.exists() else []
    print("\n".join(ingredients.quota_report(cfg, keys)))
    db.close()


def _fingerprint(rec: dict) -> str:
    key = rec["name"].lower().strip() + "|" + "|".join(sorted(i.lower().strip() for i in rec["ingredients"]))
    return hashlib.sha1(key.encode()).hexdigest()


def cmd_export(settings: Settings, args: argparse.Namespace) -> None:
    db = DB(settings.db_path, readonly=True)
    seen: set[str] = set()
    n = dupes = 0
    langs: Counter[str] = Counter()
    with open(args.output, "w", encoding="utf-8") as f:
        for rec in db.iter_recipes(include_raw=args.raw):
            if args.require_instructions and not rec["instructions"]:
                continue
            if args.dedupe:
                fp = _fingerprint(rec)
                if fp in seen:
                    dupes += 1
                    continue
                seen.add(fp)
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1
            langs[(rec["language"] or "?")[:2].lower()] += 1
    print(f"Wrote {n} recipes to {args.output} ({dupes} duplicates skipped)")
    print("languages:", ", ".join(f"{k}: {v}" for k, v in langs.most_common(25)))
    db.close()


# --------------------------------------------------------------------------- ingredients


def cmd_lines(settings: Settings, args: argparse.Namespace) -> None:
    db = open_db(settings)
    ingredients.collect_lines(db.conn, settings.ingredients)
    db.close()


def _gemini_args(settings: Settings, args: argparse.Namespace) -> tuple[list[str], gemini.UsageLog]:
    """Applies the command line overrides; returns the keys and the usage log."""
    cfg = settings.ingredients
    for name in ("batch_size", "rpm"):
        if getattr(args, name, None):
            setattr(cfg, name, getattr(args, name))
    if args.model:
        cfg.models = args.model
    keys_file = args.keys or cfg.keys_file
    if not os.path.exists(keys_file):
        raise SystemExit(f"No keys file {keys_file}: one Gemini API key per line, one key per project.")
    return gemini.load_keys(keys_file), gemini.UsageLog(cfg.usage_log)


def _stop_on_signals(processor: gemini.Processor) -> None:
    def stop(*_: object) -> None:
        if processor.stopping:
            logging.error("Aborting without waiting for the requests in flight.")
            os._exit(1)  # the worker threads would keep the interpreter alive; saved data is already committed
        logging.warning("Stopping: waiting for the requests in flight and saving them (Ctrl+C again to abort)...")
        processor.stop()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)


def cmd_process(settings: Settings, args: argparse.Namespace) -> None:
    cfg = settings.ingredients
    keys, usage = _gemini_args(settings, args)
    used = usage.used_today(keys)
    if args.dry_run:  # no migrations, no requests, no usage records
        db = DB(settings.db_path, readonly=True)
        try:
            print("\n".join(ingredients.dry_run(db.conn, cfg, keys, used, show_prompt=args.verbose)))
        finally:
            db.close()
        return
    db = open_db(settings)
    ingredients.apply_spool(db.conn, cfg)
    lanes = ingredients.build_lanes(cfg, keys, usage, used)
    processor = gemini.Processor(cfg, lanes, usage, ingredients.make_save(db.conn), max_requests=args.max_requests,
                                 spool=ingredients.spool_path(cfg))
    _stop_on_signals(processor)
    try:
        processor.run(ingredients.todo_lines(db.conn))
    finally:
        db.close()


def cmd_map(settings: Settings, args: argparse.Namespace) -> None:
    db = open_db(settings)
    ingredients.map_ingredients(db.conn, settings.ingredients, full=args.full)
    db.close()


def cmd_graph(settings: Settings, args: argparse.Namespace) -> None:
    opts = graph.GraphOptions(min_recipes=args.min_recipes, min_weight=args.min_weight, regions=args.region or [],
                              countries=args.country or [], languages=args.language or [], domains=args.domain or [],
                              diets=args.diet or [], exclude=args.exclude or [])
    db = DB(settings.db_path, readonly=True)
    try:
        g = graph.build_graph(db.conn, opts)
    finally:
        db.close()
    graph.write_graph(g, args.output)
    print("\n".join(graph.summary(g)))


def cmd_classify(settings: Settings, args: argparse.Namespace) -> None:
    if args.report or args.dry_run:  # read only: no migrations, no requests
        db = DB(settings.db_path, readonly=True)
        try:
            print("\n".join(classify.report(db.conn) if args.report else classify.dry_run(db.conn, args.verbose)))
        finally:
            db.close()
        return
    keys, usage = _gemini_args(settings, args)
    cfg = classify.settings(settings.ingredients)
    db = open_db(settings)
    try:
        classify.apply_spool(db.conn, cfg)
        lanes = ingredients.build_lanes(cfg, keys, usage, usage.used_today(keys))
        processor = gemini.Processor(cfg, lanes, usage, classify.make_save(db.conn), max_requests=args.max_requests,
                                     spool=classify.spool_path(cfg), task=classify.TASK)
        _stop_on_signals(processor)
        processor.run(classify.todo(db.conn))
    finally:
        db.close()


def cmd_names(settings: Settings, args: argparse.Namespace) -> None:
    if args.report or args.dry_run:  # read only: no migrations, no requests
        db = DB(settings.db_path, readonly=True)
        try:
            if args.report:
                print("\n".join(ingredients.names_report(db.conn)))
            else:
                print("\n".join(ingredients.names_dry_run(db.conn, settings.ingredients, show_prompt=args.verbose)))
        finally:
            db.close()
        return
    keys, usage = _gemini_args(settings, args)
    cfg = ingredients.names_settings(settings.ingredients)
    db = open_db(settings)
    try:
        if not args.apply_only:
            ingredients.apply_names_spool(db.conn, cfg)
            lanes = ingredients.build_lanes(cfg, keys, usage, usage.used_today(keys))
            processor = gemini.Processor(cfg, lanes, usage, ingredients.make_save_names(db.conn),
                                         max_requests=args.max_requests, spool=ingredients.names_spool_path(cfg),
                                         task=ingredients.NAMES)
            _stop_on_signals(processor)
            counts = ingredients.name_counts(db.conn)
            for _ in range(ingredients.NAMES_ROUNDS):  # later rounds: canonical names the model invented
                todo = ingredients.todo_names(db.conn, counts)
                if not todo or processor.run(todo) != "done":
                    break
        ingredients.apply_names(db.conn)
    finally:
        db.close()


# --------------------------------------------------------------------------- backup / check


def cmd_backup(settings: Settings, args: argparse.Namespace) -> None:
    DB(settings.db_path, readonly=True).close()  # refuses a missing DB
    for path in backup.run(settings.db_path, settings.backup, force_full=args.full):
        print(f"wrote {path}")
    print(backup.checkpoint(settings.db_path))


def cmd_check(settings: Settings, args: argparse.Namespace) -> None:
    ok, lines = backup.check(settings.backup)
    print("\n".join(lines))
    if not ok:
        raise SystemExit(1)


# --------------------------------------------------------------------------- main


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="recipe-crawler", description="Polite schema.org recipe crawler")
    p.add_argument("-c", "--config", default="config.toml")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--log-file", help="also write the log to this file (appends)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sp = sub.add_parser("probe", help="check whether sites expose crawlable schema.org recipes")
    sp.add_argument("domains", nargs="+")
    sp.add_argument("--samples", type=int, default=5, help="random pages to test per site")
    sp.add_argument("--max-sitemaps", type=int, default=3, help="sitemaps to read per site")
    sp.add_argument("--json", action="store_true")

    for name, help_ in (("discover", "only read sitemaps and fill the URL queue"),
                        ("crawl", "discover (if needed) and crawl recipes")):
        sp = sub.add_parser(name, help=help_)
        sp.add_argument("-d", "--domain", action="append", help="limit to this domain (repeatable)")
        sp.add_argument("--target", type=int, help="stop after this many recipes in total")
        if name == "discover":
            sp.add_argument("--force", action="store_true", help="re-read sitemaps even if done before")

    sub.add_parser("stats", help="show crawl and ingredient progress, today's Gemini quota")

    sp = sub.add_parser("scout", help="find candidate recipe sites, probe them, propose config lines (never enables)")
    sp.add_argument("--from-links", action="store_true", help="add external domains linked from crawled pages")
    sp.add_argument("--min-sites", type=int, help="with --from-links: linked from at least this many sites "
                                                    "(default: scout.min_linked_sites)")
    sp.add_argument("--import-wdc", metavar="FILE", help="add domains from a downloaded Web Data Commons "
                                                          "Recipe domain list (csv/tsv/txt, .gz ok)")
    sp.add_argument("--probe", type=int, metavar="N", help="probe at most N candidates (new ones first)")
    sp.add_argument("--report", action="store_true", help="print config.toml lines for the good candidates")
    sp.add_argument("--limit", type=int, default=50, help="candidates in the report")

    sp = sub.add_parser("export", help="export recipes as JSON Lines")
    sp.add_argument("output")
    sp.add_argument("--dedupe", action="store_true", help="drop recipes with same name + ingredients")
    sp.add_argument("--raw", action="store_true", help="include the original JSON-LD")
    sp.add_argument("--require-instructions", action="store_true")

    sub.add_parser("lines", help="collect the distinct ingredient lines into ingredient_lines")

    sp = sub.add_parser("process", help="send to-do ingredient lines to Gemini until today's quota is used up")
    sp.add_argument("--keys", help="keys file (default: ingredients.keys_file)")
    sp.add_argument("--model", action="append", help="model (repeatable, default: ingredients.models)")
    sp.add_argument("--batch-size", type=int, help="lines per request")
    sp.add_argument("--rpm", type=int, help="requests per minute per lane")
    sp.add_argument("--max-requests", type=int, help="stop after this many requests in total (for testing)")
    sp.add_argument("--dry-run", action="store_true", help="only print what it would send")

    sp = sub.add_parser("classify", help="diet, food group and allergens per ingredient with Gemini (ingredient_info)")
    sp.add_argument("--keys", help="keys file (default: ingredients.keys_file)")
    sp.add_argument("--model", action="append", help="model (repeatable, default: ingredients.models)")
    sp.add_argument("--max-requests", type=int, help="stop after this many requests in total (for testing)")
    sp.add_argument("--dry-run", action="store_true", help="only print what it would send")
    sp.add_argument("--report", action="store_true", help="only print the progress and the distribution")

    sp = sub.add_parser("graph", help="ingredient co-occurrence graph as GraphML for networkx (read only)")
    sp.add_argument("output", help="file to write, e.g. data.nosync/ingredients.graphml")
    sp.add_argument("--min-recipes", type=int, default=graph.GraphOptions.min_recipes,
                    help="leave out ingredients used in fewer recipes (default %(default)s)")
    sp.add_argument("--min-weight", type=int, default=graph.GraphOptions.min_weight,
                    help="no edge for pairs used together in fewer recipes (default %(default)s)")
    sp.add_argument("--region", action="append", help="only recipes of this site_region (repeatable)")
    sp.add_argument("--country", action="append", help="only recipes of this site_country (repeatable)")
    sp.add_argument("--language", action="append", help="only recipes in this language, e.g. de (repeatable)")
    sp.add_argument("--domain", action="append", help="only recipes of this site (repeatable)")
    sp.add_argument("--diet", action="append", choices=classify.DIETS,
                    help="only recipes with this diet, from `classify` (repeatable)")
    sp.add_argument("--exclude", action="append", help="leave out this ingredient, e.g. salt (repeatable)")

    sp = sub.add_parser("map", help="map processed lines back to recipe_ingredients (incremental)")
    sp.add_argument("--full", action="store_true", help="drop and rebuild recipe_ingredients")

    sp = sub.add_parser("names", help="merge ingredient names with Gemini (ingredient_names) and apply them")
    sp.add_argument("--keys", help="keys file (default: ingredients.keys_file)")
    sp.add_argument("--model", action="append", help="model (repeatable, default: ingredients.models)")
    sp.add_argument("--max-requests", type=int, help="stop after this many requests in total (for testing)")
    sp.add_argument("--dry-run", action="store_true", help="only print what it would send")
    sp.add_argument("--report", action="store_true", help="only print the progress and the biggest merges")
    sp.add_argument("--apply-only", action="store_true", help="no requests: only apply ingredient_names to "
                                                               "recipe_ingredients")

    sp = sub.add_parser("backup", help="daily backup of the irreplaceable tables, a full one when due")
    sp.add_argument("--full", action="store_true", help="also take a full backup now")
    sub.add_parser("check", help="quick_check the newest backups and check that they are recent")

    args = p.parse_args(argv)
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file, encoding="utf-8"))
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
    )
    if args.log_file:  # a file is read days later: full date
        handlers[1].setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s"))
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("google_genai").setLevel(logging.WARNING)
    if args.cmd in ("probe", "scout"):
        logging.getLogger("crawler").setLevel(logging.ERROR)
    settings = load_config(args.config, require_contact=args.cmd in ("probe", "discover", "crawl")
                           or (args.cmd == "scout" and bool(args.probe)))

    # One instance per command and database (discover and crawl count as one; names and map both rewrite
    # recipe_ingredients, so they count as one too).
    read_only = getattr(args, "dry_run", False) or getattr(args, "report", False)
    instance = {"discover": "crawl", "crawl": "crawl", "lines": "lines", "map": "map", "backup": "backup",
                "scout": "scout",
                "process": None if read_only else "process",
                "names": None if read_only else "map",
                "classify": None if read_only else "classify"}.get(args.cmd)
    lock = single_instance(settings.db_path, instance) if instance else None
    try:
        if args.cmd == "probe":
            cmd_probe(settings, args)
        elif args.cmd == "discover":
            cmd_crawl(settings, args, discover_only=True)
        elif args.cmd == "crawl":
            cmd_crawl(settings, args)
        elif args.cmd == "stats":
            cmd_stats(settings, args)
        elif args.cmd == "scout":
            cmd_scout(settings, args)
        elif args.cmd == "export":
            cmd_export(settings, args)
        elif args.cmd == "lines":
            cmd_lines(settings, args)
        elif args.cmd == "process":
            cmd_process(settings, args)
        elif args.cmd == "map":
            cmd_map(settings, args)
        elif args.cmd == "names":
            cmd_names(settings, args)
        elif args.cmd == "graph":
            cmd_graph(settings, args)
        elif args.cmd == "classify":
            cmd_classify(settings, args)
        elif args.cmd == "backup":
            cmd_backup(settings, args)
        elif args.cmd == "check":
            cmd_check(settings, args)
    finally:
        if lock is not None:
            os.close(lock)
