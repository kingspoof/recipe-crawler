"""`recipe-crawler backup` and `check`: backups of recipes.db that can be taken while the crawler runs.

- daily: only the tables that cannot be rebuilt cheaply (BackupSettings.DAILY_TABLES: the Gemini answers paid
  for with quota, the canonical names, the discovery state), from one consistent read snapshot, as a small
  gzipped SQLite file. Restore: gunzip, then copy the tables over with ATTACH.
- full: the whole database with Connection.backup() in one step (consistent, even while `crawl` writes),
  written to *.partial, checked with quick_check, then renamed. Restore: stop everything, replace recipes.db
  (and delete its -wal / -shm files only then, with nothing running).

Never `cp` the live database while anything runs: the copy can be torn (and the -wal holds recent commits).
"""

from __future__ import annotations

import gzip
import logging
import shutil
import sqlite3
import time
from datetime import datetime
from pathlib import Path

from .config import BackupSettings
from .schema import has_table

log = logging.getLogger("backup")

# Folders that are synced to the cloud: a multi-GB file that changes daily does not belong there.
SYNCED_MARKERS = ("Mobile Documents", "iCloud", "CloudStorage", "Dropbox", "OneDrive", "Google Drive")
FULL_EVERY_SLACK = 3600  # a daily job that starts a bit earlier than last week still takes the weekly full


def backup_dir(cfg: BackupSettings) -> Path:
    if cfg.dir is None:
        raise SystemExit("Set [backup] dir in config.toml to a folder outside iCloud Drive (e.g. on an external "
                         "disk) before taking backups.")
    resolved = str(cfg.dir.expanduser().resolve())
    if any(marker in resolved for marker in SYNCED_MARKERS):
        raise SystemExit(f"[backup] dir {cfg.dir} is in a cloud-synced folder: use a local folder or a disk.")
    path = Path(resolved)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _connect_ro(db_path: Path) -> sqlite3.Connection:
    if not Path(db_path).exists():
        raise SystemExit(f"No database at {db_path}")
    return sqlite3.connect(Path(db_path).resolve().as_uri() + "?mode=ro", uri=True, timeout=60)


def quick_check(path: Path) -> str:
    con = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True)
    try:
        rows = [r[0] for r in con.execute("PRAGMA quick_check")]
    except sqlite3.DatabaseError as e:
        return f"error: {e}"
    finally:
        con.close()
    return "ok" if rows == ["ok"] else "; ".join(rows[:5])


def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _free_space_check(directory: Path, needed: int) -> None:
    free = shutil.disk_usage(directory).free
    if free < needed * 1.1:
        raise SystemExit(f"Not enough space in {directory}: {free / 1e9:.1f} GB free, ~{needed / 1e9:.1f} GB needed.")


def _tables_size(con: sqlite3.Connection, tables: tuple[str, ...]) -> int:
    """Bytes the tables and their indexes take: from dbstat if SQLite has it, else summed up from the column
    values (doubled for the indexes)."""
    tables = tuple(t for t in tables if has_table(con, t))
    if not tables:
        return 0
    marks = ",".join("?" * len(tables))
    try:  # every b-tree of these tables: the table itself, its indexes and the automatic ones (UNIQUE, PK)
        return con.execute(f"SELECT COALESCE(SUM(pgsize), 0) FROM dbstat WHERE name IN "
                           f"(SELECT name FROM sqlite_master WHERE tbl_name IN ({marks}))", tables).fetchone()[0]
    except sqlite3.OperationalError:  # no dbstat in this SQLite build
        total = 0
        for t in tables:
            cols = [r[1] for r in con.execute(f"PRAGMA table_info({t})")]
            expr = " + ".join(f"COALESCE(LENGTH({c}), 0) + 8" for c in cols)
            total += con.execute(f"SELECT COALESCE(SUM({expr}), 0) FROM {t}").fetchone()[0] * 2  # + indexes
        return total


def daily(db_path: Path, cfg: BackupSettings) -> Path:
    directory = backup_dir(cfg)
    final = directory / f"daily-{_stamp()}.db.gz"
    tmp = directory / (final.name.removesuffix(".gz") + ".partial")
    src = _connect_ro(db_path)
    try:
        # the uncompressed copy and the .gz exist at the same time for a moment
        _free_space_check(directory, int(_tables_size(src, cfg.DAILY_TABLES) * 1.5))
    except BaseException:
        src.close()
        raise
    dst = sqlite3.connect(tmp)
    try:
        src.execute("BEGIN")  # one read snapshot for every table, while other processes keep writing
        rows = 0
        for table in cfg.DAILY_TABLES:
            if not has_table(src, table):
                continue
            for (sql,) in src.execute("SELECT sql FROM sqlite_master WHERE tbl_name = ? AND sql IS NOT NULL "
                                      "ORDER BY type = 'index'", (table,)).fetchall():
                dst.execute(sql)
            cols = len(src.execute(f"SELECT * FROM {table} LIMIT 0").description)
            cur = src.execute(f"SELECT * FROM {table}")
            while chunk := cur.fetchmany(50_000):
                dst.executemany(f"INSERT INTO {table} VALUES ({','.join('?' * cols)})", chunk)
                rows += len(chunk)
        src.execute("COMMIT")
        dst.commit()
    finally:
        src.close()
        dst.close()
    result = quick_check(tmp)
    if result != "ok":
        tmp.unlink()
        raise SystemExit(f"daily backup failed quick_check: {result}")
    gz_tmp = directory / (final.name + ".partial")
    with open(tmp, "rb") as f_in, gzip.open(gz_tmp, "wb", compresslevel=6) as f_out:
        shutil.copyfileobj(f_in, f_out, 1024 * 1024)
    tmp.unlink()
    gz_tmp.rename(final)
    log.info("daily backup %s: %s rows, %.1f MB", final.name, f"{rows:,}", final.stat().st_size / 1e6)
    return final


def full(db_path: Path, cfg: BackupSettings) -> Path:
    directory = backup_dir(cfg)
    final = directory / f"full-{_stamp()}.db"
    tmp = directory / (final.name + ".partial")
    size = Path(db_path).stat().st_size + _wal_size(db_path)
    _free_space_check(directory, size)
    t = time.time()
    src = _connect_ro(db_path)
    dst = sqlite3.connect(tmp)
    try:
        src.backup(dst)  # pages=-1: everything in one step, so the copy is one consistent snapshot
    finally:
        dst.close()
        src.close()
    result = quick_check(tmp)
    if result != "ok":
        tmp.unlink()
        raise SystemExit(f"full backup failed quick_check: {result}")
    tmp.rename(final)
    log.info("full backup %s: %.1f GB in %.0f s", final.name, final.stat().st_size / 1e9, time.time() - t)
    return final


def _wal_size(db_path: Path) -> int:
    wal = Path(str(db_path) + "-wal")
    return wal.stat().st_size if wal.exists() else 0


def _backups(directory: Path, kind: str) -> list[Path]:
    """Finished backups of one kind, oldest first (the names sort by time)."""
    suffix = ".db.gz" if kind == "daily" else ".db"
    return sorted(p for p in directory.glob(f"{kind}-*{suffix}") if p.name.endswith(suffix))


def _taken_at(path: Path) -> datetime:
    return datetime.strptime(path.name.split("-", 1)[1][:15], "%Y%m%d-%H%M%S")


def prune(cfg: BackupSettings) -> list[Path]:
    """Keeps the newest daily_keep daily backups; of the full ones the newest full_keep, plus the newest of
    each of the last weekly_keep calendar weeks. Leftover *.partial files older than a day are removed."""
    directory = backup_dir(cfg)
    removed = []
    dailies = _backups(directory, "daily")
    removed += dailies[:-cfg.daily_keep] if cfg.daily_keep else dailies
    fulls = _backups(directory, "full")
    keep = set(fulls[-cfg.full_keep:]) if cfg.full_keep else set()
    newest_per_week: dict[tuple[int, int], Path] = {}
    for p in fulls:  # oldest first: the last one per week wins
        newest_per_week[_taken_at(p).isocalendar()[:2]] = p
    for week in sorted(newest_per_week)[-cfg.weekly_keep:] if cfg.weekly_keep else []:
        keep.add(newest_per_week[week])
    removed += [p for p in fulls if p not in keep]
    removed += [p for p in directory.glob("*.partial") if p.stat().st_mtime < time.time() - 86400]
    for p in removed:
        p.unlink()
        log.info("removed old backup %s", p.name)
    return removed


def full_due(cfg: BackupSettings) -> bool:
    fulls = _backups(backup_dir(cfg), "full")
    if not fulls:
        return True
    age = time.time() - fulls[-1].stat().st_mtime
    return age >= cfg.full_every_days * 86400 - FULL_EVERY_SLACK


def run(db_path: Path, cfg: BackupSettings, force_full: bool = False) -> list[Path]:
    """The daily job's backup step: a daily backup, a full one when due (or forced), then pruning."""
    made = [daily(db_path, cfg)]
    if force_full or full_due(cfg):
        made.append(full(db_path, cfg))
    prune(cfg)
    return made


def check(cfg: BackupSettings, max_age_days: float = 2) -> tuple[bool, list[str]]:
    """quick_check of the newest daily and full backup, and whether they are recent enough."""
    directory = backup_dir(cfg)
    ok, out = True, []
    for kind, limit in (("daily", max_age_days), ("full", cfg.full_every_days + max_age_days)):
        found = _backups(directory, kind)
        if not found:
            ok = False
            out.append(f"{kind}: none in {directory}")
            continue
        newest = found[-1]
        age = (time.time() - newest.stat().st_mtime) / 86400
        if kind == "daily":
            tmp = directory / (newest.name + ".check.partial")
            with gzip.open(newest, "rb") as f_in, open(tmp, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out, 1024 * 1024)
            result = quick_check(tmp)
            tmp.unlink()
        else:
            result = quick_check(newest)
        stale = age > limit
        ok = ok and result == "ok" and not stale
        out.append(f"{kind}: {newest.name}, {age:.1f} days old{' (TOO OLD)' if stale else ''}, "
                   f"{newest.stat().st_size / 1e6:,.1f} MB, quick_check {result}")
    return ok, out


def checkpoint(db_path: Path) -> str:
    """Moves the WAL into the database and truncates it (end of the daily job).

    TRUNCATE waits for readers and holds off writers while it waits, so it gets only a 1 s busy timeout; if
    others are busy, a PASSIVE checkpoint (never blocks anyone) does what it can instead."""
    con = sqlite3.connect(db_path, timeout=1)
    try:
        busy, log_pages, done = con.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        mode = "truncate"
        if busy:
            busy, log_pages, done = con.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            mode = "passive"
    finally:
        con.close()
    return f"wal checkpoint ({mode}): {done}/{log_pages} pages"
