import gzip
import os
import sqlite3
import time
from datetime import datetime, timedelta

import pytest

from recipe_crawler import backup
from recipe_crawler.config import BackupSettings
from recipe_crawler.db import DB


@pytest.fixture
def live(tmp_path):
    db = DB(tmp_path / "data" / "r.db")
    db.conn.execute("INSERT INTO ingredient_lines (line, n, status, items) VALUES ('salt', 3, 1, '[]')")
    db.conn.execute("INSERT INTO ingredient_names VALUES ('cilantro', 'coriander')")
    db.set_discovered("x.test", 2, 10)
    db.add_urls("x.test", ["https://x.test/1"])
    yield db
    db.close()


@pytest.fixture
def cfg(tmp_path):
    return BackupSettings(dir=tmp_path / "backups")


def test_daily_has_only_the_precious_tables_and_is_consistent(live, cfg, tmp_path):
    path = backup.daily(tmp_path / "data" / "r.db", cfg)
    assert path.name.startswith("daily-") and path.suffix == ".gz"
    raw = tmp_path / "restored.db"
    raw.write_bytes(gzip.decompress(path.read_bytes()))
    con = sqlite3.connect(raw)
    tables = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert tables == {"ingredient_lines", "ingredient_names", "domains"}
    assert con.execute("SELECT line, status, items FROM ingredient_lines").fetchall() == [("salt", 1, "[]")]
    assert "ingredient_lines_todo" in {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
    con.close()
    assert not list(cfg.dir.glob("*.partial"))


def test_full_backup_while_another_connection_writes(live, cfg, tmp_path):
    live.conn.execute("BEGIN IMMEDIATE")  # a writer in the middle of a transaction: not in the backup
    live.conn.execute("INSERT INTO domains(domain) VALUES ('uncommitted')")
    path = backup.full(tmp_path / "data" / "r.db", cfg)
    live.conn.execute("COMMIT")
    assert backup.quick_check(path) == "ok"
    con = sqlite3.connect(path)
    assert con.execute("SELECT COUNT(*) FROM urls").fetchone()[0] == 1
    assert con.execute("SELECT COUNT(*) FROM domains WHERE domain = 'uncommitted'").fetchone()[0] == 0
    con.close()


def test_backup_dir_is_required_and_not_in_icloud(tmp_path):
    with pytest.raises(SystemExit, match="backup"):
        backup.backup_dir(BackupSettings())
    with pytest.raises(SystemExit, match="cloud"):
        backup.backup_dir(BackupSettings(dir=tmp_path / "Library" / "Mobile Documents" / "com~apple~CloudDocs"))


def fake(directory, kind, when):
    directory.mkdir(parents=True, exist_ok=True)
    name = f"{kind}-{when:%Y%m%d-%H%M%S}" + (".db.gz" if kind == "daily" else ".db")
    p = directory / name
    p.write_bytes(b"x")
    os.utime(p, (when.timestamp(), when.timestamp()))
    return p


def test_retention(cfg):
    now = datetime(2026, 10, 31, 9, 0)
    dailies = [fake(cfg.dir, "daily", now - timedelta(days=i)) for i in range(20)]
    fulls = [fake(cfg.dir, "full", now - timedelta(days=i)) for i in (0, 1, 7, 8, 14, 21, 28, 35, 42)]
    stale = cfg.dir / "full-x.db.partial"
    stale.write_bytes(b"x")
    os.utime(stale, (time.time() - 2 * 86400,) * 2)
    backup.prune(cfg)
    left = {p.name for p in cfg.dir.iterdir()}
    assert {p.name for p in dailies[:14]} <= left and not {p.name for p in dailies[14:]} & left
    kept_fulls = {p.name for p in fulls if p.name in left}
    # newest 2 (days 0, 1) + newest of each of the last 4 ISO weeks (days 0, 7, 14, 21: Saturdays)
    assert kept_fulls == {fulls[i].name for i in (0, 1, 2, 4, 5)}
    assert "full-x.db.partial" not in left


def test_run_takes_a_full_backup_when_due_and_check(live, cfg, tmp_path):
    db_path = tmp_path / "data" / "r.db"
    made = backup.run(db_path, cfg)
    assert [p.name.split("-")[0] for p in made] == ["daily", "full"]
    time.sleep(1.1)  # new timestamp in the file name
    made = backup.run(db_path, cfg)
    assert [p.name.split("-")[0] for p in made] == ["daily"]  # full not due yet
    ok, lines = backup.check(cfg)
    assert ok and all("quick_check ok" in line for line in lines)
    newest = sorted(cfg.dir.glob("full-*.db"))[-1]
    newest.write_bytes(b"garbage" * 1000)
    ok, lines = backup.check(cfg)
    assert not ok


def test_cli_backup_and_check(live, tmp_path, capsys):
    path = tmp_path / "config.toml"
    path.write_text('[crawler]\ndb_path = "data/r.db"\n[backup]\ndir = "backups"\n')
    from recipe_crawler.cli import main

    main(["-c", str(path), "backup"])
    out = capsys.readouterr().out
    assert "daily-" in out and "full-" in out and "wal checkpoint" in out
    main(["-c", str(path), "check"])
    assert "quick_check ok" in capsys.readouterr().out


def test_checkpoint_does_not_block_writers_behind_a_reader(live, tmp_path):
    """Regression (review S1): TRUNCATE with a 60 s timeout held off writers while a reader was open."""
    import threading

    path = tmp_path / "data" / "r.db"
    reader = sqlite3.connect(path, isolation_level=None)
    reader.execute("BEGIN")
    reader.execute("SELECT COUNT(*) FROM urls").fetchone()  # an open read snapshot (e.g. `export`)
    live.add_urls("x.test", [f"https://x.test/{i}" for i in range(100)])
    errors = []

    def writer():
        con = sqlite3.connect(path, timeout=5, isolation_level=None)
        try:
            time.sleep(0.1)
            con.execute("INSERT INTO domains(domain) VALUES ('during checkpoint')")
        except sqlite3.OperationalError as e:
            errors.append(e)
        finally:
            con.close()

    t = threading.Thread(target=writer)
    t.start()
    start = time.time()
    result = backup.checkpoint(path)
    t.join()
    assert time.time() - start < 3 and not errors
    assert "passive" in result
    reader.execute("COMMIT")
    assert "truncate" in backup.checkpoint(path)
    reader.close()


def test_daily_refuses_without_enough_space(live, cfg, tmp_path, monkeypatch):
    """Review M4: daily had no free-space check."""
    import collections

    usage = collections.namedtuple("usage", "total used free")
    monkeypatch.setattr(backup.shutil, "disk_usage", lambda p: usage(10, 10, 0))
    with pytest.raises(SystemExit, match="Not enough space"):
        backup.daily(tmp_path / "data" / "r.db", cfg)
    assert not list(cfg.dir.iterdir())


def test_tables_size_without_dbstat(live, tmp_path):
    con = sqlite3.connect(tmp_path / "data" / "r.db")
    with_dbstat = backup._tables_size(con, ("ingredient_lines",))

    class NoDbstat:  # a SQLite build without the dbstat table
        def execute(self, sql, *a):
            if "dbstat" in sql:
                raise sqlite3.OperationalError("no such table: dbstat")
            return con.execute(sql, *a)

    assert with_dbstat > 0 and backup._tables_size(NoDbstat(), ("ingredient_lines",)) > 0
    con.close()
