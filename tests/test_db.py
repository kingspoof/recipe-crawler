import sqlite3

import pytest

from recipe_crawler.db import DB, transaction


def test_failed_add_urls_leaves_no_open_transaction(tmp_path):
    """Regression: a locked add_urls used to leave BEGIN open, and every later write was lost on close."""
    path = tmp_path / "r.db"
    db = DB(path, timeout=0.05)
    other = sqlite3.connect(path, isolation_level=None)
    other.execute("BEGIN IMMEDIATE")  # another process holds the write lock
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        db.add_urls("x", ["https://x/1"])
    assert not db.conn.in_transaction
    other.execute("ROLLBACK")
    other.close()

    assert db.add_urls("x", ["https://x/1", "https://x/2"]) == 2  # no "transaction within a transaction"
    db.mark("https://x/1", "done", 200)
    db.close()
    db = DB(path)
    assert db.conn.execute("SELECT status FROM urls WHERE url = 'https://x/1'").fetchone()[0] == "done"
    db.close()


def test_transaction_rolls_back_and_refuses_to_nest(tmp_path):
    db = DB(tmp_path / "r.db")
    with pytest.raises(ZeroDivisionError):
        with transaction(db.conn):
            db.conn.execute("INSERT INTO domains(domain) VALUES ('x')")
            1 / 0
    assert not db.conn.in_transaction
    assert db.conn.execute("SELECT COUNT(*) FROM domains").fetchone()[0] == 0
    with transaction(db.conn):
        with pytest.raises(RuntimeError):
            with transaction(db.conn):
                pass
    db.close()


def test_migration_waits_for_no_other_process(tmp_path, monkeypatch):
    from recipe_crawler import schema
    from recipe_crawler.db import Busy

    path = tmp_path / "r.db"
    running = DB(path)  # e.g. a long `crawl`: holds the shared lock
    new = schema.CURRENT + 1
    monkeypatch.setattr(schema, "MIGRATIONS", schema.MIGRATIONS + [
        schema.Migration(new, "x", lambda c: c.execute("CREATE TABLE x (a)"), backup=False)])
    monkeypatch.setattr(schema, "CURRENT", new)
    with pytest.raises(Busy, match="stop it"):
        DB(path)
    running.close()
    db = DB(path)  # now it may migrate
    assert schema.version(db.conn) == new
    DB(path).close()  # and others can open it next to it (lock downgraded to shared)
    db.close()


def test_single_instance(tmp_path):
    from recipe_crawler.db import Busy, single_instance
    import os

    fd = single_instance(tmp_path / "r.db", "process")
    with pytest.raises(Busy, match="process"):
        single_instance(tmp_path / "r.db", "process")
    single_instance(tmp_path / "r.db", "crawl")  # a different command is fine
    os.close(fd)
    single_instance(tmp_path / "r.db", "process")


def test_export_raw_lists_its_columns(tmp_path):
    db = DB(tmp_path / "r.db")
    db.conn.execute("ALTER TABLE recipes ADD COLUMN legacy_col TEXT")  # like the real database
    db.conn.execute("INSERT INTO recipes(url, domain, name, ingredients, instructions, raw, legacy_col, crawled_at) "
                    "VALUES ('u', 'x', 'n', '[]', '[]', '{\"a\": 1}', 'old', 0)")
    [rec] = db.iter_recipes(include_raw=True)
    assert rec["raw"] == {"a": 1} and "legacy_col" not in rec and "id" not in rec
    [rec] = db.iter_recipes()
    assert "raw" not in rec
    db.close()
