import sqlite3

import pytest

from recipe_crawler import schema
from recipe_crawler.db import DB

NOTEBOOK_LINES = """
CREATE TABLE ingredient_lines (
    id INTEGER PRIMARY KEY, line TEXT NOT NULL UNIQUE, n INTEGER NOT NULL, status INTEGER NOT NULL DEFAULT 0,
    items TEXT, model TEXT, done_at REAL
);
"""


def old_db(path, version=0):
    con = sqlite3.connect(path)
    con.executescript(schema.CRAWLER_TABLES + NOTEBOOK_LINES)
    con.execute("INSERT INTO ingredient_lines (line, n, status, items) VALUES ('salt', 5, 1, '[]')")
    con.execute("INSERT INTO urls (url, domain) VALUES ('https://x/1', 'x')")
    con.execute(f"PRAGMA user_version = {version}")
    con.commit()
    con.close()


def test_baseline_on_the_existing_layout_keeps_everything(tmp_path):
    old_db(tmp_path / "r.db")
    calls = []
    db = DB(tmp_path / "r.db", backup=lambda: calls.append(1))
    assert schema.version(db.conn) == schema.CURRENT and calls == []  # baseline: no backup needed
    assert [tuple(r) for r in db.conn.execute("SELECT line, status, items, original FROM ingredient_lines")] == [
        ("salt", 1, "[]", None)]
    assert db.conn.execute("SELECT COUNT(*) FROM urls").fetchone()[0] == 1
    assert schema.has_table(db.conn, "recipe_ingredients")
    db.close()
    assert schema.migrate(DB(tmp_path / "r.db").conn) == 0  # idempotent


def test_newer_database_is_refused_for_writing(tmp_path):
    old_db(tmp_path / "r.db", version=schema.CURRENT + 1)
    with pytest.raises(schema.SchemaTooNew):
        DB(tmp_path / "r.db")
    DB(tmp_path / "r.db", readonly=True).close()  # reading still works (with a warning)


def test_readonly_never_migrates_or_creates(tmp_path):
    with pytest.raises(SystemExit):
        DB(tmp_path / "missing.db", readonly=True)
    assert not (tmp_path / "missing.db").exists()
    old_db(tmp_path / "r.db")
    db = DB(tmp_path / "r.db", readonly=True)
    assert schema.version(db.conn) == 0 and "original" not in schema.columns(db.conn, "ingredient_lines")
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        db.conn.execute("DELETE FROM urls")
    db.close()


def test_migration_needing_a_backup(tmp_path, monkeypatch):
    def v2(conn):
        conn.execute("ALTER TABLE urls ADD COLUMN note TEXT")

    def v3_broken(conn):
        conn.execute("ALTER TABLE urls ADD COLUMN other TEXT")
        raise RuntimeError("bug in migration")

    old_db(tmp_path / "r.db", version=1)
    monkeypatch.setattr(schema, "MIGRATIONS", schema.MIGRATIONS + [schema.Migration(2, "note", v2)])
    monkeypatch.setattr(schema, "CURRENT", 2)
    with pytest.raises(SystemExit, match="backup"):
        DB(tmp_path / "r.db")  # no backup configured: refused, nothing changed
    con = sqlite3.connect(tmp_path / "r.db")
    assert schema.version(con) == 1
    con.close()

    calls = []
    db = DB(tmp_path / "r.db", backup=lambda: calls.append(1))
    assert calls == [1] and schema.version(db.conn) == 2 and "note" in schema.columns(db.conn, "urls")
    db.close()

    monkeypatch.setattr(schema, "MIGRATIONS", schema.MIGRATIONS + [schema.Migration(3, "broken", v3_broken)])
    monkeypatch.setattr(schema, "CURRENT", 3)
    with pytest.raises(RuntimeError):
        DB(tmp_path / "r.db", backup=lambda: None)
    con = sqlite3.connect(tmp_path / "r.db")
    assert schema.version(con) == 2 and "other" not in schema.columns(con, "urls")  # rolled back together
    con.close()


def test_new_empty_database_needs_no_backup(tmp_path):
    db = DB(tmp_path / "new.db")
    assert schema.version(db.conn) == schema.CURRENT
    db.close()
