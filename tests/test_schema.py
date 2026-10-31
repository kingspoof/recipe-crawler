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
    def add_note(conn):
        conn.execute("ALTER TABLE urls ADD COLUMN note TEXT")

    def broken(conn):
        conn.execute("ALTER TABLE urls ADD COLUMN other TEXT")
        raise RuntimeError("bug in migration")

    base = schema.CURRENT
    old_db(tmp_path / "r.db")
    DB(tmp_path / "r.db").close()  # at the current version
    monkeypatch.setattr(schema, "MIGRATIONS", schema.MIGRATIONS + [schema.Migration(base + 1, "note", add_note)])
    monkeypatch.setattr(schema, "CURRENT", base + 1)
    with pytest.raises(SystemExit, match="backup"):
        DB(tmp_path / "r.db")  # no backup configured: refused, nothing changed
    con = sqlite3.connect(tmp_path / "r.db")
    assert schema.version(con) == base
    con.close()

    calls = []
    db = DB(tmp_path / "r.db", backup=lambda: calls.append(1))
    assert calls == [1] and schema.version(db.conn) == base + 1 and "note" in schema.columns(db.conn, "urls")
    db.close()

    monkeypatch.setattr(schema, "MIGRATIONS", schema.MIGRATIONS + [schema.Migration(base + 2, "broken", broken)])
    monkeypatch.setattr(schema, "CURRENT", base + 2)
    with pytest.raises(RuntimeError):
        DB(tmp_path / "r.db", backup=lambda: None)
    con = sqlite3.connect(tmp_path / "r.db")
    assert schema.version(con) == base + 1 and "other" not in schema.columns(con, "urls")  # rolled back together
    con.close()


def test_new_empty_database_needs_no_backup(tmp_path):
    db = DB(tmp_path / "new.db")
    assert schema.version(db.conn) == schema.CURRENT
    db.close()


def test_v2_adds_incomplete_discoveries_to_a_v1_database(tmp_path):
    old_db(tmp_path / "r.db", version=1)  # what this branch's first version left behind
    con = sqlite3.connect(tmp_path / "r.db")
    con.execute("INSERT INTO domains(domain, discovered_at) VALUES ('x', 1)")
    con.commit()
    con.close()
    db = DB(tmp_path / "r.db")
    assert schema.version(db.conn) >= 2
    assert tuple(db.conn.execute("SELECT domain, discovered_at, incomplete_discoveries FROM domains").fetchone()) == (
        "x", 1, 0)
    db.close()


def test_every_migration_is_idempotent(tmp_path):
    """Review N3: each migration also works when its change is already there."""
    db = DB(tmp_path / "r.db")
    for m in schema.MIGRATIONS:
        m.apply(db.conn)  # again, on a database that already has everything
    db.close()
