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
