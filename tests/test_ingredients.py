import json
import sqlite3
import time
from collections import deque

import pytest

from recipe_crawler import gemini, ingredients, schema
from recipe_crawler.config import IngredientSettings, load_config
from recipe_crawler.db import DB
from recipe_crawler.gemini import ApiError, Lane, Line, Processor, Reply, UsageLog, fingerprint

# ingredient_lines as the notebook created it (no `original`), to test the migration on the real layout
NOTEBOOK_LINES = """
CREATE TABLE ingredient_lines (
    id INTEGER PRIMARY KEY, line TEXT NOT NULL UNIQUE, n INTEGER NOT NULL, status INTEGER NOT NULL DEFAULT 0,
    items TEXT, model TEXT, done_at REAL
);
CREATE INDEX ingredient_lines_todo ON ingredient_lines(status, n DESC);
"""


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


class FakeClient:
    """Answers every line with its text as the name, unless `handler` says otherwise."""

    def __init__(self, handler=None):
        self.handler = handler
        self.calls = []

    def generate(self, model, contents, cfg):
        lines = json.loads(contents[len(gemini.PROMPT):])
        self.calls.append([text for _, text in lines])
        if self.handler:
            reply = self.handler(len(self.calls), lines)
            if reply is not None:
                return reply
        return answer(lines)


def answer(lines, skip=()):
    out = [{"i": i, "en": text, "a": 1, "u": "g"} for i, text in lines if text not in skip]
    return Reply(json.dumps(out), {"prompt_tokens": 10, "output_tokens": 5}, "STOP")


@pytest.fixture
def cfg(tmp_path):
    return IngredientSettings(batch_size=2, rpd=100, rpm=6000, in_flight=1, stagger=0,
                              usage_log=tmp_path / "usage.jsonl", keys_file=tmp_path / "keys.txt")


def notebook_db(path):
    """recipes.db as it was before schema versioning: crawler tables + the notebook's ingredient_lines."""
    con = sqlite3.connect(path)
    con.executescript(schema.CRAWLER_TABLES + NOTEBOOK_LINES)
    con.close()


@pytest.fixture
def db(tmp_path):
    notebook_db(tmp_path / "recipes.db")
    d = DB(tmp_path / "recipes.db")  # migrates
    yield d
    d.close()


def add_recipe(conn, ingredients_, url=None):
    cur = conn.execute(
        "INSERT INTO recipes(url, domain, name, ingredients, instructions, crawled_at) VALUES (?, ?, ?, ?, '[]', 0)",
        (url or f"https://x/{time.time_ns()}", "x", "r", json.dumps(ingredients_)))
    return cur.lastrowid


def add_lines(conn, *lines):
    ingredients.ensure_schema(conn)
    conn.executemany("INSERT INTO ingredient_lines (line, n) VALUES (?, ?)",
                     [(line, 100 - i) for i, line in enumerate(lines)])


def run(db, cfg, clients, max_requests=None, clock=None, keys=("k1",), lanes=None, sleeps=None):
    clock = clock or FakeClock()
    usage = UsageLog(cfg.usage_log)
    used = usage.used_today(list(keys))
    lanes = lanes or [Lane(k, c, "m", used[fingerprint(k), "m"], cfg.rpd) for k, c in zip(keys, clients)]
    p = Processor(cfg, lanes, usage, ingredients.make_save(db.conn), max_requests=max_requests, clock=clock,
                  sleep=clock.sleep, retry_sleep=(sleeps.append if sleeps is not None else lambda s: None))
    return p, p.run(ingredients.todo_lines(db.conn))


def statuses(conn):
    return dict(conn.execute("SELECT line, status FROM ingredient_lines"))


def usage_records(cfg):
    return [json.loads(line) for line in cfg.usage_log.read_text().splitlines()]


# --------------------------------------------------------------------------- process


def test_normal_batch_saved_most_common_first(db, cfg):
    add_lines(db.conn, "a", "b", "c")
    client = FakeClient()
    _, reason = run(db, cfg, [client])
    assert reason == "done"
    assert client.calls == [["a", "b"], ["c"]]
    row = db.conn.execute("SELECT status, items, model, done_at FROM ingredient_lines WHERE line = 'a'").fetchone()
    assert row[0] == 1 and json.loads(row[1]) == [["a", 1, "g"]] and row[2] == "m" and row[3]
    recs = usage_records(cfg)
    assert len(recs) == 2 and all(r["key"] == fingerprint("k1") and r["finish"] == "STOP" for r in recs)


def test_original_spelling_is_sent(db, cfg):
    add_lines(db.conn, "5 t cream")
    db.conn.execute("UPDATE ingredient_lines SET original = '5 T cream'")
    client = FakeClient()
    run(db, cfg, [client])
    assert client.calls == [["5 T cream"]]
    assert 'uppercase "T" is "tbsp"' in gemini.PROMPT


def test_left_out_line_retried_then_failed(db, cfg):
    add_lines(db.conn, "a", "b")
    client = FakeClient(lambda n, lines: answer(lines, skip={"b"}))
    run(db, cfg, [client])
    assert client.calls == [["a", "b"], ["b"]]  # max_attempts = 2
    assert statuses(db.conn) == {"a": 1, "b": -1}


def test_bad_json_counts_as_attempt(db, cfg):
    add_lines(db.conn, "a")
    client = FakeClient(lambda n, lines: Reply("[{oops", {}, "MAX_TOKENS") if n == 1 else None)
    run(db, cfg, [client])
    assert len(client.calls) == 2 and statuses(db.conn) == {"a": 1}
    assert usage_records(cfg)[0]["error"].startswith("bad json")


def test_503_retried_with_backoff(db, cfg):
    add_lines(db.conn, "a")

    def handler(n, lines):
        if n <= 2:
            raise ApiError(503, "overloaded")

    sleeps = []
    p, _ = run(db, cfg, [FakeClient(handler)], sleeps=sleeps)
    assert sleeps == [15, 30]
    assert statuses(db.conn) == {"a": 1}
    recs = usage_records(cfg)
    assert [r.get("error", "")[:3] for r in recs] == ["503", "503", ""] and recs[-1]["retries"] == 2
    assert p.lanes[0].used == 3  # every retry is a request


def test_network_error_gives_up_and_requeues(db, cfg):
    add_lines(db.conn, "a")

    def handler(n, lines):
        if n <= 6:  # 1 + MAX_RETRIES attempts fail: the batch goes back in the queue, same attempt count
            raise ConnectionResetError()

    client = FakeClient(handler)
    run(db, cfg, [client])
    assert len(client.calls) == 7 and statuses(db.conn) == {"a": 1}


def test_429_short_term_pauses_then_continues(db, cfg):
    add_lines(db.conn, "a")
    clock = FakeClock()
    client = FakeClient(lambda n, lines: (_ for _ in ()).throw(ApiError(429, "slow down", {})) if n == 1 else None)
    _, reason = run(db, cfg, [client], clock=clock)
    assert reason == "done" and statuses(db.conn) == {"a": 1}
    assert clock.t >= 1000 + 120  # paused 2 min


def test_429_per_day_exhausts_lane(db, cfg):
    add_lines(db.conn, "a", "b", "c")
    details = [{"violations": [{"quotaId": "GenerateRequestsPerDayPerProjectPerModel-FreeTier"}]}]
    blocked = FakeClient(lambda n, lines: (_ for _ in ()).throw(ApiError(429, "quota", details)))
    p, reason = run(db, cfg, [blocked])
    assert reason == "quota" and p.lanes[0].exhausted
    assert len(blocked.calls) == 1 and set(statuses(db.conn).values()) == {0}
    assert "PerDay" in usage_records(cfg)[0]["details"]


def test_rpd_reached_exits(db, cfg):
    cfg.rpd, cfg.batch_size = 3, 1
    add_lines(db.conn, "a", "b", "c", "d", "e")
    UsageLog(cfg.usage_log).write({"day": gemini.quota_day(), "key": fingerprint("k1"), "model": "m"})
    client = FakeClient()
    _, reason = run(db, cfg, [client])
    assert reason == "quota" and len(client.calls) == 2  # 1 already used today
    assert list(statuses(db.conn).values()).count(1) == 2


def test_max_requests(db, cfg):
    cfg.batch_size = 1
    add_lines(db.conn, "a", "b", "c")
    client = FakeClient()
    _, reason = run(db, cfg, [client], max_requests=2)
    assert reason == "max_requests" and len(client.calls) == 2


def test_old_integer_keys_counted_for_fingerprint(tmp_path, cfg):
    cfg.keys_file.write_text("# comment\nk1\n\nk2\nk1\n")
    keys = gemini.load_keys(cfg.keys_file)
    assert keys == ["k1", "k2"]
    log = UsageLog(cfg.usage_log)
    today = gemini.quota_day()
    for rec in ({"key": 1}, {"key": 1}, {"key": 0}, {"key": 7}, {"key": fingerprint("k2")}, {"key": 1, "day": "2000-01-01"}):
        log.write({"day": today, "model": "m", **rec})
    with open(cfg.usage_log, "a") as f:
        f.write('{"half a line')
    used = log.used_today(keys)
    assert used == {(fingerprint("k2"), "m"): 3, (fingerprint("k1"), "m"): 1, ("#7", "m"): 1}
    assert any(f"{fingerprint('k2')}/m: 3/100" in line for line in ingredients.quota_report(cfg, keys))


def test_stop_saves_in_flight(db, cfg):
    cfg.in_flight = 2
    add_lines(db.conn, "a", "b", "c", "d", "e", "f")
    holder = {}

    def handler(n, lines):
        holder["p"].stop()  # like SIGINT while this request is in flight
        time.sleep(0.05)
        return answer(lines)

    client = FakeClient(handler)
    clock = FakeClock()
    usage = UsageLog(cfg.usage_log)
    p = Processor(cfg, [Lane("k1", client, "m", 0, cfg.rpd)], usage, ingredients.make_save(db.conn),
                  clock=clock, sleep=clock.sleep)
    holder["p"] = p
    assert p.run(ingredients.todo_lines(db.conn)) == "stopped"
    done = [line for line, s in statuses(db.conn).items() if s == 1]
    assert len(done) == 2 * len(client.calls) and 0 < len(client.calls) <= 2


# --------------------------------------------------------------------------- lines


def test_lines_keeps_done_and_fills_original(db, cfg):
    add_recipe(db.conn, ["5 T cream", "Salt", "salt", "  salt ", "x" * 201])
    add_recipe(db.conn, ["5 T  Cream", "5 T cream", "Salt"])
    add_lines(db.conn)  # migrates the notebook table
    db.conn.execute("INSERT INTO ingredient_lines (line, n, status, items, model) VALUES ('salt', 1, 1, '[]', 'm')")
    ingredients.collect_lines(db.conn, cfg)
    rows = {r[0]: tuple(r[1:]) for r in db.conn.execute(
        "SELECT line, n, status, items, original FROM ingredient_lines")}
    assert rows["salt"] == (4, 1, "[]", None)  # done stays done; "salt" 2x vs "Salt" 2x: no majority for Salt
    assert rows["5 t cream"] == (3, 0, None, "5 T cream")
    assert rows["x" * 201][1] == -2
    ingredients.collect_lines(db.conn, cfg)  # idempotent
    assert {r[0]: tuple(r[1:]) for r in db.conn.execute(
        "SELECT line, n, status, items, original FROM ingredient_lines")} == rows


def test_migration_is_additive(db):
    db.conn.execute("INSERT INTO ingredient_lines (line, n, status, items) VALUES ('a', 1, 1, '[]')")
    ingredients.ensure_schema(db.conn)
    ingredients.ensure_schema(db.conn)
    assert [tuple(r) for r in db.conn.execute("SELECT line, status, original FROM ingredient_lines")] == [
        ("a", 1, None)]


# --------------------------------------------------------------------------- map


def done(conn, line, items, at):
    conn.execute("INSERT INTO ingredient_lines (line, n, status, items, done_at) VALUES (?, 1, 1, ?, ?) "
                 "ON CONFLICT(line) DO UPDATE SET status = 1, items = excluded.items, done_at = excluded.done_at",
                 (line, json.dumps(items), at))


def ri(conn):
    return sorted(tuple(r) for r in conn.execute(
        "SELECT recipe_id, pos, line_id, name, amount, unit FROM recipe_ingredients"))


def test_map_incremental_and_full(db, cfg):
    ingredients.ensure_schema(db.conn)
    r1 = add_recipe(db.conn, ["2 EL Zucker", "salz"])
    r2 = add_recipe(db.conn, ["1 egg"])
    r3 = add_recipe(db.conn, ["salz", "pepper"])
    done(db.conn, "2 el zucker", [["sugar", 2, "EL"]], 1)
    done(db.conn, "1 egg", [["egg", 1, None]], 1)
    db.conn.execute("INSERT INTO ingredient_names VALUES ('egg', 'chicken egg')")
    stats = ingredients.map_ingredients(db.conn, cfg)  # first run: full
    assert stats["rebuilt"] == 3
    assert [r[3:] for r in ri(db.conn)] == [("sugar", 2, "tbsp"), ("chicken egg", 1, None)]
    state = dict(db.conn.execute("SELECT recipe_id, pending FROM recipe_ingredients_state"))
    assert state == {r1: 1, r2: 0, r3: 2}

    before = dict(db.conn.execute("SELECT recipe_id, mapped_at FROM recipe_ingredients_state"))
    time.sleep(0.01)
    done(db.conn, "salz", [["salt", None, None]], time.time())
    r4 = add_recipe(db.conn, ["1 egg"])
    stats = ingredients.map_ingredients(db.conn, cfg)
    assert stats["recipes"] == 3 and stats["rebuilt"] == 3  # checked r1, r3 (pending), r4 (new); not r2
    after = dict(db.conn.execute("SELECT recipe_id, mapped_at FROM recipe_ingredients_state"))
    assert after[r2] == before[r2] and after[r1] > before[r1] and after[r3] > before[r3] and r4 in after
    stats = ingredients.map_ingredients(db.conn, cfg)  # r3 ("pepper") still pending: checked, nothing new
    assert stats["recipes"] == 1 and stats["rebuilt"] == 0

    incremental = ri(db.conn)
    ingredients.map_ingredients(db.conn, cfg, full=True)
    assert ri(db.conn) == incremental
    assert {r[0] for r in incremental} == {r1, r2, r3, r4}


def test_unit_fix_and_amount_cap():
    clean = ingredients.clean
    assert clean("Sugar", 2, "EK") == ("sugar", 2, "tbsp")
    assert clean("oil", 1, "pl") == ("oil", 1, "tbsp")
    assert clean("herbs", 1, "hrst") == ("herbs", 1, "handful")
    assert clean("salt", 1, "tk") == ("salt", 1, "tsp")
    assert clean("oil", 1, "c.s.") == ("oil", 1, "tbsp") == clean("oil", 1, "cs")
    for u in ("ks", "stk", "st"):
        assert clean("egg", 2, u) == ("egg", 2, "piece")
    for u in ("tbs", "tbl"):
        assert clean("oil", 1, u) == ("oil", 1, "tbsp")
    assert clean("cream", 5, "t") == ("cream", 5, "t") and clean("milk", 1, "c") == ("milk", 1, "c")
    assert clean("aubergine", 500000, "g") == ("aubergine", None, "g")
    assert clean("water", 600, "dl") == ("water", None, "ml")  # 60,000 ml after the conversion
    assert clean("flour", 50000, "g") == ("flour", 50000, "g")
    assert clean("rice", 200000, "cup") == ("rice", None, "cup")
    assert clean("rice", 2000, "cup") == ("rice", 2000, "cup")
    assert clean(None, 1, "g") is None and clean("x", True, "g") == ("x", None, "g")


# --------------------------------------------------------------------------- config / cli


def test_ingredient_commands_do_not_need_contact(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text('[crawler]\ncontact = ""\ndb_path = "r.db"\n[ingredients]\nbatch_size = 10\nmodels = ["a", "b"]\n')
    s = load_config(path, require_contact=False)
    assert s.ingredients.batch_size == 10 and s.ingredients.models == ["a", "b"]
    assert s.ingredients.keys_file == tmp_path / "gemini_keys.txt" and s.ingredients.rpd == 500
    with pytest.raises(SystemExit):
        load_config(path)


def test_cli_dry_run_and_stats(tmp_path, capsys):
    path = tmp_path / "config.toml"
    path.write_text('[crawler]\ncontact = ""\ndb_path = "r.db"\n')
    (tmp_path / "gemini_keys.txt").write_text("k1\n")
    notebook_db(tmp_path / "r.db")
    con = sqlite3.connect(tmp_path / "r.db")
    con.execute("INSERT INTO ingredient_lines (line, n) VALUES ('a', 1)")
    con.commit()
    con.close()
    from recipe_crawler.cli import main

    main(["-c", str(path), "process", "--dry-run", "--keys", str(tmp_path / "gemini_keys.txt")])
    out = capsys.readouterr().out
    assert "1 lines to do" in out and '[0, "a"]' in out and fingerprint("k1") in out
    con = sqlite3.connect(tmp_path / "r.db")
    assert "original" not in {r[1] for r in con.execute("PRAGMA table_info(ingredient_lines)")}  # untouched
    con.close()
    main(["-c", str(path), "stats"])
    out = capsys.readouterr().out
    assert "to do" in out and "next reset" in out


# --------------------------------------------------------------------------- review fixes


def test_config_error_disables_lane_without_failing_lines(db, cfg):
    add_lines(db.conn, "a", "b", "c")
    bad = FakeClient(lambda n, lines: (_ for _ in ()).throw(ApiError(400, "API key not valid", None)))
    good = FakeClient()
    p, reason = run(db, cfg, [bad, good], keys=("bad", "good"))
    assert reason == "done" and p.lanes[0].exhausted and len(bad.calls) == 1
    assert set(statuses(db.conn).values()) == {1}  # the bad key's batch went to the good key, nothing failed


def test_original_needs_to_beat_lowercase_only(db, cfg):
    add_recipe(db.conn, ["Salt"] * 4 + ["SALT"] * 3 + ["salt"] * 2)
    ingredients.collect_lines(db.conn, cfg)
    assert db.conn.execute("SELECT original FROM ingredient_lines WHERE line = 'salt'").fetchone()[0] == "Salt"


def test_reset_failed_line_is_mapped_incrementally(db, cfg):
    ingredients.ensure_schema(db.conn)
    r1 = add_recipe(db.conn, ["1 egg", "pan de chancaca"])
    done(db.conn, "1 egg", [["egg", 1, None]], 1)
    db.conn.execute("INSERT INTO ingredient_lines (line, n, status) VALUES ('pan de chancaca', 1, -1)")
    ingredients.map_ingredients(db.conn, cfg)
    assert db.conn.execute("SELECT pending FROM recipe_ingredients_state").fetchone()[0] == 1
    time.sleep(0.01)
    db.conn.execute("UPDATE ingredient_lines SET status = 0 WHERE status = -1")  # reset by hand, then processed
    done(db.conn, "pan de chancaca", [["chancaca", 1, "piece"]], time.time())
    assert ingredients.map_ingredients(db.conn, cfg)["rebuilt"] == 1
    assert [r[3] for r in ri(db.conn)] == ["egg", "chancaca"]
    assert dict(db.conn.execute("SELECT recipe_id, pending FROM recipe_ingredients_state")) == {r1: 0}


def test_per_day_429_remembered_by_next_run(db, cfg):
    add_lines(db.conn, "a")
    details = [{"violations": [{"quotaId": "GenerateRequestsPerDayPerProjectPerModel-FreeTier"}]}]
    blocked = FakeClient(lambda n, lines: (_ for _ in ()).throw(ApiError(429, "quota", details)))
    run(db, cfg, [blocked])
    usage = UsageLog(cfg.usage_log)
    assert usage.used_today(["k1"])[fingerprint("k1"), "m"] == 1
    assert usage.exhausted == {(fingerprint("k1"), "m")}


def test_network_outage_never_fails_lines(db, cfg):
    """Regression (review B1): ConnectError on the first 24 calls left innocent lines at -1."""
    add_lines(db.conn, "a", "b")
    client = FakeClient(lambda n, lines: (_ for _ in ()).throw(ConnectionError("down")) if n <= 24 else None)
    clock = FakeClock()
    p, reason = run(db, cfg, [client], clock=clock)
    assert reason == "done" and statuses(db.conn) == {"a": 1, "b": 1} and p.failed == 0
    assert clock.t > 1000 + 2 * 60  # the lane paused during the outage


def test_service_wide_5xx_never_fails_lines(db, cfg):
    cfg.batch_size = 1
    add_lines(db.conn, "a", "b", "c")
    client = FakeClient(lambda n, lines: (_ for _ in ()).throw(ApiError(500, "internal")) if n <= 40 else None)
    p, reason = run(db, cfg, [client])
    assert reason == "done" and set(statuses(db.conn).values()) == {1} and p.failed == 0


def test_batch_specific_5xx_is_split_down_to_the_bad_line(db, cfg):
    cfg.batch_size = 4
    add_lines(db.conn, "a", "b", "poison", "d", "e", "f", "g", "h")

    def handler(n, lines):
        if any(text == "poison" for _, text in lines):
            raise ApiError(500, "internal")

    client = FakeClient(handler)
    p, reason = run(db, cfg, [client])
    assert reason == "done"
    assert statuses(db.conn) == {"a": 1, "b": 1, "poison": gemini.GAVE_UP, "d": 1, "e": 1, "f": 1, "g": 1, "h": 1}
    poisoned = [c for c in client.calls if "poison" in c]
    # 4 -> 2 -> 1, each call = 1 + MAX_RETRIES requests: a bounded cost, not the whole day
    assert [len(c) for c in poisoned][-1] == 1 and len(poisoned) <= 6 * (1 + gemini.MAX_RETRIES)
    assert any("gave up" in row for row in ingredients.progress(db.conn))


def test_429_is_never_charged_to_the_lines(db, cfg):
    add_lines(db.conn, "a")
    clock = FakeClock()
    client = FakeClient(lambda n, lines: (_ for _ in ()).throw(ApiError(429, "slow down", {})) if n <= 3 else None)
    run(db, cfg, [client], clock=clock)
    assert statuses(db.conn) == {"a": 1}


def test_local_bug_is_not_a_network_error(db, cfg):
    add_lines(db.conn, "a")
    client = FakeClient(lambda n, lines: (_ for _ in ()).throw(TypeError("bug in our code")))
    with pytest.raises(TypeError):
        run(db, cfg, [client])
    assert len(client.calls) == 1  # not retried 5 times as if it were a connection reset


def test_truncated_answer_keeps_complete_objects(db, cfg):
    """Regression: a MAX_TOKENS answer failed the whole batch; after 2 tries every line was -1."""
    cfg.batch_size = 4
    add_lines(db.conn, "a", "b", "c", "d")

    def handler(n, lines):
        if n == 1:
            text = answer(lines).text
            return Reply(text[:text.index('{"i": 2')] + '{"i": 2, "en": "c', {}, "MAX_TOKENS")  # cut inside c

    client = FakeClient(handler)
    run(db, cfg, [client])
    assert statuses(db.conn) == {"a": 1, "b": 1, "c": 1, "d": 1}
    assert client.calls == [["a", "b", "c", "d"], ["c", "d"]]
    rec = usage_records(cfg)[0]
    assert rec["truncated"] == 2 and rec["error"].startswith("bad json")


def test_salvage():
    assert gemini.salvage('[{"i": 0}, {"i": 1}, {"i"') == [{"i": 0}, {"i": 1}]
    assert gemini.salvage("[") == [] and gemini.salvage("oops") == []


def test_unwritable_database_spools_answers_and_stops(db, cfg):
    """Regression: an error in save() crashed the loop and lost every request in flight."""
    cfg.batch_size = 1
    add_lines(db.conn, "a", "b", "c")
    calls = []

    def save(answered, failed, model, gave_up=()):
        calls.append(len(answered))
        raise sqlite3.OperationalError("database or disk is full")

    clock = FakeClock()
    usage = UsageLog(cfg.usage_log)
    client = FakeClient()
    p = Processor(cfg, [Lane("k1", client, "m", 0, cfg.rpd)], usage, save, clock=clock, sleep=clock.sleep,
                  retry_sleep=lambda s: None, spool=ingredients.spool_path(cfg))
    assert p.run(ingredients.todo_lines(db.conn)) == "stopped"
    assert len(calls) == gemini.SAVE_TRIES and len(client.calls) == 1  # retried, then no new requests
    assert set(statuses(db.conn).values()) == {0}

    assert ingredients.apply_spool(db.conn, cfg) == 1  # the next `process` saves them first
    assert statuses(db.conn)["a"] == 1 and not ingredients.spool_path(cfg).exists()
    assert ingredients.apply_spool(db.conn, cfg) == 0


def test_bug_in_finished_waits_for_in_flight_then_raises(db, cfg, monkeypatch):
    cfg.in_flight = 2
    add_lines(db.conn, "a", "b", "c", "d")
    client = FakeClient(lambda n, lines: time.sleep(0.1) or None)  # both requests in flight at the same time
    p = Processor(cfg, [Lane("k1", client, "m", 0, cfg.rpd)], UsageLog(cfg.usage_log),
                  ingredients.make_save(db.conn))  # real clock: rpm 6000 = 10 ms between requests
    real = p._finished
    n = []

    def finished(*a):
        n.append(1)
        if len(n) == 1:
            raise KeyError("bug")
        return real(*a)

    monkeypatch.setattr(p, "_finished", finished)
    with pytest.raises(KeyError):
        p.run(ingredients.todo_lines(db.conn))
    assert len(client.calls) == 2 and list(statuses(db.conn).values()).count(1) == 2  # the other one was saved


def test_build_lanes_starts_per_day_429_lanes_exhausted(cfg):
    cfg.models = ["m1", "m2"]
    usage = UsageLog(cfg.usage_log)
    usage.write({"day": gemini.quota_day(), "key": fingerprint("k1"), "model": "m2", "details": "PerDay"})
    used = usage.used_today(["k1"])
    lanes = ingredients.build_lanes(cfg, ["k1"], usage, used, client=lambda k: FakeClient())
    assert [(lane.model, lane.used, lane.exhausted) for lane in lanes] == [("m1", 0, False), ("m2", 1, True)]


def test_map_full_crash_keeps_the_old_table_and_writers_get_in_between(db, cfg, monkeypatch, tmp_path):
    ingredients.ensure_schema(db.conn)
    for i in range(5):
        add_recipe(db.conn, ["1 egg"])
    done(db.conn, "1 egg", [["egg", 1, None]], 1)
    ingredients.map_ingredients(db.conn, cfg)
    before = ri(db.conn)
    monkeypatch.setattr(ingredients, "FULL_CHUNK", 2)

    other = sqlite3.connect(tmp_path / "recipes.db", timeout=0, isolation_level=None)
    calls = []
    real_norm = ingredients.norm

    def norm(x):
        calls.append(x)
        if len(calls) == 3:  # between chunk 1 and 2: another process writes (would fail if map held the lock)
            other.execute("INSERT INTO domains(domain) VALUES ('written during map --full')")
        if len(calls) == 5:
            raise MemoryError("crash in the middle")
        return real_norm(x)

    monkeypatch.setattr(ingredients, "norm", norm)
    with pytest.raises(MemoryError):
        ingredients.map_ingredients(db.conn, cfg, full=True)
    assert not db.conn.in_transaction and ri(db.conn) == before  # old table untouched
    assert other.execute("SELECT COUNT(*) FROM domains").fetchone()[0] == 1
    other.close()
    monkeypatch.setattr(ingredients, "norm", real_norm)
    ingredients.map_ingredients(db.conn, cfg, full=True)  # the leftover _new tables are replaced
    assert ri(db.conn) == before
    assert not schema.has_table(db.conn, "recipe_ingredients_new")


def test_after_the_first_spool_answers_go_straight_to_the_spool(db, cfg):
    """Regression (review S4): every later answer still tried the broken database 3 times."""
    cfg.batch_size, cfg.in_flight = 1, 3
    add_lines(db.conn, "a", "b", "c")
    tries = []

    def save(answered, failed, model, gave_up=()):
        tries.append(1)
        raise sqlite3.OperationalError("database is locked")

    client = FakeClient(lambda n, lines: time.sleep(0.05) or None)  # three requests in flight at once
    p = Processor(cfg, [Lane("k1", client, "m", 0, cfg.rpd)], UsageLog(cfg.usage_log), save,
                  retry_sleep=lambda s: None, spool=ingredients.spool_path(cfg))
    assert p.run(ingredients.todo_lines(db.conn)) == "stopped"
    assert len(tries) == gemini.SAVE_TRIES  # only the first answer tried the database
    assert len(gemini.spool_read(ingredients.spool_path(cfg))) == len(client.calls) == 3


def test_save_connection_gets_a_short_busy_timeout(db):
    ingredients.make_save(db.conn)
    assert db.conn.execute("PRAGMA busy_timeout").fetchone()[0] == ingredients.SAVE_BUSY_TIMEOUT_MS


def test_no_busy_polling_while_only_waiting_for_requests_in_flight(db, cfg, monkeypatch):
    """Regression (review M1): with nothing left to send the loop waited 10 ms at a time."""
    cfg.batch_size, cfg.in_flight = 1, 2  # the lane could send more: only "nothing left" stops it
    add_lines(db.conn, "a")
    timeouts = []
    real_wait = gemini.wait

    def wait(fs, timeout=None, return_when=None):
        timeouts.append(timeout)
        return real_wait(fs, timeout=timeout, return_when=return_when)

    monkeypatch.setattr(gemini, "wait", wait)
    client = FakeClient(lambda n, lines: time.sleep(0.3) or None)
    p = Processor(cfg, [Lane("k1", client, "m", 0, cfg.rpd)], UsageLog(cfg.usage_log),
                  ingredients.make_save(db.conn))  # real clock: the lane is ready again after 10 ms
    assert p.run(ingredients.todo_lines(db.conn)) == "done"
    assert len(timeouts) <= 2 and all(t == 2.0 for t in timeouts)


def test_lines_commits_in_chunks(db, cfg, monkeypatch):
    """Review M3: ~2M upserts in one transaction held the write lock for tens of seconds."""
    monkeypatch.setattr(ingredients, "LINES_CHUNK", 2)
    add_recipe(db.conn, ["a", "b", "c", "d", "e", "x" * 201])
    commits = []
    db.conn.set_trace_callback(lambda sql: commits.append(sql) if sql == "COMMIT" else None)
    ingredients.collect_lines(db.conn, cfg)
    db.conn.set_trace_callback(None)
    assert len(commits) == 3 + 1  # 6 lines in chunks of 2, then the status update
    assert len(statuses(db.conn)) == 6 and statuses(db.conn)["x" * 201] == -2


def test_one_model_down_does_not_blame_its_batches(db, cfg):
    """Review N1: with a global success counter, a model in a 5xx outage got its batches blamed (split, -3)
    because another model kept succeeding."""
    cfg.batch_size, cfg.rpm, cfg.rpd = 2, 60, 10_000  # ~1 request/s per lane: the bad lane retries while work is left
    add_lines(db.conn, *[f"line {i}" for i in range(4000)])  # ~35 min of work: the bad lanes come back
    down = FakeClient(lambda n, lines: (_ for _ in ()).throw(ApiError(500, "internal")))
    up = FakeClient()
    clock = FakeClock()
    usage = UsageLog(cfg.usage_log)
    # like the reviewer's simulation: three keys on the model that is down, one on the model that works
    lanes = [Lane(k, down, "m-down", 0, cfg.rpd) for k in ("k1", "k2", "k3")] + [Lane("k1", up, "m-up", 0, cfg.rpd)]
    p, reason = run(db, cfg, None, clock=clock, lanes=lanes)
    assert reason == "done" and set(statuses(db.conn).values()) == {1} and p.failed == 0
    assert max(len(c) for c in down.calls) == 2  # never split
    # each bad lane pauses 2, 4, 8, 16 ... min: a handful of calls per lane, not one per batch (~2000)
    assert len(down.calls) <= 3 * 6 * (1 + gemini.MAX_RETRIES)



def test_blame_needs_successes_of_the_same_model(cfg):
    """Review N1, pinned at the decision: successes of another model do not blame a batch, and a lane that
    only fails is paused even when its batch is blamed."""
    clock = FakeClock()
    down = Lane("k1", FakeClient(), "m-down", 0, cfg.rpd)
    down2 = Lane("k2", FakeClient(), "m-down", 0, cfg.rpd)
    up = Lane("k1", FakeClient(), "m-up", 0, cfg.rpd)
    p = Processor(cfg, [down, down2, up], UsageLog(cfg.usage_log), lambda *a: None, clock=clock, sleep=clock.sleep)
    batch = [Line(1, "a"), Line(2, "b")]
    retry, todo = deque(), deque([Line(9, "other")])

    p._server_error(down, batch, "500 internal", retry, todo)  # first failure: never blamed
    assert list(todo)[-2:] == batch and not retry
    todo = deque([Line(9, "other")])
    p.successes["m-up"] += 5  # only the other model got answers since
    p._server_error(down, batch, "500 internal", retry, todo)
    assert list(todo)[-2:] == batch and not retry and all(line.max_batch == 0 for line in batch)  # not split

    todo = deque([Line(9, "other")])
    p.successes["m-down"] += 1  # now the same model works for other batches: this batch is to blame
    down2.ok_at_failure = down2.ok  # down2 failed before and got no answer since
    p._server_error(down2, batch, "500 internal", retry, todo)
    assert list(retry) == batch and all(line.max_batch == 1 for line in batch)  # split
    assert down2.paused_until > clock.t  # down2 never got an answer: paused although the batch is blamed
