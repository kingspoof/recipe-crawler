import json
import sqlite3
import time

import pytest

from recipe_crawler import classify, graph, schema
from recipe_crawler.config import IngredientSettings
from recipe_crawler.db import DB
from recipe_crawler.gemini import Lane, Processor, Reply, UsageLog


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


INFO = {"butter": ("vegetarian", "dairy", ["milk"]), "flour": ("vegan", "grain", ["gluten"]),
        "bacon": ("meat", "meat", []), "fish sauce": ("pescatarian", "sauce_condiment", ["fish"]),
        "stock": ("unknown", "sauce_condiment", ["celery"]), "salt": ("vegan", "herb_spice", [])}


class Client:
    def __init__(self, skip=()):
        self.skip, self.calls = skip, []

    def generate(self, model, contents, cfg, schema=None):
        assert contents.startswith(classify.PROMPT) and schema is classify.SCHEMA
        names = json.loads(contents[len(classify.PROMPT):])
        self.calls.append([n for _, n in names])
        out = [{"i": i, "d": INFO[n][0], "g": INFO[n][1], "a": INFO[n][2]} for i, n in names if n not in self.skip]
        return Reply(json.dumps(out), {}, "STOP")


@pytest.fixture
def db(tmp_path):
    d = DB(tmp_path / "recipes.db")
    yield d
    d.close()


@pytest.fixture
def cfg(tmp_path):
    return IngredientSettings(rpd=100, rpm=6000, in_flight=1, stagger=0, usage_log=tmp_path / "usage.jsonl")


def add(conn, names, pending=0):
    rid = conn.execute("INSERT INTO recipes(url, domain, name, ingredients, instructions, crawled_at) "
                       "VALUES (?, 'x', 'r', '[]', '[]', 0)", (f"https://x/{time.time_ns()}",)).lastrowid
    conn.executemany("INSERT INTO recipe_ingredients VALUES (?, ?, 0, ?, NULL, NULL)",
                     [(rid, pos, n) for pos, n in enumerate(names)])
    conn.execute("INSERT INTO recipe_ingredients_state VALUES (?, 0, ?)", (rid, pending))
    return rid


def run(db, cfg, client):
    c = classify.settings(cfg)
    clock = FakeClock()
    p = Processor(c, [Lane("k1", client, "m", 0, c.rpd)], UsageLog(c.usage_log), classify.make_save(db.conn),
                  clock=clock, sleep=clock.sleep, retry_sleep=lambda s: None, task=classify.TASK)
    return p.run(classify.todo(db.conn))


def test_parse_validates_values():
    result = [{"i": 0, "d": "vegan", "g": "grain", "a": ["gluten", "bogus", "gluten"]},
              {"i": 1, "d": "carnivore", "g": "meat", "a": []}, {"i": 2, "d": "vegan", "g": "rock", "a": []},
              {"i": 3, "d": "meat", "g": "meat", "a": None}, {"i": 7, "d": "vegan", "g": "grain", "a": []}]
    assert classify.parse(4, result) == {0: ("vegan", "grain", ["gluten"]), 3: ("meat", "meat", [])}


def test_schema_enums_match_the_constants():
    item = classify.SCHEMA["items"]["properties"]
    assert item["d"]["enum"] == list(classify.DIETS) and item["g"]["enum"] == list(classify.FOOD_GROUPS)
    assert item["a"]["items"]["enum"] == list(classify.ALLERGENS)


def test_classify_most_used_first_and_left_out_stay_to_do(db, cfg):
    add(db.conn, ["flour", "butter", "salt"])
    add(db.conn, ["flour", "bacon"])
    client = Client(skip={"bacon"})
    run(db, cfg, client)
    assert client.calls[0] == ["flour", "bacon", "butter", "salt"]  # flour: 2 recipes; then alphabetical
    assert [line.text for line in classify.todo(db.conn)] == ["bacon"]
    row = db.conn.execute("SELECT diet, food_group, allergens, model FROM ingredient_info WHERE name = 'butter'")
    assert tuple(row.fetchone()) == ("vegetarian", "dairy", '["milk"]', "m")


def test_recipe_diets_and_allergens(db, cfg):
    vegan = add(db.conn, ["flour", "salt"])
    veggie = add(db.conn, ["flour", "butter"])
    fish = add(db.conn, ["flour", "fish sauce"])
    meat = add(db.conn, ["bacon", "stock"])  # meat wins over unknown
    unknown = add(db.conn, ["butter", "stock"])
    unclassified = add(db.conn, ["flour", "mystery"])
    pending = add(db.conn, ["flour"], pending=1)  # a line not processed yet
    run(db, cfg, Client(skip={"mystery"}))
    assert classify.recipe_diets(db.conn) == {vegan: "vegan", veggie: "vegetarian", fish: "pescatarian",
                                              meat: "meat", unknown: "unknown", unclassified: "unknown",
                                              pending: "unknown"}
    allergens = classify.recipe_allergens(db.conn)
    assert allergens[veggie] == {"gluten", "milk"} and allergens[vegan] == {"gluten"}
    assert allergens[meat] == {"celery"}
    assert any("recipes by diet" in line for line in classify.report(db.conn))


def test_graph_gets_attributes_and_diet_filter(db, cfg):
    for _ in range(2):
        add(db.conn, ["flour", "salt"])
        add(db.conn, ["flour", "butter", "salt"])
        add(db.conn, ["flour", "bacon", "salt"])
    run(db, cfg, Client())
    g = graph.build_graph(db.conn, graph.GraphOptions(min_recipes=1, min_weight=1))
    assert g.nodes["butter"]["diet"] == "vegetarian" and g.nodes["flour"]["allergens"] == "gluten"
    assert g.nodes["salt"]["allergens"] == "" and g.nodes["bacon"]["food_group"] == "meat"
    g = graph.build_graph(db.conn, graph.GraphOptions(min_recipes=1, min_weight=1, diets=["vegan", "vegetarian"]))
    assert g.graph["recipes"] == 4 and "bacon" not in g and g.graph["diets"] == "vegan, vegetarian"


def test_migration_4_adds_the_table(tmp_path):
    path = tmp_path / "r.db"
    con = sqlite3.connect(path, isolation_level=None)
    for m in schema.MIGRATIONS[:3]:
        m.apply(con)
    con.execute("PRAGMA user_version = 3")
    con.execute("INSERT INTO urls (url, domain) VALUES ('https://x/1', 'x')")
    con.close()
    calls = []
    db = DB(path, backup=lambda: calls.append(1))
    assert schema.version(db.conn) == schema.CURRENT and calls == [] and schema.has_table(db.conn, "ingredient_info")
    db.close()
