import json
import math
import time

import networkx as nx
import pytest

from recipe_crawler import graph, schema
from recipe_crawler.db import DB


@pytest.fixture
def conn(tmp_path):
    db = DB(tmp_path / "recipes.db")
    yield db.conn
    db.close()


def add(conn, names, region="Europe", country="DE", language="de", domain="x.de"):
    rid = conn.execute(
        "INSERT INTO recipes(url, domain, site_country, site_region, language, name, ingredients, instructions, "
        "crawled_at) VALUES (?, ?, ?, ?, ?, 'r', '[]', '[]', 0)",
        (f"https://{domain}/{time.time_ns()}", domain, country, region, language)).lastrowid
    conn.executemany("INSERT INTO recipe_ingredients VALUES (?, ?, 0, ?, NULL, NULL)",
                     [(rid, pos, name) for pos, name in enumerate(names)])
    return rid


def test_counts_pmi_and_thresholds(conn):
    for _ in range(3):
        add(conn, ["flour", "egg", "butter", "butter"])  # butter twice: counted once per recipe
    add(conn, ["flour", "sugar"])
    add(conn, ["salt"])
    g = graph.build_graph(conn, graph.GraphOptions(min_recipes=2, min_weight=1))
    assert g.graph["recipes"] == 5
    assert set(g.nodes) == {"flour", "egg", "butter"}  # sugar, salt: 1 recipe each
    assert g.nodes["flour"] == {"recipes": 4, "share": 0.8}
    edge = g.edges["flour", "egg"]
    assert edge["weight"] == 3
    assert edge["pmi"] == pytest.approx(math.log(3 * 5 / (4 * 3)))
    assert edge["npmi"] == pytest.approx(math.log(3 * 5 / (4 * 3)) / -math.log(3 / 5))
    assert g.number_of_edges() == 3
    g = graph.build_graph(conn, graph.GraphOptions(min_recipes=2, min_weight=4))
    assert g.number_of_edges() == 0 and g.number_of_nodes() == 3


def test_filters_and_exclude(conn):
    add(conn, ["rice", "soy sauce"], region="East Asia", country="JP", language="ja", domain="a.jp")
    add(conn, ["rice", "soy sauce", "salt"], region="East Asia", country="JP", language="ja", domain="a.jp")
    add(conn, ["flour", "salt"], language="de-CH")
    opts = graph.GraphOptions(min_recipes=1, min_weight=1, regions=["East Asia"], exclude=["salt"])
    g = graph.build_graph(conn, opts)
    assert g.graph["recipes"] == 2 and set(g.nodes) == {"rice", "soy sauce"}
    assert g.edges["rice", "soy sauce"]["weight"] == 2
    assert g.graph["regions"] == "East Asia" and g.graph["exclude"] == "salt"
    g = graph.build_graph(conn, graph.GraphOptions(min_recipes=1, min_weight=1, languages=["de"]))
    assert set(g.nodes) == {"flour", "salt"}  # "de" matches "de-CH"


def test_graphml_round_trip(conn, tmp_path):
    add(conn, ["flour", "egg"])
    add(conn, ["flour", "egg", "milk"])
    g = graph.build_graph(conn, graph.GraphOptions(min_recipes=1, min_weight=1))
    graph.write_graph(g, tmp_path / "g.graphml")
    h = nx.read_graphml(tmp_path / "g.graphml")
    assert h.edges["flour", "egg"]["weight"] == 2 and h.nodes["milk"]["recipes"] == 1
    assert h.graph["recipes"] == 2


def test_many_pairs_are_merged_across_chunks(conn, monkeypatch):
    for _ in range(4):
        add(conn, ["a", "b", "c"])
    names, recipes = graph.recipe_ingredient_sets(conn, graph.GraphOptions())
    import numpy as np
    keep = np.arange(len(names))
    calls = []
    monkeypatch.setattr(graph, "PAIRS_CHUNK", 2)
    monkeypatch.setattr(graph, "_count", lambda codes: calls.append(1) or np.unique(codes, return_counts=True))
    a, b, w = graph.cooccurrence(recipes * 2, keep)
    assert len(calls) == 8  # 3 pairs per recipe > 2: one chunk per recipe, merged at the end
    assert sorted(w.tolist()) == [8, 8, 8]


def test_empty_database(conn):
    g = graph.build_graph(conn, graph.GraphOptions())
    assert g.number_of_nodes() == 0 and g.graph["recipes"] == 0
