"""Ingredient co-occurrence graph from recipe_ingredients, written as GraphML (read with nx.read_graphml).

Nodes are the (canonical) ingredient names:
- recipes: how many of the counted recipes use it, share: recipes / all counted recipes.
Edges join two ingredients used in the same recipe:
- weight: how many recipes use both,
- pmi: log(P(a, b) / (P(a) P(b))), > 0 = together more often than chance,
- npmi: pmi / -log P(a, b), in [-1, 1], comparable between frequent and rare pairs.
Once `classify` ran, nodes also get diet, food_group and allergens (comma-separated, "" = none).
Graph attributes: recipes (counted), the filters and thresholds used, created_at.

Only recipes with at least one mapped ingredient count. An ingredient is counted once per recipe, also when it
appears in several lines ("butter" for the dough and the filling).
"""

from __future__ import annotations

import logging
import math
import sqlite3
import time
from array import array
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import networkx as nx
import numpy as np

from . import classify

log = logging.getLogger("graph")

PAIRS_CHUNK = 20_000_000  # pair codes collected before they are reduced to (pair, count): keeps memory flat


@dataclass
class GraphOptions:
    min_recipes: int = 20  # ingredients used in fewer recipes are left out (with their edges)
    min_weight: int = 10  # pairs used together in fewer recipes get no edge
    regions: list[str] = field(default_factory=list)  # recipes.site_region; empty = all
    countries: list[str] = field(default_factory=list)  # recipes.site_country
    languages: list[str] = field(default_factory=list)  # recipes.language (prefix: "de" matches "de-CH")
    domains: list[str] = field(default_factory=list)  # recipes.domain
    diets: list[str] = field(default_factory=list)  # recipe diet (classify.recipe_diets), e.g. vegan
    exclude: list[str] = field(default_factory=list)  # ingredient names left out (e.g. "salt", "water")

    def filters(self) -> dict[str, str]:
        return {k: ", ".join(v) for k, v in (("regions", self.regions), ("countries", self.countries),
                                             ("languages", self.languages), ("domains", self.domains),
                                             ("diets", self.diets)) if v}


def _recipe_filter(opts: GraphOptions) -> tuple[str, list[str]]:
    clauses, params = [], []
    for column, values in (("site_region", opts.regions), ("site_country", opts.countries),
                           ("domain", opts.domains)):
        if values:
            clauses.append(f"{column} IN ({','.join('?' * len(values))})")
            params += values
    if opts.languages:
        clauses.append("(" + " OR ".join("lower(language) = ? OR lower(language) LIKE ?" for _ in opts.languages) + ")")
        for lang in opts.languages:
            params += [lang.lower(), lang.lower() + "-%"]
    return " AND ".join(clauses), params


def recipe_ingredient_sets(conn: sqlite3.Connection, opts: GraphOptions) -> tuple[list[str], list[array]]:
    """(names, one array of name indexes per recipe). Each ingredient once per recipe, excluded names dropped."""
    where, params = _recipe_filter(opts)
    wanted = None
    if where:  # a set of ids, so recipe_ingredients is still read in one pass in recipe_id order
        wanted = {id for (id,) in conn.execute(f"SELECT id FROM recipes WHERE {where}", params)}
    if opts.diets:
        by_diet = {id for id, diet in classify.recipe_diets(conn).items() if diet in opts.diets}
        wanted = by_diet if wanted is None else wanted & by_diet
    if wanted is not None:
        log.info("%s recipes match the filters", f"{len(wanted):,}")
    excluded = set(opts.exclude)
    index: dict[str, int] = {}
    names: list[str] = []
    recipes: list[array] = []
    current, seen = None, set()
    for recipe_id, name in conn.execute("SELECT recipe_id, name FROM recipe_ingredients ORDER BY recipe_id"):
        if recipe_id != current:
            if seen:
                recipes.append(array("i", sorted(seen)))
            current, seen = recipe_id, set()
        if (wanted is not None and recipe_id not in wanted) or name in excluded:
            continue
        i = index.get(name)
        if i is None:
            i = index[name] = len(names)
            names.append(name)
        seen.add(i)
    if seen:
        recipes.append(array("i", sorted(seen)))
    return names, recipes


def cooccurrence(recipes: list[array], keep: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(a, b, count) for every pair of kept ingredients (a < b, as indexes into the kept ones) in some recipe.
    keep maps a name index to its kept index, or -1."""
    n = int(keep.max()) + 1 if len(keep) else 0
    chunks, buffer, size = [], [], 0
    triu: dict[int, tuple[np.ndarray, np.ndarray]] = {}
    for ids in recipes:
        k = keep[np.frombuffer(ids, dtype=np.int32)] if len(ids) else np.empty(0, dtype=np.int64)
        k = k[k >= 0]
        if len(k) < 2:
            continue
        k.sort()
        if len(k) not in triu:
            triu[len(k)] = np.triu_indices(len(k), 1)
        i, j = triu[len(k)]
        buffer.append(k[i] * n + k[j])
        size += len(i)
        if size > PAIRS_CHUNK:
            chunks.append(_count(np.concatenate(buffer)))
            buffer, size = [], 0
    if buffer:
        chunks.append(_count(np.concatenate(buffer)))
    if not chunks:
        empty = np.empty(0, dtype=np.int64)
        return empty, empty, empty
    codes = np.concatenate([c for c, _ in chunks])
    counts = np.concatenate([c for _, c in chunks])
    order = np.argsort(codes, kind="stable")
    codes, counts = codes[order], counts[order]
    starts = np.flatnonzero(np.r_[True, codes[1:] != codes[:-1]])
    codes, counts = codes[starts], np.add.reduceat(counts, starts)
    return codes // n, codes % n, counts


def _count(codes: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    return np.unique(codes, return_counts=True)


def build_graph(conn: sqlite3.Connection, opts: GraphOptions) -> nx.Graph:
    t = time.time()
    names, recipes = recipe_ingredient_sets(conn, opts)
    total = len(recipes)
    uses = np.zeros(len(names), dtype=np.int64)
    for ids in recipes:
        uses[np.frombuffer(ids, dtype=np.int32)] += 1
    kept = np.flatnonzero(uses >= opts.min_recipes)
    keep = np.full(len(names), -1, dtype=np.int64)
    keep[kept] = np.arange(len(kept))
    log.info("%s recipes, %s ingredients, %s with >= %d recipes (%.0f s)", f"{total:,}", f"{len(names):,}",
             f"{len(kept):,}", opts.min_recipes, time.time() - t)
    a, b, w = cooccurrence(recipes, keep)
    del recipes
    strong = w >= opts.min_weight
    a, b, w = a[strong], b[strong], w[strong]

    g = nx.Graph(recipes=total, min_recipes=opts.min_recipes, min_weight=opts.min_weight,
                 exclude=", ".join(opts.exclude), created_at=time.strftime("%Y-%m-%d %H:%M:%S"), **opts.filters())
    known = classify.info(conn)
    for i in kept:
        attrs = {"recipes": int(uses[i]), "share": float(uses[i] / total)}
        if names[i] in known:
            diet, group, allergens = known[names[i]]
            attrs.update(diet=diet, food_group=group, allergens=",".join(allergens))
        g.add_node(names[i], **attrs)
    ka, kb = uses[kept[a]], uses[kept[b]]
    pmi = np.log(w * total / (ka * kb))
    joint = w / total
    npmi = np.where(joint < 1, pmi / -np.log(np.minimum(joint, 1 - 1e-12)), 1.0)
    g.add_edges_from((names[kept[x]], names[kept[y]], {"weight": int(c), "pmi": float(p), "npmi": float(q)})
                     for x, y, c, p, q in zip(a, b, w, pmi, npmi))
    log.info("graph: %s nodes, %s edges (pairs in >= %d recipes) in %.0f s", f"{g.number_of_nodes():,}",
             f"{g.number_of_edges():,}", opts.min_weight, time.time() - t)
    return g


def write_graph(g: nx.Graph, path: str | Path) -> None:
    t = time.time()
    nx.write_graphml(g, path)
    log.info("wrote %s (%.1f MB) in %.0f s", path, Path(path).stat().st_size / 1e6, time.time() - t)


def summary(g: nx.Graph, top: int = 10) -> list[str]:
    edges = sorted(g.edges(data=True), key=lambda e: -e[2]["weight"])
    strong = sorted((e for e in g.edges(data=True) if e[2]["weight"] >= 100), key=lambda e: -e[2]["npmi"])
    out = [f"{g.number_of_nodes():,} ingredients, {g.number_of_edges():,} edges, "
           f"from {g.graph['recipes']:,} recipes"]
    out.append("most common pairs: " + ", ".join(f"{a} + {b} ({d['weight']:,})" for a, b, d in edges[:top]))
    out.append("strongest pairs (npmi, >= 100 recipes): " + ", ".join(
        f"{a} + {b} ({d['npmi']:.2f})" for a, b, d in strong[:top]))
    return out
