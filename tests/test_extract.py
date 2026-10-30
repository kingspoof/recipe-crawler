import json

from recipe_crawler.extract import extract_links, extract_recipes, parse_duration_minutes
from recipe_crawler.sitemaps import parse_sitemap


def page(*blocks, lang="en"):
    scripts = "".join(f'<script type="application/ld+json">{json.dumps(b)}</script>' for b in blocks)
    return f"<html lang='{lang}'><head>{scripts}</head><body></body></html>"


def test_simple_recipe():
    p = page({
        "@context": "https://schema.org", "@type": "Recipe", "name": "Pad Thai",
        "recipeIngredient": ["200 g rice noodles", "2 eggs"],
        "recipeInstructions": [{"@type": "HowToStep", "text": "Soak noodles."}, {"@type": "HowToStep", "text": "Fry."}],
        "recipeCuisine": "Thai", "totalTime": "PT1H5M", "recipeYield": ["2", "2 servings"],
        "image": {"@type": "ImageObject", "url": "https://x/img.jpg"},
        "author": {"@type": "Person", "name": "Pim"},
        "aggregateRating": {"ratingValue": "4,5", "ratingCount": "12"},
    }, lang="th")
    [r] = extract_recipes(p, "https://x/pad-thai")
    assert r.name == "Pad Thai"
    assert r.ingredients == ["200 g rice noodles", "2 eggs"]
    assert r.instructions == ["Soak noodles.", "Fry."]
    assert r.cuisine == ["Thai"]
    assert r.total_time_min == 65
    assert r.recipe_yield == "2 servings"
    assert r.image == "https://x/img.jpg"
    assert r.author == "Pim"
    assert r.rating_value == 4.5 and r.rating_count == 12
    assert r.language == "th"


def test_graph_type_list_sections_and_entities():
    p = page({"@context": "https://schema.org", "@graph": [
        {"@type": "WebPage", "name": "ignored"},
        {"@type": ["Recipe", "NewsArticle"], "name": "Crème brûlée &amp; co",
         "recipeIngredient": ["<b>4</b> egg yolks", "4 egg yolks"],
         "recipeInstructions": [{"@type": "HowToSection", "name": "Custard",
                                 "itemListElement": [{"@type": "HowToStep", "text": "Whisk."}]}]},
    ]})
    [r] = extract_recipes(p, "u")
    assert r.name == "Crème brûlée & co"
    assert r.ingredients == ["4 egg yolks"]  # tags stripped, de-duplicated
    assert r.instructions == ["Whisk."]


def test_instruction_string_is_split_into_steps():
    p = page({"@type": "Recipe", "name": "Chai", "recipeIngredient": ["tea"],
              "recipeInstructions": "Boil water.<br>Add tea.\nStrain."})
    [r] = extract_recipes(p, "u")
    assert r.instructions == ["Boil water.", "Add tea.", "Strain."]


def test_broken_json_and_non_recipes():
    p = ('<script type="application/ld+json">{"@type": "Recipe", "name": "X", "recipeIngredient": ["a",],}</script>'
         '<script type="application/ld+json">{"@type": "Article", "name": "Y"}</script>')
    assert [r.name for r in extract_recipes(p, "u")] == ["X"]
    assert extract_recipes(page({"@type": "Recipe", "name": "No ingredients"}), "u") == []


def test_microdata_fallback():
    p = """<div itemscope itemtype="http://schema.org/Recipe"><h1 itemprop="name">Borscht</h1>
           <li itemprop="recipeIngredient">beets</li><li itemprop="recipeIngredient">dill</li>
           <p itemprop="recipeInstructions">Cook it.</p><meta itemprop="totalTime" content="PT2H"></div>"""
    [r] = extract_recipes(p, "u")
    assert (r.name, r.ingredients, r.instructions, r.total_time_min) == ("Borscht", ["beets", "dill"], ["Cook it."], 120)


def test_durations():
    assert parse_duration_minutes("PT45M") == 45
    assert parse_duration_minutes("P1DT2H") == 1560
    assert parse_duration_minutes("PT0M") is None
    assert parse_duration_minutes("45 minutes") is None


def test_sitemaps():
    index = b'<?xml version="1.0"?><sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' \
            b'<sitemap><loc>https://x/recipe-sitemap.xml</loc></sitemap></sitemapindex>'
    assert parse_sitemap(index) == (["https://x/recipe-sitemap.xml"], [])
    urlset = b'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc> https://x/a </loc>' \
             b'<image:image xmlns:image="i"><image:loc>https://x/a.jpg</image:loc></image:image></url></urlset>'
    assert parse_sitemap(urlset) == ([], ["https://x/a"])


def test_links():
    html_ = '<a href="/r/1#top">a</a><a class=x href="https://o/r/2">b</a><a href="mailto:a@b">c</a>'
    assert extract_links(html_, "https://x/page") == ["https://x/r/1", "https://o/r/2"]


def test_microdata_ignores_nested_items():
    p = """<div itemscope itemtype="http://schema.org/Recipe">
           <ol itemscope itemtype="http://schema.org/BreadcrumbList"><li itemprop="itemListElement" itemscope
               itemtype="http://schema.org/ListItem"><span itemprop="name">Home</span></li></ol>
           <h1 itemprop="name">Kifle</h1><li itemprop="recipeIngredient">flour</li></div>"""
    [r] = extract_recipes(p, "u")
    assert r.name == "Kifle"
