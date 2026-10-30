"""Extract schema.org/Recipe data from HTML pages.

Almost every recipe site worldwide embeds a JSON-LD block of type Recipe
(Google requires it for recipe rich results), so one generic extractor covers
thousands of sites. A small microdata fallback handles older sites.
"""

from __future__ import annotations

import html
import json
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Iterator

_JSONLD_RE = re.compile(
    r"<script[^>]+type\s*=\s*[\"']?application/ld\+json[\"']?[^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
_HTML_LANG_RE = re.compile(r"<html[^>]*\slang\s*=\s*[\"']?([A-Za-z]{2,3}(?:[-_][A-Za-z0-9]+)?)", re.IGNORECASE)
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")
_DURATION_RE = re.compile(
    r"^P(?:(?P<d>\d+(?:\.\d+)?)D)?(?:T(?:(?P<h>\d+(?:\.\d+)?)H)?(?:(?P<m>\d+(?:\.\d+)?)M)?(?:(?P<s>\d+(?:\.\d+)?)S)?)?$",
    re.IGNORECASE,
)


@dataclass
class Recipe:
    url: str
    name: str
    ingredients: list[str]
    instructions: list[str]
    description: str | None = None
    language: str | None = None
    cuisine: list[str] = field(default_factory=list)
    category: list[str] = field(default_factory=list)
    keywords: list[str] = field(default_factory=list)
    prep_time_min: int | None = None
    cook_time_min: int | None = None
    total_time_min: int | None = None
    recipe_yield: str | None = None
    image: str | None = None
    author: str | None = None
    date_published: str | None = None
    rating_value: float | None = None
    rating_count: int | None = None
    nutrition: dict[str, str] | None = None
    raw: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- helpers


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    # Some sites double-escape entities (&amp;amp;), so unescape twice.
    value = html.unescape(html.unescape(value))
    value = _TAG_RE.sub(" ", value)
    return _WS_RE.sub(" ", value).strip()


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _text_list(value: Any, split_commas: bool = False) -> list[str]:
    out: list[str] = []
    for item in _as_list(value):
        if isinstance(item, dict):
            item = item.get("name") or item.get("text") or item.get("@value")
        text = clean_text(item)
        if not text:
            continue
        if split_commas:
            out.extend(p.strip() for p in text.split(",") if p.strip())
        else:
            out.append(text)
    # de-duplicate, keep order
    return list(dict.fromkeys(out))


def _is_recipe_type(t: Any) -> bool:
    for v in _as_list(t):
        if isinstance(v, str) and v.rsplit("/", 1)[-1].rsplit(":", 1)[-1].lower() == "recipe":
            return True
    return False


def _walk(node: Any) -> Iterator[dict[str, Any]]:
    """Yield every dict that is typed as Recipe anywhere in a JSON-LD tree."""
    if isinstance(node, list):
        for item in node:
            yield from _walk(item)
    elif isinstance(node, dict):
        if _is_recipe_type(node.get("@type")):
            yield node
        for key in ("@graph", "mainEntity", "mainEntityOfPage", "itemListElement", "item"):
            if key in node:
                yield from _walk(node[key])


def parse_duration_minutes(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    m = _DURATION_RE.match(value.strip())
    if not m or not any(m.groupdict().values()):
        return None
    d, h, mi, s = (float(m.group(k) or 0) for k in ("d", "h", "m", "s"))
    minutes = round(d * 1440 + h * 60 + mi + s / 60)
    # Values like PT0M or absurd ones (years) are noise.
    return minutes if 0 < minutes < 60 * 24 * 30 else None


def _instructions(value: Any) -> list[str]:
    steps: list[str] = []

    def visit(v: Any) -> None:
        if v is None:
            return
        if isinstance(v, list):
            for x in v:
                visit(x)
        elif isinstance(v, dict):
            if "itemListElement" in v:  # HowToSection
                visit(v["itemListElement"])
            else:  # HowToStep / HowToDirection
                visit(v.get("text") or v.get("name") or v.get("description"))
        else:
            raw = str(v)
            # A single string often contains several steps separated by newlines or <br>/<li>.
            parts = re.split(r"(?:\r?\n)+|<br\s*/?>|</li>|</p>", raw, flags=re.IGNORECASE)
            for p in parts:
                t = clean_text(p)
                if t:
                    steps.append(t)

    visit(value)
    return steps


def _first_url(value: Any) -> str | None:
    for item in _as_list(value):
        if isinstance(item, str) and item.strip():
            return item.strip()
        if isinstance(item, dict):
            u = item.get("url") or item.get("contentUrl") or item.get("@id")
            if isinstance(u, str) and u.strip():
                return u.strip()
    return None


def _author(value: Any) -> str | None:
    names = _text_list(value)
    return ", ".join(names) if names else None


def _float(v: Any) -> float | None:
    try:
        return float(str(v).replace(",", "."))
    except (TypeError, ValueError):
        return None


def _int(v: Any) -> int | None:
    f = _float(v)
    return int(f) if f is not None else None


def _yield(v: Any) -> str | None:
    vals = [clean_text(x) for x in _as_list(v) if clean_text(x)]
    if not vals:
        return None
    # Prefer the most descriptive value, e.g. ["4", "4 servings"] -> "4 servings"
    return max(vals, key=len)


def _from_jsonld(node: dict[str, Any], url: str) -> Recipe | None:
    name = clean_text(node.get("name") or node.get("headline"))
    ingredients = _text_list(node.get("recipeIngredient") or node.get("ingredients"))
    if not name or not ingredients:
        return None
    rating = node.get("aggregateRating") if isinstance(node.get("aggregateRating"), dict) else {}
    nutrition_raw = node.get("nutrition") if isinstance(node.get("nutrition"), dict) else None
    nutrition = (
        {k: clean_text(v) for k, v in nutrition_raw.items() if not k.startswith("@") and clean_text(v)}
        if nutrition_raw
        else None
    )
    lang = node.get("inLanguage")
    return Recipe(
        url=url,
        name=name,
        ingredients=ingredients,
        instructions=_instructions(node.get("recipeInstructions")),
        description=clean_text(node.get("description")) or None,
        language=clean_text(lang) if isinstance(lang, str) else None,
        cuisine=_text_list(node.get("recipeCuisine"), split_commas=True),
        category=_text_list(node.get("recipeCategory"), split_commas=True),
        keywords=_text_list(node.get("keywords"), split_commas=True),
        prep_time_min=parse_duration_minutes(node.get("prepTime")),
        cook_time_min=parse_duration_minutes(node.get("cookTime")),
        total_time_min=parse_duration_minutes(node.get("totalTime")),
        recipe_yield=_yield(node.get("recipeYield")),
        image=_first_url(node.get("image")),
        author=_author(node.get("author")),
        date_published=clean_text(node.get("datePublished")) or None,
        rating_value=_float(rating.get("ratingValue")),
        rating_count=_int(rating.get("ratingCount") or rating.get("reviewCount")),
        nutrition=nutrition or None,
        raw=node,
    )


def _load_json(text: str) -> Any:
    text = text.strip()
    if text.startswith("<!--"):
        text = text.removeprefix("<!--").removesuffix("-->").strip()
    if text.startswith("/*<![CDATA[*/"):
        text = text.removeprefix("/*<![CDATA[*/").removesuffix("/*]]>*/").strip()
    try:
        return json.loads(text, strict=False)
    except json.JSONDecodeError:
        # Common breakage: trailing commas.
        try:
            return json.loads(re.sub(r",\s*([}\]])", r"\1", text), strict=False)
        except json.JSONDecodeError:
            return None


# --------------------------------------------------------------------------- microdata fallback


def _from_microdata(page: str, url: str) -> list[Recipe]:
    from lxml import html as lxml_html

    try:
        doc = lxml_html.fromstring(page)
    except (ValueError, lxml_html.etree.ParserError):
        return []
    results = []
    for scope in doc.xpath('//*[@itemscope][contains(@itemtype, "schema.org/Recipe")]'):

        def props(name: str) -> list[str]:
            out = []
            for el in scope.xpath(f'.//*[@itemprop="{name}"]'):
                # Skip properties of nested items (e.g. a breadcrumb's "name" inside the recipe).
                owner = el.xpath("ancestor::*[@itemscope][1]")
                if owner and owner[0] is not scope:
                    continue
                v = el.get("content") or el.get("datetime") or el.get("src") or el.text_content()
                t = clean_text(v)
                if t:
                    out.append(t)
            return out

        names = props("name")
        ingredients = props("recipeIngredient") or props("ingredients")
        if not names or not ingredients:
            continue
        times = {k: (props(k) or [None])[0] for k in ("prepTime", "cookTime", "totalTime")}
        results.append(
            Recipe(
                url=url,
                name=names[0],
                ingredients=list(dict.fromkeys(ingredients)),
                instructions=props("recipeInstructions"),
                description=(props("description") or [None])[0],
                cuisine=props("recipeCuisine"),
                category=props("recipeCategory"),
                prep_time_min=parse_duration_minutes(times["prepTime"]),
                cook_time_min=parse_duration_minutes(times["cookTime"]),
                total_time_min=parse_duration_minutes(times["totalTime"]),
                recipe_yield=(props("recipeYield") or [None])[0],
                image=(props("image") or [None])[0],
                author=(props("author") or [None])[0],
            )
        )
    return results


# --------------------------------------------------------------------------- public API

_HREF_RE = re.compile(r"""<a\s[^>]*?href\s*=\s*["']([^"'#\s]+)""", re.IGNORECASE)


def extract_links(page: str, base_url: str) -> list[str]:
    """Absolute, fragment-free link targets found in <a href> tags."""
    from urllib.parse import urljoin

    out = []
    for href in _HREF_RE.findall(page):
        href = html.unescape(href)
        if href.startswith(("mailto:", "javascript:", "tel:", "data:")):
            continue
        out.append(urljoin(base_url, href))
    return list(dict.fromkeys(out))



def extract_recipes(page: str, url: str) -> list[Recipe]:
    """Return all recipes found on the page (usually 0 or 1)."""
    found: list[Recipe] = []
    seen: set[str] = set()
    for block in _JSONLD_RE.findall(page):
        data = _load_json(block)
        if data is None:
            continue
        for node in _walk(data):
            recipe = _from_jsonld(node, url)
            if recipe and recipe.name not in seen:
                seen.add(recipe.name)
                found.append(recipe)
    if not found and re.search(r"schema\.org/Recipe", page, re.IGNORECASE):
        found = _from_microdata(page, url)
    if found:
        m = _HTML_LANG_RE.search(page)
        html_lang = m.group(1) if m else None
        for r in found:
            r.language = r.language or html_lang
    return found
