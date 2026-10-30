"""Sitemap discovery: turn a site's sitemaps into a list of candidate recipe URLs."""

from __future__ import annotations

import asyncio
import gzip
import io
import re
import zlib
from collections import deque
from typing import AsyncIterator, Awaitable, Callable
from urllib.parse import urlsplit

from lxml import etree

from .config import Site
from .net import Response

# Child sitemaps that almost never contain recipes (only used when the site has no sitemap_pattern).
DEFAULT_SITEMAP_SKIP = re.compile(
    r"image|video|tag|author|categor|attachment|product|page-sitemap|news|event|store|shop|comment"
    r"|forum|user|profil|glossar|lexikon|magazin|article|artikel|blog-|guide|collection|search|brand"
    r"|410|deleted|pruning|ingredient|cuisine|occasion|course",
    re.IGNORECASE,
)
# "recipe" in the languages of the seed sites. If some child sitemaps of an index match this,
# only those are followed (e.g. recipe-sitemap.xml instead of post-sitemap.xml).
RECIPE_WORD = re.compile(
    r"recipe|recette|rezept|recet|ricett|receit|recept|przepis|tarif|resep|syntag|retsept|рецепт|συνταγ",
    re.IGNORECASE,
)
# Page URLs that are not recipes (only used when the site has no url_pattern).
DEFAULT_URL_SKIP = re.compile(
    r"/(tags?|authors?|categor(y|ies)|page|search|suche|users?|profil(e)?|login|cart|shop|videos?|feed"
    r"|forum|about|contact|privacy|impressum|datenschutz|agb|terms)(/|$)"
    r"|\.(jpe?g|png|gif|webp|svg|pdf|mp4|xml)$",
    re.IGNORECASE,
)

FALLBACK_SITEMAPS = ("/sitemap.xml", "/sitemap_index.xml", "/wp-sitemap.xml", "/sitemap-index.xml")

MAX_SITEMAPS_PER_SITE = 20_000

Fetch = Callable[[str], Awaitable[Response | None]]


def _localname(tag: object) -> str:
    return etree.QName(tag).localname.lower() if isinstance(tag, str) else ""


def _maybe_gunzip(body: bytes) -> bytes:
    if body[:2] != b"\x1f\x8b":
        return body
    # Bounded decompression to avoid gzip bombs.
    d = zlib.decompressobj(16 + zlib.MAX_WBITS)
    out = d.decompress(body, 60 * 1024 * 1024)
    if d.unconsumed_tail:
        raise gzip.BadGzipFile("decompressed sitemap larger than 60 MB")
    return out


def parse_sitemap(body: bytes) -> tuple[list[str], list[str]]:
    """Return (child_sitemaps, page_urls).

    Streams through the XML instead of building a tree, so a 50 MB sitemap needs little memory.
    CPU-heavy for big files: call it via asyncio.to_thread.
    """
    body = _maybe_gunzip(body).lstrip()
    if not body.startswith(b"<"):
        # Plain-text sitemap: one URL per line.
        lines = body.decode("utf-8", "replace").splitlines()
        return [], [ln.strip() for ln in lines if ln.strip().startswith("http")]
    locs: list[str] = []
    is_index = False
    try:
        for event, el in etree.iterparse(io.BytesIO(body), events=("start", "end"), recover=True,
                                         huge_tree=True, resolve_entities=False, no_network=True):
            name = _localname(el.tag)
            if event == "start":
                if name == "sitemapindex":
                    is_index = True
                continue
            if name == "loc":
                parent = el.getparent()
                if el.text and parent is not None and _localname(parent.tag) in ("url", "sitemap"):
                    locs.append(el.text.strip())
            elif name in ("url", "sitemap"):
                el.clear(keep_tail=False)
                # Drop already-processed siblings so memory stays flat.
                while el.getprevious() is not None:
                    del el.getparent()[0]
    except etree.XMLSyntaxError:
        pass
    return (locs, []) if is_index else ([], locs)


def _same_site(url: str, domain: str) -> bool:
    host = (urlsplit(url).hostname or "").lower()
    return host.removeprefix("www.") == domain.removeprefix("www.")


def select_sitemaps(site: Site, urls: list[str]) -> list[str]:
    """Pick which sitemaps (robots.txt entries or children of an index) to follow."""
    urls = [u.strip() for u in urls if u.strip().startswith("http")]
    if site.sitemap_pattern:
        return [u for u in urls if site.sitemap_pattern.search(u)]
    kept = [u for u in urls if not DEFAULT_SITEMAP_SKIP.search(urlsplit(u).path)]
    preferred = [u for u in kept if RECIPE_WORD.search(urlsplit(u).path)]
    return preferred or kept


def keep_page_url(site: Site, url: str) -> bool:
    if not _same_site(url, site.domain):
        return False
    path = urlsplit(url).path
    if path in ("", "/"):
        return False
    if site.url_pattern:
        return bool(site.url_pattern.search(url))
    return not DEFAULT_URL_SKIP.search(path)


async def iter_sitemaps(site: Site, roots: list[str], fetch: Fetch,
                        max_sitemaps: int = MAX_SITEMAPS_PER_SITE) -> AsyncIterator[tuple[str, list[str]]]:
    """Depth-first walk over sitemap indexes, yielding (sitemap_url, kept_page_urls) per urlset.

    Depth-first so that the first page URLs arrive after a few requests, not after reading every index.
    """
    queue = deque(roots)
    seen: set[str] = set()
    while queue and len(seen) < max_sitemaps:
        sm = queue.popleft()
        if sm in seen:
            continue
        seen.add(sm)
        resp = await fetch(sm)
        if resp is None or resp.status != 200:
            continue
        try:
            children, urls = await asyncio.to_thread(parse_sitemap, resp.body)
        except (OSError, zlib.error, gzip.BadGzipFile):
            continue
        queue.extendleft(reversed([c for c in select_sitemaps(site, children) if c not in seen]))
        if urls:
            yield sm, [u for u in urls if keep_page_url(site, u)]
        del resp


def keep_followed_link(site: Site, url: str) -> bool:
    """For follow_links sites: keep recipe pages (url_pattern) and listing pages (follow_pattern)."""
    if not _same_site(url, site.domain) or urlsplit(url).scheme not in ("http", "https"):
        return False
    if site.url_pattern and site.url_pattern.search(url):
        return True
    return bool(site.follow_pattern and site.follow_pattern.search(url))
