"""Load crawler settings and the list of seed sites from config.toml."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Site:
    domain: str
    country: str  # ISO code of the cuisine the site mainly covers
    language: str
    region: str = ""  # broader region, e.g. "South Asia", for balancing the dataset
    # Explicit sitemap URLs. If empty, they are discovered from robots.txt.
    sitemaps: list[str] = field(default_factory=list)
    # Only follow child sitemaps whose URL matches this regex (e.g. "recipe").
    sitemap_pattern: re.Pattern[str] | None = None
    # Only queue page URLs matching this regex (e.g. "/recipes?/").
    url_pattern: re.Pattern[str] | None = None
    # Seconds between requests to this domain (overrides the default).
    delay: float | None = None
    # Stop after saving this many recipes from this site (overrides the default).
    max_recipes: int | None = None
    # For sites without recipe sitemaps: follow links found on crawled pages.
    # Links matching url_pattern are queued as recipe pages, links matching
    # follow_pattern are queued as listing pages (categories, pagination, ...).
    follow_links: bool = False
    follow_pattern: re.Pattern[str] | None = None
    start_urls: list[str] = field(default_factory=list)
    enabled: bool = True

    @property
    def base_url(self) -> str:
        return f"https://{self.domain}"


@dataclass
class Settings:
    user_agent: str
    db_path: Path
    default_delay: float = 2.0
    max_parallel_domains: int = 40
    max_recipes_per_site: int = 60_000
    # Stop all sites of a region once it has this many recipes (0 = no limit), so one region can't dominate.
    max_recipes_per_region: int = 0
    target_total: int = 1_000_000
    timeout: float = 30.0
    max_attempts: int = 3
    sites: list[Site] = field(default_factory=list)

    def site(self, domain: str) -> Site:
        for s in self.sites:
            if s.domain == domain:
                return s
        raise KeyError(f"Unknown domain {domain!r} (not in config)")


def _regex(value: str | None) -> re.Pattern[str] | None:
    return re.compile(value, re.IGNORECASE) if value else None


def load_config(path: str | Path = "config.toml") -> Settings:
    path = Path(path)
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    c = data.get("crawler", {})
    contact = c.get("contact", "").strip()
    if not contact or "CHANGE-ME" in contact or "example" in contact:
        raise SystemExit(
            f"Please set crawler.contact in {path} to an email or URL so site owners can reach you."
        )
    ua = c.get("user_agent", "RecipeResearchCrawler/0.1").strip()
    sites = []
    seen = set()
    for s in data.get("seeds", {}).get("site", []):
        domain = s["domain"].strip().lower()
        if domain in seen:
            raise SystemExit(f"Duplicate site in config: {domain}")
        seen.add(domain)
        sites.append(
            Site(
                domain=domain,
                country=s.get("country", "").upper(),
                language=s.get("language", "").lower(),
                region=s.get("region", ""),
                sitemaps=list(s.get("sitemaps", [])),
                sitemap_pattern=_regex(s.get("sitemap_pattern")),
                url_pattern=_regex(s.get("url_pattern")),
                delay=s.get("delay"),
                max_recipes=s.get("max_recipes"),
                follow_links=s.get("follow_links", False),
                follow_pattern=_regex(s.get("follow_pattern")),
                start_urls=list(s.get("start_urls", [])),
                enabled=s.get("enabled", True),
            )
        )
        if sites[-1].follow_links and not sites[-1].url_pattern:
            raise SystemExit(f"{domain}: follow_links requires a url_pattern for recipe pages")
    return Settings(
        user_agent=f"{ua} (+{contact})",
        db_path=path.parent / c.get("db_path", "recipes.db"),
        default_delay=float(c.get("default_delay", 2.0)),
        max_parallel_domains=int(c.get("max_parallel_domains", 40)),
        max_recipes_per_site=int(c.get("max_recipes_per_site", 60_000)),
        max_recipes_per_region=int(c.get("max_recipes_per_region", 0)),
        target_total=int(c.get("target_total", 1_000_000)),
        timeout=float(c.get("timeout", 30.0)),
        max_attempts=int(c.get("max_attempts", 3)),
        sites=sites,
    )
