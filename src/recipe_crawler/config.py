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
class IngredientSettings:
    """Gemini ingredient processing ([ingredients] in config.toml), defaults from ingredient_processing.ipynb."""

    models: list[str] = field(default_factory=lambda: ["gemini-3.5-flash-lite"])  # one lane per key x model
    thinking_level: str = "minimal"
    batch_size: int = 650  # lines per request: ~18.5K output tokens
    max_output_tokens: int = 32768
    max_line_chars: int = 200  # longer lines are almost always prose: skipped (status -2)
    max_attempts: int = 2  # a line the model leaves out this often is marked failed (status -1)
    rpd: int = 500  # requests per day per key x model (free tier); 0 = no own limit
    rpm: int = 14  # our own pace per lane (the free tier allows 15)
    in_flight: int = 200  # requests at the same time per lane
    stagger: float = 5.0  # seconds between the first request of each lane
    usage_log: Path = Path("data.nosync/gemini_usage.jsonl")
    keys_file: Path = Path("gemini_keys.txt")  # one key per line, one key per Google Cloud project


@dataclass
class BackupSettings:
    """[backup] in config.toml. dir is required for `backup` (no default, so nothing lands in a synced folder)."""

    # Cannot be rebuilt cheaply: the Gemini answers (quota), canonical names, discovery state.
    DAILY_TABLES = ("ingredient_lines", "ingredient_names", "domains")

    dir: Path | None = None
    daily_keep: int = 14
    full_keep: int = 2  # the newest full backups, always kept
    weekly_keep: int = 4  # plus the newest full backup of each of the last N calendar weeks
    full_every_days: float = 7


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
    # Read a site's sitemaps again after this many days to find new recipes (0 = never, only `discover --force`).
    rediscover_after_days: float = 30
    sites: list[Site] = field(default_factory=list)
    ingredients: IngredientSettings = field(default_factory=IngredientSettings)
    backup: BackupSettings = field(default_factory=BackupSettings)

    def site(self, domain: str) -> Site:
        for s in self.sites:
            if s.domain == domain:
                return s
        raise KeyError(f"Unknown domain {domain!r} (not in config)")


def _regex(value: str | None) -> re.Pattern[str] | None:
    return re.compile(value, re.IGNORECASE) if value else None


def _ingredients(data: dict, base: Path) -> IngredientSettings:
    d = IngredientSettings()
    return IngredientSettings(
        models=list(data.get("models", d.models)),
        thinking_level=data.get("thinking_level", d.thinking_level),
        batch_size=int(data.get("batch_size", d.batch_size)),
        max_output_tokens=int(data.get("max_output_tokens", d.max_output_tokens)),
        max_line_chars=int(data.get("max_line_chars", d.max_line_chars)),
        max_attempts=int(data.get("max_attempts", d.max_attempts)),
        rpd=int(data.get("rpd", d.rpd)),
        rpm=int(data.get("rpm", d.rpm)),
        in_flight=int(data.get("in_flight", d.in_flight)),
        stagger=float(data.get("stagger", d.stagger)),
        usage_log=base / data.get("usage_log", d.usage_log),
        keys_file=base / data.get("keys_file", d.keys_file),
    )


def _backup(data: dict, base: Path) -> BackupSettings:
    d = BackupSettings()
    raw = str(data.get("dir", "")).strip()
    return BackupSettings(
        dir=base / Path(raw).expanduser() if raw else None,  # an absolute dir stays as it is
        daily_keep=int(data.get("daily_keep", d.daily_keep)),
        full_keep=int(data.get("full_keep", d.full_keep)),
        weekly_keep=int(data.get("weekly_keep", d.weekly_keep)),
        full_every_days=float(data.get("full_every_days", d.full_every_days)),
    )


def load_config(path: str | Path = "config.toml", require_contact: bool = True) -> Settings:
    """require_contact: only commands that send requests to recipe sites (crawl, discover, probe) need one."""
    path = Path(path)
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    c = data.get("crawler", {})
    contact = c.get("contact", "").strip()
    if require_contact and (not contact or "CHANGE-ME" in contact or "example" in contact):
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
        rediscover_after_days=float(c.get("rediscover_after_days", 30)),
        sites=sites,
        ingredients=_ingredients(data.get("ingredients", {}), path.parent),
        backup=_backup(data.get("backup", {}), path.parent),
    )
