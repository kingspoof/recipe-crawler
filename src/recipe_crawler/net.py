"""HTTP fetching, robots.txt handling and per-domain politeness."""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime

import httpx
from protego import Protego

MAX_PAGE_BYTES = 8 * 1024 * 1024
MAX_SITEMAP_BYTES = 60 * 1024 * 1024  # spec limit is 50 MB uncompressed

_META_CHARSET_RE = re.compile(rb"""<meta[^>]+charset\s*=\s*["']?([A-Za-z0-9_\-]+)""", re.IGNORECASE)


class TooLarge(Exception):
    pass


@dataclass
class Response:
    url: str
    status: int
    headers: httpx.Headers
    body: bytes
    elapsed: float

    @property
    def content_type(self) -> str:
        return self.headers.get("content-type", "").lower()

    def text(self) -> str:
        charset = None
        m = re.search(r"charset=([A-Za-z0-9_\-]+)", self.content_type)
        if m:
            charset = m.group(1)
        else:
            m2 = _META_CHARSET_RE.search(self.body[:4096])
            if m2:
                charset = m2.group(1).decode("ascii", "ignore")
        try:
            return self.body.decode(charset or "utf-8", errors="replace")
        except LookupError:
            return self.body.decode("utf-8", errors="replace")

    def retry_after(self) -> float | None:
        value = self.headers.get("retry-after")
        if not value:
            return None
        try:
            return float(value)
        except ValueError:
            try:
                return max(0.0, parsedate_to_datetime(value).timestamp() - time.time())
            except (TypeError, ValueError):
                return None


class Fetcher:
    def __init__(self, user_agent: str, timeout: float):
        self.user_agent = user_agent
        # httpx's timeout applies to each read, so a server that trickles bytes could hold a worker forever.
        # This limits the whole request; sitemaps (up to 60 MB) get longer.
        self.total_timeout = 2 * timeout
        # Token used to match robots.txt "User-agent:" groups, e.g. "RecipeResearchCrawler".
        self.robots_token = user_agent.split("/")[0].split(" ")[0]
        self.client = httpx.AsyncClient(
            headers={
                "User-Agent": user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=httpx.Timeout(timeout, connect=15.0),
            follow_redirects=False,  # DomainWorker follows them itself, checking robots.txt for every hop
            limits=httpx.Limits(max_connections=200, max_keepalive_connections=100),
        )

    async def get(self, url: str, max_bytes: int = MAX_PAGE_BYTES, follow_redirects: bool = False) -> Response:
        start = time.monotonic()
        total = self.total_timeout * (5 if max_bytes > MAX_PAGE_BYTES else 1)
        try:
            async with asyncio.timeout(total):
                return await self._get(url, max_bytes, follow_redirects, start)
        except TimeoutError:
            raise httpx.ReadTimeout(f"no complete response after {total:.0f} s") from None

    async def _get(self, url: str, max_bytes: int, follow_redirects: bool, start: float) -> Response:
        async with self.client.stream("GET", url, follow_redirects=follow_redirects) as r:
            declared = r.headers.get("content-length")
            if declared and declared.isdigit() and int(declared) > max_bytes:
                raise TooLarge(f"{declared} bytes")
            chunks, size = [], 0
            async for chunk in r.aiter_bytes():
                size += len(chunk)
                if size > max_bytes:
                    raise TooLarge(f">{max_bytes} bytes")
                chunks.append(chunk)
            return Response(str(r.url), r.status_code, r.headers, b"".join(chunks), time.monotonic() - start)

    async def aclose(self) -> None:
        await self.client.aclose()


class Throttle:
    """Guarantees at least `delay` seconds between the end of one request and the start of the next."""

    MAX_DELAY = 120.0

    def __init__(self, delay: float):
        self.base_delay = delay
        self.delay = delay
        self._next = 0.0

    async def wait(self, stop: asyncio.Event | None = None) -> None:
        """Sleeps until the next request may start; returns early when `stop` is set."""
        now = time.monotonic()
        if now >= self._next:
            return
        if stop is None:
            await asyncio.sleep(self._next - now)
            return
        try:
            await asyncio.wait_for(stop.wait(), self._next - now)
        except TimeoutError:
            pass

    def done(self, elapsed: float = 0.0) -> None:
        # If the server is slow, slow down too.
        self._next = time.monotonic() + max(self.delay, elapsed)

    def backoff(self, retry_after: float | None = None) -> None:
        self.delay = min(self.delay * 2, self.MAX_DELAY)
        self._next = time.monotonic() + max(self.delay, min(retry_after or 0, 3600))

    def relax(self) -> None:
        """After a success, slowly return toward the base delay."""
        if self.delay > self.base_delay:
            self.delay = max(self.base_delay, self.delay * 0.9)


class Robots:
    def __init__(self, parser: Protego | None, token: str, allow_all: bool = False):
        self.parser = parser
        self.token = token
        self.allow_all = allow_all

    @property
    def unreachable(self) -> bool:
        return self.parser is None and not self.allow_all

    def allowed(self, url: str) -> bool:
        if self.allow_all:
            return True
        if self.parser is None:
            return False
        return self.parser.can_fetch(url, self.token)

    @property
    def crawl_delay(self) -> float | None:
        if self.parser is None:
            return None
        d = self.parser.crawl_delay(self.token)
        return float(d) if d is not None else None

    @property
    def sitemaps(self) -> list[str]:
        return list(self.parser.sitemaps) if self.parser else []


async def fetch_robots(fetcher: Fetcher, base_url: str) -> tuple[Robots, str | None]:
    """Fetch robots.txt following RFC 9309: 4xx = allow all, 5xx/network error = disallow all."""
    try:
        # RFC 9309: follow at least five redirects for robots.txt itself
        r = await fetcher.get(f"{base_url}/robots.txt", max_bytes=2 * 1024 * 1024, follow_redirects=True)
    except (httpx.HTTPError, TooLarge) as e:
        return Robots(None, fetcher.robots_token), f"robots.txt unreachable: {type(e).__name__}: {e}"
    if r.status >= 500 or r.status == 429:
        return Robots(None, fetcher.robots_token), f"robots.txt returned HTTP {r.status}"
    if r.status in (401, 403):
        # RFC 9309 would allow crawling here, but in practice this means bots are blocked.
        return Robots(None, fetcher.robots_token), f"robots.txt returned HTTP {r.status} (bot blocking)"
    if r.status >= 400:
        return Robots(None, fetcher.robots_token, allow_all=True), None
    text = r.text()
    if "<html" in text[:1000].lower():  # some sites serve an HTML page instead of robots.txt
        return Robots(None, fetcher.robots_token, allow_all=True), None
    return Robots(Protego.parse(text), fetcher.robots_token), None
