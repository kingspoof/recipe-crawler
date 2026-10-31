"""Gemini side of the ingredient processing: prompt, client, usage log / quota, lanes and the request scheduler.

Ported from ingredient_processing.ipynb (cells 2, 4 and 6). The scheduler never touches the database itself:
it gets the to-do lines and a save callback, so it can be tested with a fake client and an in-memory queue.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import threading
import time
from collections import Counter, deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Protocol
from zoneinfo import ZoneInfo

import httpx

from .config import IngredientSettings

log = logging.getLogger("gemini")

QUOTA_TZ = ZoneInfo("America/Los_Angeles")  # daily quotas reset at midnight Pacific time
RETRY_WAIT = 15.0  # 5xx / network: 15, 30, 60, 120, 240 s
MAX_RETRIES = 5
# A batch whose request failed this often with 5xx / network errors (each time after MAX_RETRIES retries) is
# charged one attempt per line, like lines the model left out. Otherwise one batch that always gets a 500 goes
# back to the front of the queue forever and uses up the whole day's quota.
MAX_TRANSIENT = 2
# What a network problem looks like from google-genai (it uses httpx). Anything else is a bug and is raised.
NETWORK_ERRORS = (httpx.HTTPError, OSError, TimeoutError)

UNITS = ["g", "kg", "mg", "ml", "cl", "dl", "l", "tsp", "tbsp", "cup", "oz", "lb", "fl oz", "pinch", "dash", "piece",
         "clove", "slice", "sheet", "can", "jar", "package", "bunch", "sprig", "stalk", "leaf", "handful", "drop"]

# Validated with gemini_quality_test.ipynb (~0.1-0.2% untranslated). Only change: the t / T line.
PROMPT = f"""You turn recipe ingredient lines into structured data.

The input is a JSON array of [index, line]. The lines can be in any language.
For every line return one or more objects {{"i": index, "en": name, "a": amount, "u": unit}}:
- en: the ingredient name TRANSLATED INTO ENGLISH, lowercase, singular. Always English, also when the line is a
  single word: never copy a word from the input unless it is also the English word ("avocado", "curry").
  Only the ingredient: no quantity, unit, brand, size or notes.
  Drop preparation and state words (fresh, frozen, dried, ground, chopped, minced, sliced, grated, peeled, ...),
  keep words that make it a different ingredient ("brown sugar", "egg white", "soy sauce", "coconut milk").
- a: the amount as a number. Fractions as decimals ("1½" -> 1.5), ranges as the first number ("2-3" -> 2), null if none.
- u: the unit in English, singular. Use one of {", ".join(UNITS)} if it fits,
  otherwise a short English word. null if there is none; plain counts ("3 eggs") have no unit.
- Never use "spoon". A big, soup or table spoon is "tbsp" ("el", "ek", "msk", "spl", "ss", "c.s.", "ст. л.", "łyżka",
  "lingura", "colher de sopa", "sudu besar", "כף", "大さじ"). A small or tea spoon is "tsp" ("tl", "tsk", "c.c.",
  "ч. л.", "łyżeczka", "colher de chá", "sudu kecil", "小さじ").
- In English recipes a lowercase "t" is "tsp" and an uppercase "T" is "tbsp".
- "gr" -> g. Counting words ("stk", "ks", "db", "szt", "шт", "adet", "biji", "個") -> piece. Tins ("lata", "dose") -> can.
- Alternatives ("butter or margarine"): only the first one.
- Several ingredients in one line ("salt and pepper"): one object each, with the same i.
- Not an ingredient (headings like "for the sauce:", equipment, tips): {{"i": index, "en": null, "a": null, "u": null}}.
Every index must appear in the output.

Example input:  [[0, "2 cups chopped onions"], [1, "salz und pfeffer"], [2, "für die soße:"], [3, "1 el sojasauce"],
                 [4, "3 eier"], [5, "1 cebolla"], [6, "czosnek"], [7, "1 kg брашно"], [8, "水 2大匙"], [9, "1 ks vejce"],
                 [10, "2 łyżki oleju"]]
Example output: [{{"i":0,"en":"onion","a":2,"u":"cup"}},{{"i":1,"en":"salt","a":null,"u":null}},{{"i":1,"en":"pepper","a":null,"u":null}},{{"i":2,"en":null,"a":null,"u":null}},{{"i":3,"en":"soy sauce","a":1,"u":"tbsp"}},{{"i":4,"en":"egg","a":3,"u":null}},{{"i":5,"en":"onion","a":1,"u":"piece"}},{{"i":6,"en":"garlic","a":null,"u":null}},{{"i":7,"en":"flour","a":1,"u":"kg"}},{{"i":8,"en":"water","a":2,"u":"tbsp"}},{{"i":9,"en":"egg","a":1,"u":"piece"}},{{"i":10,"en":"oil","a":2,"u":"tbsp"}}]

Input:
"""

RESPONSE_SCHEMA = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "i": {"type": "integer"},
            "en": {"type": ["string", "null"]},
            "a": {"type": ["number", "null"]},
            "u": {"type": ["string", "null"]},
        },
        "required": ["i", "en", "a", "u"],
    },
}


# --------------------------------------------------------------------------- client


class ApiError(Exception):
    """An HTTP error answer from the API (503 overloaded, 429 quota, 400, ...)."""

    def __init__(self, code: int | None, message: str = "", details: Any = None):
        super().__init__(f"{code} {message}")
        self.code, self.message, self.details = code, message, details


@dataclass
class Reply:
    text: str | None
    usage: dict[str, int] = field(default_factory=dict)  # prompt_tokens, thoughts_tokens, output_tokens, ...
    finish: str | None = None


class Client(Protocol):
    def generate(self, model: str, contents: str, cfg: IngredientSettings) -> Reply: ...


class GeminiClient:
    """The real client: google-genai, translated to Reply / ApiError. Anything else raised = network error."""

    def __init__(self, api_key: str):
        from google import genai

        self._client = genai.Client(api_key=api_key)

    def generate(self, model: str, contents: str, cfg: IngredientSettings) -> Reply:
        from google.genai import errors, types

        config = types.GenerateContentConfig(
            temperature=0,
            max_output_tokens=cfg.max_output_tokens,
            response_mime_type="application/json",
            response_json_schema=RESPONSE_SCHEMA,
            thinking_config=types.ThinkingConfig(thinking_level=cfg.thinking_level) if cfg.thinking_level else None,
        )
        try:
            response = self._client.models.generate_content(model=model, contents=contents, config=config)
        except errors.APIError as e:
            raise ApiError(e.code, e.message or "", e.details) from e
        usage = response.usage_metadata
        finish = response.candidates[0].finish_reason if response.candidates else None
        return Reply(
            text=response.text,
            usage={
                "prompt_tokens": (usage and usage.prompt_token_count) or 0,
                "thoughts_tokens": (usage and usage.thoughts_token_count) or 0,
                "output_tokens": (usage and usage.candidates_token_count) or 0,
                "cached_tokens": (usage and usage.cached_content_token_count) or 0,
                "total_tokens": (usage and usage.total_token_count) or 0,
            },
            finish=getattr(finish, "name", finish),
        )


# --------------------------------------------------------------------------- keys and quota bookkeeping


def fingerprint(key: str) -> str:
    """How a key appears in the usage log and the output: never the key itself."""
    return hashlib.sha256(key.encode()).hexdigest()[:8]


def load_keys(path: Path) -> list[str]:
    """One key per line, '#' comments, duplicates dropped (same order as the notebook, for the old log records)."""
    keys = [line.strip() for line in Path(path).read_text().splitlines()]
    return list(dict.fromkeys(k for k in keys if k and not k.startswith("#")))


def quota_day(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts or time.time(), QUOTA_TZ).date().isoformat()


def next_reset() -> datetime:
    """Next quota reset (midnight Pacific), in local time."""
    now = datetime.now(QUOTA_TZ)
    return datetime.combine(now.date() + timedelta(days=1), datetime.min.time(), QUOTA_TZ).astimezone()


class UsageLog:
    """One JSON line per request (also every retry), because the API does not report how much quota is left."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._lock = threading.Lock()
        self.exhausted: set[tuple[str, str]] = set()  # filled by used_today()

    def write(self, rec: dict) -> None:
        with self._lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.path, "a") as f:
                f.write(json.dumps(rec) + "\n")

    def used_today(self, keys: list[str], day: str | None = None) -> Counter[tuple[str, str]]:
        """Requests per (key fingerprint, model) today. Every attempt counts, also 503s (AI Studio, 2026-10-29).

        Old records have "key": line index in the keys file; they are counted for the key at that index now.
        Indexes beyond the file stay "#<index>". Also sets self.exhausted: the (key, model) pairs that got a
        429 PerDay today, so a new run does not spend a request to find out again.
        """
        day = day or quota_day()
        fps = [fingerprint(k) for k in keys]
        used: Counter[tuple[str, str]] = Counter()
        exhausted: set[tuple[str, str]] = set()
        try:
            with open(self.path) as f:
                for line in f:
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:  # half-written line after a hard abort
                        continue
                    if rec.get("day") != day:
                        continue
                    key = rec.get("key")
                    if isinstance(key, int):
                        key = fps[key] if 0 <= key < len(fps) else f"#{key}"
                    used[key, rec.get("model")] += 1
                    if "PerDay" in rec.get("details", ""):  # Google says the day is used up, whatever we counted
                        exhausted.add((key, rec.get("model")))
        except FileNotFoundError:
            pass
        self.exhausted = exhausted
        return used


# --------------------------------------------------------------------------- one request


def transient(rec: dict) -> bool:
    """Failed for reasons that have nothing to do with the lines: overloaded, quota, network."""
    return rec.get("error", "").startswith(("5", "429", "network"))


def call(client: Client, key: str, model: str, lines: list[str], cfg: IngredientSettings, usage: UsageLog,
         sleep: Callable[[float], Any] = time.sleep, retry_wait: float = RETRY_WAIT,
         max_retries: int = MAX_RETRIES) -> tuple[Any, dict]:
    """Returns (parsed JSON or None, record). Runs in a worker thread, never touches the database."""
    payload = json.dumps([[i, line] for i, line in enumerate(lines)], ensure_ascii=False)
    rec: dict[str, Any] = {"day": quota_day(), "key": key, "model": model, "thinking": cfg.thinking_level,
                           "lines": len(lines)}
    for attempt in range(max_retries + 1):
        t = time.time()
        try:
            reply = client.generate(model, PROMPT + payload, cfg)
            break
        except ApiError as e:
            rec.update(ts=time.time(), latency=time.time() - t, retries=attempt, error=f"{e.code} {e.message}"[:300])
            if e.code == 429:  # says which limit was hit (per minute / per day, requests / tokens)
                rec["details"] = json.dumps(e.details, ensure_ascii=False)[:2000]
            retry = e.code is not None and e.code >= 500
        except NETWORK_ERRORS as e:  # connection reset, timeout, ...
            rec.update(ts=time.time(), latency=time.time() - t, retries=attempt, error=f"network {e!r}"[:300])
            retry = True
        usage.write(rec)
        if not retry or attempt == max_retries:
            return None, rec
        sleep(retry_wait * 2 ** attempt)
    rec.pop("error", None)
    rec.pop("details", None)
    rec.update(ts=time.time(), latency=time.time() - t, retries=attempt, **reply.usage, finish=reply.finish)
    try:
        result = json.loads(reply.text)  # type: ignore[arg-type]
    except (json.JSONDecodeError, TypeError) as e:  # e.g. cut off at MAX_TOKENS
        rec["error"] = f"bad json: {e}"[:300]
        result = None
        if reply.finish == "MAX_TOKENS" and reply.text:
            result = salvage(reply.text)  # keep the objects that were complete
            rec["truncated"] = len(result)
    usage.write(rec)
    return result, rec


def salvage(text: str) -> list:
    """The complete objects at the start of a JSON array that was cut off."""
    decoder = json.JSONDecoder()
    out: list = []
    pos = text.find("[") + 1
    if not pos:
        return out
    while True:
        while pos < len(text) and text[pos] in " \t\r\n,":
            pos += 1
        try:
            obj, pos = decoder.raw_decode(text, pos)
        except json.JSONDecodeError:
            return out
        out.append(obj)


def parse(n_lines: int, result: Any) -> dict[int, list]:
    """{line index: [[name, amount, unit], ...]} for every line the model answered."""
    out: dict[int, list] = {}
    for item in result if isinstance(result, list) else []:
        i = item.get("i") if isinstance(item, dict) else None
        if isinstance(i, int) and not isinstance(i, bool) and 0 <= i < n_lines:
            out.setdefault(i, []).append([item.get("en"), item.get("a"), item.get("u")])
    return out


# --------------------------------------------------------------------------- lanes and the scheduler


class Lane:
    """One key x model with its own daily quota, rate limit and pauses."""

    def __init__(self, key: str, client: Client, model: str, used: int, rpd: int):
        self.key, self.client, self.model = fingerprint(key), client, model
        self.name = f"{self.key}/{model}"
        self.rpd = rpd
        self.day, self.used = quota_day(), used
        self.in_flight = 0
        self.next_at = 0.0  # rate limit
        self.paused_until = 0.0  # after a 429
        self.n_429 = 0
        self.exhausted = False  # today's quota is used up (by our count or by a 429 PerDay)

    def check_quota(self) -> None:
        if quota_day() != self.day:  # new quota day while running
            self.day, self.used, self.exhausted = quota_day(), 0, False
        if self.rpd and self.used >= self.rpd and not self.exhausted:
            self.exhausted = True
            log.info("%s: %d requests used today, done until the reset", self.name, self.rpd)

    def ready(self, now: float, in_flight: int) -> bool:
        self.check_quota()
        return not self.exhausted and self.in_flight < in_flight and now >= max(self.next_at, self.paused_until)

    def got_429(self, now: float, details: str = "") -> None:
        if "PerDay" in details:  # the daily quota of this project is used up
            if not self.exhausted:
                log.warning("%s: 429 per-day quota exceeded, done until the reset", self.name)
            self.exhausted = True
            return
        if now < self.paused_until:  # several requests were in flight: count one 429 per pause, not each of them
            return
        self.n_429 += 1
        # short-term throttling (no quotaId, or per minute): 2, 4, 8, 16, 30, 30 ... min
        self.paused_until = now + min(60 * 2 ** self.n_429, 1800)
        log.warning("%s: 429 quota exceeded (#%d), pausing %.0f min", self.name, self.n_429,
                    (self.paused_until - now) / 60)


@dataclass
class Line:
    id: int
    text: str  # what is sent: the original spelling if there is one, else the lowercase key
    attempts: int = 0  # times the model left it out so far
    transient: int = 0  # requests with this line that failed with 5xx / network since the last charge


# save(answered [(line id, items)], failed [line id], model)
SaveFn = Callable[[list[tuple[int, list]], list[int], str], None]
SAVE_TRIES = 3


def spool_write(path: Path, answered: list[tuple[int, list]], failed: list[int], model: str) -> None:
    """Answers that could not be saved to the database (locked, disk full, ...): applied on the next run."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a") as f:
        f.write(json.dumps({"answered": answered, "failed": failed, "model": model}, ensure_ascii=False) + "\n")
        f.flush()


def spool_read(path: Path) -> list[tuple[list[tuple[int, list]], list[int], str]]:
    out = []
    try:
        with open(path) as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:  # half-written line after a hard abort
                    continue
                out.append(([(id, items) for id, items in rec["answered"]], rec["failed"], rec["model"]))
    except FileNotFoundError:
        pass
    return out


class Processor:
    """Sends the to-do lines on every lane until they are done or every lane has used up today's quota."""

    def __init__(self, cfg: IngredientSettings, lanes: list[Lane], usage: UsageLog, save: SaveFn,
                 max_requests: int | None = None, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], Any] | None = None, retry_sleep: Callable[[float], Any] = time.sleep,
                 retry_wait: float = RETRY_WAIT, spool: Path | None = None):
        self.cfg, self.lanes, self.usage, self._save_fn = cfg, lanes, usage, save
        self.spool = spool  # where answers go when the database cannot take them
        self.max_requests = max_requests
        self.clock = clock
        self._stop = threading.Event()
        self.sleep = sleep or self._stop.wait  # an idle sleep ends right away on stop()
        self.retry_sleep, self.retry_wait = retry_sleep, retry_wait
        self.sent = self.done = self.failed = 0
        self.tokens: Counter[str] = Counter()
        self.error: BaseException | None = None  # a bug in a worker thread: raised after the requests in flight

    def save(self, answered: list[tuple[int, list]], failed: list[int], model: str) -> None:
        """Saves with retries. If the database still fails, the answers go to the spool file and sending stops,
        so the requests in flight (paid with quota) are not lost."""
        for attempt in range(SAVE_TRIES):
            try:
                self._save_fn(answered, failed, model)
                return
            except Exception as e:  # sqlite3.OperationalError (locked, disk full, ...)
                log.warning("saving %d answers failed (%s), try %d/%d", len(answered), e, attempt + 1, SAVE_TRIES)
                if attempt + 1 < SAVE_TRIES:
                    self.retry_sleep(2 ** attempt)
                    continue
                if self.spool is None:
                    raise
                spool_write(self.spool, answered, failed, model)
                if not self.stopping:
                    log.error("database not writable: answers go to %s (applied by the next `process`); "
                              "no new requests", self.spool)
                self.stop()

    def stop(self) -> None:
        """Stop sending; requests in flight are still waited for and saved."""
        self._stop.set()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def run(self, todo_lines: list[Line]) -> str:
        """Returns why it ended: "done", "quota", "max_requests" or "stopped"."""
        cfg = self.cfg
        todo, retry = deque(todo_lines), deque[Line]()  # retry: left out by the model, or the request failed
        total = len(todo)
        start = self.clock()
        for i, lane in enumerate(self.lanes):  # not every key at the same moment
            lane.next_at = start + i * cfg.stagger
        log.info("%s lines to do, %d lanes, used today: %s", f"{total:,}", len(self.lanes),
                 {lane.name: lane.used for lane in self.lanes})
        started = time.time()
        futures: dict[Future, tuple[Lane, list[Line]]] = {}

        def take_batch() -> list[Line]:
            batch = []
            while len(batch) < cfg.batch_size and (retry or todo):
                batch.append((retry or todo).popleft())
            return batch

        def may_send() -> bool:
            return not self.stopping and bool(todo or retry) and (
                self.max_requests is None or self.sent < self.max_requests)

        with ThreadPoolExecutor(max(1, len(self.lanes) * cfg.in_flight)) as pool:
            while True:
                now = self.clock()
                for lane in self.lanes:
                    while may_send() and lane.ready(now, cfg.in_flight):
                        batch = take_batch()
                        lane.in_flight += 1
                        lane.used += 1
                        self.sent += 1
                        lane.next_at = now + 1.05 * 60 / cfg.rpm
                        futures[pool.submit(call, lane.client, lane.key, lane.model, [b.text for b in batch], cfg,
                                            self.usage, self.retry_sleep, self.retry_wait)] = (lane, batch)
                for lane in self.lanes:
                    lane.check_quota()
                if not futures:
                    if self.stopping:
                        reason = "stopped"
                    elif not (todo or retry):
                        reason = "done"
                    elif all(lane.exhausted for lane in self.lanes):
                        reason = "quota"
                        log.info("every lane has used up today's quota; next reset %s", f"{next_reset():%a %H:%M}")
                    elif self.max_requests is not None and self.sent >= self.max_requests:
                        reason = "max_requests"
                    else:
                        reason = ""
                    if reason:
                        break
                # Until the next lane may send again (rate limit or 429 pause), but at most 30 s.
                # Lanes at the in-flight limit wake up through a finished request, not through the clock.
                # Nothing more to send (end of the run, stop): only the requests in flight, no busy polling.
                wake = min((max(lane.next_at, lane.paused_until) for lane in self.lanes
                            if not lane.exhausted and lane.in_flight < cfg.in_flight), default=now + 30) - now
                if not may_send():
                    wake = 2.0
                if not futures:  # nothing in flight: every lane is waiting
                    if wake > 60:
                        log.info("all lanes paused, sleeping %.0f min", wake / 60)
                    self.sleep(min(max(wake, 0.01), 600))
                    continue
                finished, _ = wait(futures, timeout=min(max(wake, 0.01), 2), return_when=FIRST_COMPLETED)
                for future in finished:
                    lane, batch = futures.pop(future)
                    lane.in_flight -= 1
                    try:
                        self._finished(lane, batch, future.result(), retry, started, total, len(todo))
                    except Exception as e:  # a bug: stop sending, but wait for and save what is in flight
                        log.exception("%s: request failed with an unexpected error, stopping", lane.name)
                        retry.extendleft(reversed(batch))
                        self.error = self.error or e
                        self.stop()
        log.info("finished (%s): %s done, %d failed, %d requests, tokens %s", reason, f"{self.done:,}",
                 self.failed, self.sent, dict(self.tokens))
        if self.error is not None:
            raise self.error
        return reason

    def _finished(self, lane: Lane, batch: list[Line], outcome: tuple[Any, dict], retry: deque[Line],
                  started: float, total: int, todo_left: int) -> None:
        result, rec = outcome
        error = rec.get("error", "")
        lane.used += rec.get("retries", 0)  # every retry inside call() was a request too
        if error.startswith("429"):
            lane.got_429(self.clock(), rec.get("details", ""))
        elif not error:
            lane.n_429 = 0
        if result is None and re.match(r"4\d\d ", error) and not error.startswith("429"):
            # bad key (400/403), unknown model (404), ...: not the lines' fault, and retrying will not help
            retry.extendleft(reversed(batch))
            if not lane.exhausted:
                log.error("%s: %s, lane disabled for this run", lane.name, error[:200])
            lane.exhausted = True
            return
        if result is None and transient(rec):
            if not error.startswith("429"):
                for line in batch:
                    line.transient += 1
            charged = [line for line in batch if line.transient >= MAX_TRANSIENT]
            if not charged:
                retry.extendleft(reversed(batch))  # not the lines' fault: same attempt count
                quota_ids = sorted(set(re.findall(r'"quotaId": "([^"]+)"', rec.get("details", ""))))
                log.warning("%s: %s, batch goes back in the queue %s", lane.name, error[:100], ", ".join(quota_ids))
                return
            failed = []
            for line in batch:
                line.transient = 0
                line.attempts += 1
                if line.attempts >= self.cfg.max_attempts:
                    failed.append(line.id)
                else:
                    retry.append(line)  # to the back, so it is sent in a different batch next time
            log.warning("%s: %s, %d failed requests in a row for this batch: charged one attempt (%d lines failed)",
                        lane.name, error[:100], MAX_TRANSIENT, len(failed))
            if failed:
                self.save([], failed, lane.model)
                self.failed += len(failed)
            return
        parsed = parse(len(batch), result)
        # Cut off at MAX_TOKENS: the lines after the cut were never reached, so they are not charged. The line
        # at the cut is (it may be the one that makes the answer explode), and so are gaps before it.
        cut = max(parsed, default=-1) + 1 if "truncated" in rec else len(batch)
        answered, failed = [], []
        for i, line in enumerate(batch):
            if i in parsed:
                answered.append((line.id, parsed[i]))
            elif i > cut:
                retry.append(line)
            elif line.attempts + 1 >= self.cfg.max_attempts:
                failed.append(line.id)
            else:
                line.attempts += 1
                retry.append(line)
        self.save(answered, failed, lane.model)
        self.done += len(answered)
        self.failed += len(failed)
        for k in ("prompt_tokens", "thoughts_tokens", "output_tokens"):
            self.tokens[k] += rec.get(k, 0)
        rate = self.done / max(time.time() - started, 1e-9)
        log.info("%s: %d/%d in %.0f s %s| done %s/%s (%.1f%%), failed %d, left %s | today %s | %s lines/h",
                 lane.name, len(answered), len(batch), rec.get("latency", 0), error[:60] + " " if error else "",
                 f"{self.done:,}", f"{total:,}", 100 * self.done / max(total, 1), self.failed,
                 f"{todo_left + len(retry):,}", [la.used for la in self.lanes], f"{rate * 3600:,.0f}")
