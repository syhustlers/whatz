"""
WhatsApp Group Link Scraper & Validator - Telegram bot for Render.
Includes Multi-Key ScraperAPI Rotation & Failover Engine with Pacing.
"""

import asyncio
import html
import io
import logging
import os
import random
import re
import sys
import threading
import time
from collections import Counter
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import unquote, urlparse

import httpx
from bs4 import BeautifulSoup
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LinkPreviewOptions,
    Update,
)
from telegram.error import BadRequest, NetworkError, RetryAfter, TelegramError, TimedOut
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# --------------------------------------------------------------------------- #
# Configuration (New Token & Hardcoded ScraperAPI Pool)
# --------------------------------------------------------------------------- #

BOT_TOKEN = "8742741857:AAEIh6zBnzPt2ezSWrU8tztk85VLGPlwtbQ"

def load_scraper_keys() -> list[str]:
    return [
        "7d566c800ca55799d6ec085d8e7c4ff2",
        "710e8884bfe8116a3618b67640fc09a8",
        "4b0a93603cff3838c0830d32b47005a2",
        "233b5663191b1ee77e5fbcb920d468d8",
        "0e8e3d38b7281b90a0bff5d43c5fe639",
        "688792c4fe1c11dcedea4fdf6ca0f6a2",
        "6e8d49f457f68036dd39dd2a1350805a",
    ]

HEALTH_PORT = int(os.environ.get("PORT", 10000))

BTN_SCRAPE = "btn_scrape"
BTN_VALIDATE_LIST = "btn_validate_list"

STATE_AWAITING_WEBSITES = "awaiting_websites"
STATE_AWAITING_LINKS = "awaiting_links"

STATUS_VALID = "valid"
STATUS_DEAD = "dead"
STATUS_APPROVAL = "approval"
STATUS_UNKNOWN = "unknown"

MAX_TEXT_FILE_BYTES = 2 * 1024 * 1024
MAX_WEBSITES = 100
MAX_LINKS = 2000

SCRAPE_CONCURRENCY = 5
VALIDATE_CONCURRENCY = 3

REQUEST_TIMEOUT_SECONDS = 5.0
REQUEST_DEADLINE_SECONDS = 8.0
SITE_DEADLINE_SECONDS = 20.0
LINK_DEADLINE_SECONDS = 75.0
MAX_VALIDATION_ATTEMPTS = 2
MAX_PAGE_BYTES = 3 * 1024 * 1024
MAX_INVITE_PAGE_BYTES = 1024 * 1024

RATE_LIMIT_STATUSES = {403, 429, 503}
DEAD_STATUSES = {404, 410}

GATEWAY_ENDPOINT = "https://api.scraperapi.com/"
GATEWAY_TIMEOUT = httpx.Timeout(15.0, connect=10.0)
GATEWAY_DEADLINE_SECONDS = 20.0
GATEWAY_TOTAL_DEADLINE_SECONDS = 60.0
GATEWAY_MAX_ATTEMPTS = 5
MAX_KEY_WAIT_SECONDS = 10.0
KEY_RATE_LIMIT_COOLDOWN_SECONDS = 2.0
KEY_ERROR_COOLDOWN_SECONDS = 1.0
KEY_SOFT_FAILURE_COOLDOWN_SECONDS = 0.5
KEY_EXHAUSTED_COOLDOWN_SECONDS = 300.0
KEY_INVALID_COOLDOWN_SECONDS = 3600.0
PACING_MIN_SECONDS = 0.2
PACING_MAX_SECONDS = 0.5

PROGRESS_INTERVAL_SECONDS = 2.5
TELEGRAM_CHUNK_LIMIT = 3800
OUTPUT_FILENAME = "valid_whatsapp_groups.txt"

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Upgrade-Insecure-Requests": "1",
}

WHATSAPP_CODE_RE = re.compile(
    r"chat\.whatsapp\.com/(?:invite/)?([A-Za-z0-9]{22})", re.IGNORECASE
)
URL_RE = re.compile(r"https?://[^\s<>\"'\]\)]+", re.IGNORECASE)
DOMAIN_RE = re.compile(r"^[\w.-]+\.[A-Za-z]{2,}(?:[:/?#]\S*)?$")

GENERIC_TITLES = {
    "whatsapp",
    "whatsapp group invite",
    "whatsapp group",
    "whatsapp web",
    "whatsapp messenger",
}

DEAD_MARKERS = (
    "invite link is invalid",
    "link is invalid",
    "invite link was reset",
    "link was reset",
    "link has been reset",
    "has been reset",
    "been revoked",
    "was revoked",
    "link is revoked",
    "link has expired",
    "link is expired",
    "invite link expired",
    "no longer valid",
    "no longer active",
    "link is no longer",
)

APPROVAL_MARKERS = (
    "request to join",
    "requests to join",
    "admin must approve",
    "admins must approve",
    "admin approval",
    "admins approval",
    "approval required",
    "requires approval",
    "require approval",
    "approve new members",
    "approve before",
    "join request",
    "needs admin approval",
)

NO_PREVIEW = LinkPreviewOptions(is_disabled=True)

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger("wa-bot")


# --------------------------------------------------------------------------- #
# Render health check server
# --------------------------------------------------------------------------- #

class HealthHandler(BaseHTTPRequestHandler):
    """Answers every GET/HEAD request with HTTP 200."""

    def _respond(self, include_body: bool) -> None:
        body = b"OK"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        if include_body:
            try:
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError):
                pass

    def do_GET(self) -> None:  # noqa: N802
        self._respond(True)

    def do_HEAD(self) -> None:  # noqa: N802
        self._respond(False)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        return


def start_health_server() -> None:
    """Starts the health check HTTP server in a background daemon thread."""
    server = ThreadingHTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler)
    server.daemon_threads = True
    thread = threading.Thread(
        target=server.serve_forever, name="health-server", daemon=True
    )
    thread.start()
    logger.info("Health check server listening on port %s", HEALTH_PORT)


# --------------------------------------------------------------------------- #
# Telegram helpers
# --------------------------------------------------------------------------- #

def main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("🌐 Scrape & Validate Websites", callback_data=BTN_SCRAPE)],
            [InlineKeyboardButton("📁 Validate Link List / File", callback_data=BTN_VALIDATE_LIST)],
        ]
    )


def _retry_seconds(exc: RetryAfter) -> float:
    value = exc.retry_after
    if isinstance(value, timedelta):
        return value.total_seconds()
    return float(value)


async def tg_call(factory: Callable[[], Awaitable[Any]], attempts: int = 3) -> Any:
    """Runs a Telegram API call with flood-wait and network error handling."""
    last_exc: Optional[Exception] = None
    for attempt in range(attempts):
        try:
            return await factory()
        except RetryAfter as exc:
            last_exc = exc
            await asyncio.sleep(min(_retry_seconds(exc), 30.0) + 1.0)
        except BadRequest:
            raise
        except (TimedOut, NetworkError) as exc:
            last_exc = exc
            await asyncio.sleep(1.5 * (attempt + 1))
    if last_exc is not None:
        raise last_exc
    return None


async def safe_send(bot: Any, chat_id: int, text: str, **kwargs: Any) -> Any:
    return await tg_call(
        lambda: bot.send_message(
            chat_id=chat_id,
            text=text,
            link_preview_options=NO_PREVIEW,
            **kwargs,
        )
    )


async def safe_send_document(
    bot: Any, chat_id: int, payload: bytes, filename: str, caption: str
) -> Any:
    return await tg_call(
        lambda: bot.send_document(
            chat_id=chat_id,
            document=io.BytesIO(payload),
            filename=filename,
            caption=caption,
        )
    )


class ProgressReporter:
    """Throttled live progress updates through message editing."""

    def __init__(self, bot: Any, chat_id: int, message_id: Optional[int]) -> None:
        self.bot = bot
        self.chat_id = chat_id
        self.message_id = message_id
        self._last_text = ""
        self._last_edit = 0.0

    async def update(self, text: str, force: bool = False) -> None:
        if self.message_id is None or text == self._last_text:
            return
        now = time.monotonic()
        if not force and now - self._last_edit < PROGRESS_INTERVAL_SECONDS:
            return
        self._last_edit = now
        self._last_text = text
        try:
            await self.bot.edit_message_text(
                chat_id=self.chat_id, message_id=self.message_id, text=text
            )
        except Exception as exc:
            logger.debug("Progress update skipped: %s", exc)


# --------------------------------------------------------------------------- #
# Parsing helpers
# --------------------------------------------------------------------------- #

def decode_bytes(data: bytes) -> str:
    if data.startswith((b"\xff\xfe", b"\xfe\xff")):
        try:
            return data.decode("utf-16")
        except UnicodeDecodeError:
            pass
    try:
        return data.decode("utf-8-sig")
    except UnicodeDecodeError:
        return data.decode("latin-1", errors="replace")


def extract_whatsapp_codes(text: str) -> list[str]:
    """Returns unique WhatsApp invite codes in order of first appearance."""
    if not text:
        return []
    cleaned = html.unescape(text).replace("\\/", "/")
    haystack = cleaned + "\n" + unquote(cleaned)
    seen: dict[str, None] = {}
    for match in WHATSAPP_CODE_RE.finditer(haystack):
        seen.setdefault(match.group(1), None)
    return list(seen)


def code_to_link(code: str) -> str:
    return f"https://chat.whatsapp.com/{code}"


def parse_website_urls(text: str) -> list[str]:
    """Parses one-URL-per-line input into a deduplicated list of http(s) URLs."""
    urls: dict[str, None] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip().strip(",;")
        if not line or line.startswith("#"):
            continue
        candidates = URL_RE.findall(line)
        if not candidates and DOMAIN_RE.match(line):
            candidates = [line]
        for candidate in candidates:
            candidate = candidate.rstrip(".,;:!?")
            if not re.match(r"^https?://", candidate, re.IGNORECASE):
                candidate = "https://" + candidate
            parsed = urlparse(candidate)
            host = (parsed.hostname or "").lower()
            if not host or "." not in host:
                continue
            if host == "chat.whatsapp.com":
                continue
            urls.setdefault(candidate, None)
    return list(urls)


# --------------------------------------------------------------------------- #
# HTTP engine
# --------------------------------------------------------------------------- #

class Cooldown:
    """Shared back-off gate so all workers slow down after a rate limit."""

    def __init__(self) -> None:
        self.until = 0.0

    def trigger(self, seconds: float) -> None:
        self.until = max(self.until, time.monotonic() + seconds)

    async def wait(self) -> None:
        delay = self.until - time.monotonic()
        if delay > 0:
            await asyncio.sleep(min(delay, 8.0))


def build_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        headers=BROWSER_HEADERS,
        timeout=httpx.Timeout(REQUEST_TIMEOUT_SECONDS),
        follow_redirects=True,
        max_redirects=5,
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
    )


async def http_get(
    client: httpx.AsyncClient,
    url: str,
    max_bytes: int,
    params: Optional[dict[str, str]] = None,
    timeout: Optional[httpx.Timeout] = None,
) -> tuple[int, str, httpx.Headers]:
    """Streams a GET request, capping the body size, and returns decoded text."""
    request_kwargs: dict[str, Any] = {}
    if params is not None:
        request_kwargs["params"] = params
    if timeout is not None:
        request_kwargs["timeout"] = timeout
    async with client.stream("GET", url, **request_kwargs) as response:
        chunks: list[bytes] = []
        total = 0
        async for chunk in response.aiter_bytes():
            chunks.append(chunk)
            total += len(chunk)
            if total >= max_bytes:
                break
        status = response.status_code
        headers = response.headers
        encoding = response.encoding or "utf-8"
    raw = b"".join(chunks)[:max_bytes]
    try:
        text = raw.decode(encoding, errors="replace")
    except LookupError:
        text = raw.decode("utf-8", errors="replace")
    return status, text, headers


def retry_delay(headers: httpx.Headers, attempt: int) -> float:
    retry_after = (headers.get("retry-after") or "").strip()
    if retry_after.isdigit():
        return float(min(int(retry_after), 6))
    return min(1.5 * (attempt + 1) + random.random(), 6.0)


# --------------------------------------------------------------------------- #
# ScraperAPI multi-key gateway
# --------------------------------------------------------------------------- #

class GatewayUnavailable(Exception):
    """Raised when no ScraperAPI key could complete the request."""


class APIKeyManager:
    """Round-robin ScraperAPI key pool with fast automatic rotation and failover."""

    def __init__(self, keys: list[str]) -> None:
        unique: list[str] = []
        for key in keys:
            key = key.strip()
            if key and key not in unique:
                unique.append(key)
        self._keys = unique
        count = len(unique)
        self._lock = asyncio.Lock()
        self._cursor = 0
        self._blocked_until = [0.0] * count
        self._unusable = [False] * count
        self._requests = [0] * count
        self._failures = [0] * count

    @property
    def enabled(self) -> bool:
        return bool(self._keys)

    @property
    def size(self) -> int:
        return len(self._keys)

    def label(self, index: int) -> str:
        return f"#{index + 1}(...{self._keys[index][-4:]})"

    async def acquire(self) -> Optional[tuple[int, str]]:
        async with self._lock:
            now = time.monotonic()
            count = len(self._keys)
            for offset in range(count):
                index = (self._cursor + offset) % count
                if self._blocked_until[index] <= now:
                    self._cursor = (index + 1) % count
                    self._requests[index] += 1
                    self._unusable[index] = False
                    return index, self._keys[index]
            return None

    async def shortest_wait(self) -> Optional[float]:
        async with self._lock:
            if not self._keys:
                return None
            now = time.monotonic()
            return max(0.0, min(until - now for until in self._blocked_until))

    async def _block(self, index: int, seconds: float, unusable: bool = False) -> None:
        async with self._lock:
            self._failures[index] += 1
            self._blocked_until[index] = max(
                self._blocked_until[index], time.monotonic() + seconds
            )
            if unusable:
                self._unusable[index] = True

    async def report_rate_limited(self, index: int) -> None:
        logger.info("ScraperAPI key %s rate-limited, rotating instantly", self.label(index))
        await self._block(index, KEY_RATE_LIMIT_COOLDOWN_SECONDS)

    async def report_error(self, index: int) -> None:
        await self._block(index, KEY_ERROR_COOLDOWN_SECONDS)

    async def report_soft_failure(self, index: int) -> None:
        await self._block(index, KEY_SOFT_FAILURE_COOLDOWN_SECONDS)

    async def report_exhausted(self, index: int) -> None:
        logger.warning("ScraperAPI key %s exhausted credits, rotating", self.label(index))
        await self._block(index, KEY_EXHAUSTED_COOLDOWN_SECONDS, unusable=True)

    async def report_invalid(self, index: int) -> None:
        logger.warning("ScraperAPI key %s rejected as invalid", self.label(index))
        await self._block(index, KEY_INVALID_COOLDOWN_SECONDS, unusable=True)

    def health_note(self) -> str:
        if not self._keys:
            return "ℹ️ No ScraperAPI keys loaded."
        now = time.monotonic()
        out = sum(
            1
            for index in range(len(self._keys))
            if self._unusable[index] and self._blocked_until[index] > now
        )
        if out:
            return f"⚠️ {out} of {len(self._keys)} ScraperAPI keys are out of credits or invalid."
        return ""

    def usage_summary(self) -> str:
        return ", ".join(
            f"{self.label(i)}: {self._requests[i]} req/{self._failures[i]} fail"
            for i in range(len(self._keys))
        )


KEY_MANAGER = APIKeyManager(load_scraper_keys())


async def gateway_get(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    url: str,
    max_bytes: int,
) -> tuple[int, str, httpx.Headers]:
    for _attempt in range(GATEWAY_MAX_ATTEMPTS):
        lease = await KEY_MANAGER.acquire()
        if lease is None:
            wait = await KEY_MANAGER.shortest_wait()
            if wait is None or wait > MAX_KEY_WAIT_SECONDS:
                raise GatewayUnavailable("no ScraperAPI key is currently available")
            await asyncio.sleep(wait + 0.05)
            continue

        index, key = lease
        try:
            async with sem:
                status, text, headers = await asyncio.wait_for(
                    http_get(
                        client,
                        GATEWAY_ENDPOINT,
                        max_bytes,
                        params={"api_key": key, "url": url},
                        timeout=GATEWAY_TIMEOUT,
                    ),
                    GATEWAY_DEADLINE_SECONDS,
                )
                await asyncio.sleep(random.uniform(PACING_MIN_SECONDS, PACING_MAX_SECONDS))
        except Exception as exc:
            logger.info(
                "Gateway request error via key %s: %s",
                KEY_MANAGER.label(index),
                exc.__class__.__name__,
            )
            await KEY_MANAGER.report_error(index)
            await asyncio.sleep(0.2)
            continue

        if status == 429:
            await KEY_MANAGER.report_rate_limited(index)
            continue
        if status == 401:
            await KEY_MANAGER.report_invalid(index)
            continue
        if status == 403:
            await KEY_MANAGER.report_exhausted(index)
            continue
        if status >= 500:
            await KEY_MANAGER.report_soft_failure(index)
            await asyncio.sleep(0.3)
            continue
        return status, text, headers

    raise GatewayUnavailable("ScraperAPI attempts exhausted")


# --------------------------------------------------------------------------- #
# Website scraping engine
# --------------------------------------------------------------------------- #

async def _direct_scrape_once(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    cooldown: Cooldown,
    url: str,
) -> tuple[Optional[list[str]], bool]:
    await cooldown.wait()
    try:
        async with sem:
            status, text, headers = await asyncio.wait_for(
                http_get(client, url, MAX_PAGE_BYTES), SITE_DEADLINE_SECONDS
            )
            await asyncio.sleep(random.uniform(0.1, 0.3))
    except Exception as exc:
        logger.info("Direct scrape failed for %s: %s", url, exc.__class__.__name__)
        return None, True
    if status < 400:
        return extract_whatsapp_codes(text), False
    if status in RATE_LIMIT_STATUSES:
        cooldown.trigger(retry_delay(headers, 0))
        return None, True
    if status >= 500:
        return None, True
    return None, False


async def scrape_site(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    cooldown: Cooldown,
    url: str,
) -> list[str]:
    codes, fallback_may_help = await _direct_scrape_once(client, sem, cooldown, url)
    if codes is not None:
        return codes
    if not fallback_may_help:
        return []

    if KEY_MANAGER.enabled:
        try:
            status, text, _headers = await asyncio.wait_for(
                gateway_get(client, sem, url, MAX_PAGE_BYTES),
                GATEWAY_TOTAL_DEADLINE_SECONDS,
            )
        except Exception as exc:
            logger.info("Gateway scrape failed for %s: %s", url, exc.__class__.__name__)
            return []
        return extract_whatsapp_codes(text) if status < 400 else []

    await asyncio.sleep(1.0)
    codes, _ = await _direct_scrape_once(client, sem, cooldown, url)
    return codes or []


async def scrape_all(
    client: httpx.AsyncClient,
    urls: list[str],
    direct_codes: list[str],
    progress: ProgressReporter,
) -> list[str]:
    sem = asyncio.Semaphore(SCRAPE_CONCURRENCY)
    cooldown = Cooldown()
    per_site: list[list[str]] = [[] for _ in urls]
    state = {"done": 0, "failed": 0}
    total = len(urls)

    def current_found() -> int:
        unique = set(direct_codes)
        for codes in per_site:
            unique.update(codes)
        return len(unique)

    async def worker(index: int, url: str) -> None:
        try:
            per_site[index] = await scrape_site(client, sem, cooldown, url)
        except Exception as exc:
            logger.warning("Scrape worker error for %s: %s", url, exc)
            per_site[index] = []
        state["done"] += 1
        await progress.update(
            f"🔎 Scraping websites: {state['done']}/{total}\n"
            f"🔗 Unique links found so far: {current_found()}",
            force=state["done"] == total,
        )

    tasks = [asyncio.create_task(worker(i, u)) for i, u in enumerate(urls)]
    try:
        if tasks:
            await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()

    merged: dict[str, None] = {}
    for code in direct_codes:
        merged.setdefault(code, None)
    for codes in per_site:
        for code in codes:
            merged.setdefault(code, None)
    return list(merged)


# --------------------------------------------------------------------------- #
# Link validation engine
# --------------------------------------------------------------------------- #

def _meta_content(soup: BeautifulSoup, prop: str) -> str:
    tag = soup.find("meta", attrs={"property": prop}) or soup.find(
        "meta", attrs={"name": prop}
    )
    if tag is None:
        return ""
    content = tag.get("content")
    return str(content).strip() if content else ""


def classify_invite_page(page: str) -> str:
    if len(page.strip()) < 200:
        return STATUS_UNKNOWN

    soup = BeautifulSoup(page, "html.parser")
    title = _meta_content(soup, "og:title")
    image = _meta_content(soup, "og:image")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    body = re.sub(r"\s+", " ", soup.get_text(" ", strip=True)).lower()
    haystack = f"{title.lower()} {body}"

    if any(marker in haystack for marker in DEAD_MARKERS):
        return STATUS_DEAD

    if not title and not image:
        return STATUS_UNKNOWN

    has_group_name = bool(title) and title.strip().lower() not in GENERIC_TITLES
    has_group_image = "pps.whatsapp.net" in image.lower()
    if not (has_group_name or has_group_image):
        return STATUS_DEAD

    if any(marker in haystack for marker in APPROVAL_MARKERS):
        return STATUS_APPROVAL

    return STATUS_VALID


async def validate_code(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    cooldown: Cooldown,
    code: str,
) -> str:
    url = code_to_link(code)
    use_gateway = KEY_MANAGER.enabled
    for attempt in range(MAX_VALIDATION_ATTEMPTS):
        try:
            if use_gateway:
                status, text, headers = await asyncio.wait_for(
                    gateway_get(client, sem, url, MAX_INVITE_PAGE_BYTES),
                    GATEWAY_TOTAL_DEADLINE_SECONDS,
                )
            else:
                await cooldown.wait()
                async with sem:
                    status, text, headers = await asyncio.wait_for(
                        http_get(client, url, MAX_INVITE_PAGE_BYTES),
                        REQUEST_DEADLINE_SECONDS,
                    )
                    await asyncio.sleep(
                        random.uniform(PACING_MIN_SECONDS, PACING_MAX_SECONDS)
                    )
        except Exception as exc:
            logger.debug("Validation request error for %s: %s", code, exc.__class__.__name__)
            await asyncio.sleep(0.5 * (attempt + 1))
            continue

        if not use_gateway and status in RATE_LIMIT_STATUSES:
            cooldown.trigger(retry_delay(headers, attempt))
            continue
        if status in DEAD_STATUSES:
            return STATUS_DEAD
        if status != 200:
            await asyncio.sleep(0.5 * (attempt + 1))
            continue

        result = classify_invite_page(text)
        if result != STATUS_UNKNOWN:
            return result
        await asyncio.sleep(0.5 * (attempt + 1))
    return STATUS_UNKNOWN


def _validation_progress_text(title: str, done: int, total: int, counts: Counter) -> str:
    return (
        f"{title}: {done}/{total}\n"
        f"✅ Instant-join: {counts[STATUS_VALID]}\n"
        f"⛔ Dead/expired: {counts[STATUS_DEAD]}\n"
        f"🔒 Admin approval: {counts[STATUS_APPROVAL]}\n"
        f"⚠️ Unverified: {counts[STATUS_UNKNOWN]}"
    )


async def validate_all(
    client: httpx.AsyncClient,
    codes: list[str],
    progress: ProgressReporter,
    title: str = "🧪 Validating links",
) -> dict[str, str]:
    sem = asyncio.Semaphore(VALIDATE_CONCURRENCY)
    cooldown = Cooldown()
    statuses: dict[str, str] = {}
    counts: Counter = Counter()
    total = len(codes)

    async def worker(code: str) -> None:
        try:
            status = await asyncio.wait_for(
                validate_code(client, sem, cooldown, code), LINK_DEADLINE_SECONDS
            )
        except Exception as exc:
            logger.debug("Validation worker error for %s: %s", code, exc.__class__.__name__)
            status = STATUS_UNKNOWN
        statuses[code] = status
        counts[status] += 1
        done = len(statuses)
        await progress.update(
            _validation_progress_text(title, done, total, counts), force=done == total
        )

    tasks = [asyncio.create_task(worker(code)) for code in codes]
    try:
        if tasks:
            await asyncio.gather(*tasks)
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
    return statuses


# --------------------------------------------------------------------------- #
# Output delivery
# --------------------------------------------------------------------------- #

def chunk_numbered_lines(lines: list[str], limit: int = TELEGRAM_CHUNK_LIMIT) -> list[str]:
    chunks: list[str] = []
    current: list[str] = []
    current_len = 0
    for line in lines:
        added = len(line) + (1 if current else 0)
        if current and current_len + added > limit:
            chunks.append("\n".join(current))
            current = [line]
            current_len = len(line)
        else:
            current.append(line)
            current_len += added
    if current:
        chunks.append("\n".join(current))
    return chunks


async def deliver_results(
    bot: Any,
    chat_id: int,
    ordered_codes: list[str],
    statuses: dict[str, str],
    note: str,
) -> None:
    counts = Counter(statuses.get(code, STATUS_UNKNOWN) for code in ordered_codes)
    valid_links = [
        code_to_link(code)
        for code in ordered_codes
        if statuses.get(code) == STATUS_VALID
    ]

    summary = (
        f"🔍 Links checked: {len(ordered_codes)}\n"
        f"✅ Instant-join: {counts[STATUS_VALID]}\n"
        f"⛔ Dead/expired/reset: {counts[STATUS_DEAD]}\n"
        f"🔒 Admin approval required: {counts[STATUS_APPROVAL]}\n"
        f"⚠️ Could not verify (rate-limited/timeouts): {counts[STATUS_UNKNOWN]}"
    )
    if note:
        summary += f"\n\n{note}"

    if not valid_links:
        await safe_send(
            bot,
            chat_id,
            "😕 No verified instant-join WhatsApp group links were found.\n\n"
            f"{summary}\n\nChoose what to do next:",
            reply_markup=main_menu_markup(),
        )
        return

    numbered = [f"{index}. {link}" for index, link in enumerate(valid_links, start=1)]
    for chunk in chunk_numbered_lines(numbered):
        await safe_send(bot, chat_id, chunk)
        await asyncio.sleep(0.3)

    payload = ("\n".join(numbered) + "\n").encode("utf-8")
    await safe_send_document(
        bot,
        chat_id,
        payload,
        OUTPUT_FILENAME,
        f"📎 Backup file: {len(valid_links)} verified instant-join links",
    )

    await safe_send(
        bot,
        chat_id,
        f"✅ Finished!\n\n{summary}\n\nChoose what to do next:",
        reply_markup=main_menu_markup(),
    )


# --------------------------------------------------------------------------- #
# Background job
# --------------------------------------------------------------------------- #

async def run_job(
    bot: Any,
    chat_id: int,
    user_data: dict,
    mode: str,
    urls: list[str],
    codes: list[str],
) -> None:
    try:
        start_text = (
            "⏳ Starting website scraping..."
            if mode == STATE_AWAITING_WEBSITES
            else "⏳ Starting link validation..."
        )
        status_message = await safe_send(bot, chat_id, start_text)
        progress = ProgressReporter(
            bot, chat_id, status_message.message_id if status_message else None
        )

        note = ""
        async with build_client() as client:
            if mode == STATE_AWAITING_WEBSITES:
                all_codes = await scrape_all(client, urls, codes, progress)
            else:
                all_codes = list(codes)

            if len(all_codes) > MAX_LINKS:
                all_codes = all_codes[:MAX_LINKS]
                note = f"ℹ️ Only the first {MAX_LINKS} links were checked."

            if not all_codes:
                await progress.update("ℹ️ No WhatsApp group links were found.", force=True)
                await safe_send(
                    bot,
                    chat_id,
                    "😕 I couldn't find any WhatsApp group invite links.\n\n"
                    "Choose what to do next:",
                    reply_markup=main_menu_markup(),
                )
                return

            statuses = await validate_all(client, all_codes, progress)

            unverified = [c for c in all_codes if statuses.get(c) == STATUS_UNKNOWN]
            if unverified:
                await asyncio.sleep(2.0)
                retry_statuses = await validate_all(
                    client, unverified, progress, title="🔁 Retrying unverified links"
                )
                statuses.update(retry_statuses)

            key_note = KEY_MANAGER.health_note()
            if key_note:
                note = (note + "\n" + key_note).strip()
            if KEY_MANAGER.enabled:
                logger.info("ScraperAPI usage: %s", KEY_MANAGER.usage_summary())

        await progress.update(
            f"✅ Finished checking {len(all_codes)} links. Sending results...",
            force=True,
        )
        await deliver_results(bot, chat_id, all_codes, statuses, note)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Job failed for chat %s", chat_id)
        try:
            await safe_send(
                bot,
                chat_id,
                "❌ Something went wrong while processing your batch. "
                "Please try again.",
                reply_markup=main_menu_markup(),
            )
        except Exception:
            logger.exception("Could not send failure notice to chat %s", chat_id)
    finally:
        user_data["busy"] = False


# --------------------------------------------------------------------------- #
# Telegram handlers
# --------------------------------------------------------------------------- #

WELCOME_TEXT = (
    "👋 Welcome to the WhatsApp Group Link Bot!\n\n"
    "I can scrape websites for WhatsApp group invite links, or validate links "
    "you already have, and return only the groups you can join instantly "
    "(no dead links, no admin approval).\n\n"
    "Choose an option below:"
)

BUSY_TEXT = "⏳ I'm still working on your previous batch. Please wait until it finishes."


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    if not context.user_data.get("busy"):
        context.user_data["state"] = None
    await safe_send(
        context.bot, chat.id, WELCOME_TEXT, reply_markup=main_menu_markup()
    )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    if context.user_data.get("busy"):
        await safe_send(context.bot, chat.id, BUSY_TEXT)
        return
    context.user_data["state"] = None
    await safe_send(
        context.bot,
        chat.id,
        "🛑 Cancelled. Choose an option:",
        reply_markup=main_menu_markup(),
    )


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    chat = update.effective_chat
    if query is None or chat is None:
        return
    try:
        await query.answer()
    except TelegramError:
        pass

    if context.user_data.get("busy"):
        await safe_send(context.bot, chat.id, BUSY_TEXT)
        return

    if query.data == BTN_SCRAPE:
        context.user_data["state"] = STATE_AWAITING_WEBSITES
        await safe_send(
            context.bot,
            chat.id,
            "🌐 Website mode\n\n"
            "Send me the website URLs (one per line), or upload a .txt file "
            "containing one URL per line.",
        )
    elif query.data == BTN_VALIDATE_LIST:
        context.user_data["state"] = STATE_AWAITING_LINKS
        await safe_send(
            context.bot,
            chat.id,
            "📁 Link validation mode\n\n"
            "Send me your WhatsApp group links (one per line, or pasted text), "
            "or upload a .txt file containing them.",
        )


async def handle_user_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE, text: str
) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    user_data = context.user_data
    bot = context.bot

    if user_data.get("busy"):
        await safe_send(bot, chat.id, BUSY_TEXT)
        return

    state = user_data.get("state")
    if state not in (STATE_AWAITING_WEBSITES, STATE_AWAITING_LINKS):
        await safe_send(
            bot,
            chat.id,
            "Please choose an option first:",
            reply_markup=main_menu_markup(),
        )
        return

    urls: list[str] = []
    codes: list[str] = []
    note = ""

    if state == STATE_AWAITING_WEBSITES:
        codes = extract_whatsapp_codes(text)
        urls = parse_website_urls(text)
        if not urls and not codes:
            await safe_send(
                bot,
                chat.id,
                "⚠️ I couldn't find any valid website URLs. "
                "Please send them one per line (for example: https://example.com).",
            )
            return
        if len(urls) > MAX_WEBSITES:
            urls = urls[:MAX_WEBSITES]
            note = f"\nℹ️ Only the first {MAX_WEBSITES} websites will be scraped."
        await safe_send(
            bot,
            chat.id,
            f"📥 Received {len(urls)} website(s). Working on it...{note}",
        )
    else:
        codes = extract_whatsapp_codes(text)
        if not codes:
            await safe_send(
                bot,
                chat.id,
                "⚠️ I couldn't find any WhatsApp group links "
                "(https://chat.whatsapp.com/...). Please try again.",
            )
            return
        await safe_send(
            bot, chat.id, f"📥 Received {len(codes)} unique link(s). Working on it..."
        )

    user_data["busy"] = True
    user_data["state"] = None
    context.application.create_task(
        run_job(bot, chat.id, user_data, state, urls, codes)
    )


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    if message is None or not message.text:
        return
    await handle_user_input(update, context, message.text)


async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    message = update.effective_message
    chat = update.effective_chat
    if message is None or message.document is None or chat is None:
        return

    if context.user_data.get("busy"):
        await safe_send(context.bot, chat.id, BUSY_TEXT)
        return

    if context.user_data.get("state") not in (
        STATE_AWAITING_WEBSITES,
        STATE_AWEIGHT_LINKS if "STATE_AWEIGHT_LINKS" in globals() else STATE_AWAITING_LINKS,
    ):
        await safe_send(
            context.bot,
            chat.id,
            "Please choose an option first:",
            reply_markup=main_menu_markup(),
        )
        return

    document = message.document
    file_name = (document.file_name or "").lower()
    if not (file_name.endswith(".txt") or document.mime_type == "text/plain"):
        await safe_send(context.bot, chat.id, "⚠️ Please upload a plain .txt file.")
        return
    if document.file_size and document.file_size > MAX_TEXT_FILE_BYTES:
        await safe_send(
            context.bot,
            chat.id,
            f"⚠️ That file is too large. The limit is {MAX_TEXT_FILE_BYTES // (1024 * 1024)} MB.",
        )
        return

    try:
        telegram_file = await tg_call(lambda: document.get_file())
        data = await tg_call(lambda: telegram_file.download_as_bytearray())
    except Exception:
        logger.exception("Failed to download document for chat %s", chat.id)
        await safe_send(
            context.bot,
            chat.id,
            "❌ I couldn't download that file. Please try uploading it again.",
        )
        return

    await handle_user_input(update, context, decode_bytes(bytes(data)))


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Unhandled exception while processing an update", exc_info=context.error)


# --------------------------------------------------------------------------- #
# Entry point
# --------------------------------------------------------------------------- #

def main() -> None:
    if not BOT_TOKEN:
        logger.critical("BOT_TOKEN is not set. Exiting.")
        sys.exit(1)

    start_health_server()
    logger.info("ScraperAPI gateway: %s key(s) loaded", KEY_MANAGER.size)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(15)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(15)
        .get_updates_connect_timeout(15)
        .get_updates_read_timeout(30)
        .get_updates_pool_timeout(15)
        .build()
    )

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", start_command))
    application.add_handler(CommandHandler("cancel", cancel_command))
    application.add_handler(
        CallbackQueryHandler(menu_callback, pattern=f"^({BTN_SCRAPE}|{BTN_VALIDATE_LIST})$")
    )
    application.add_handler(MessageHandler(filters.Document.ALL, document_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    application.add_error_handler(error_handler)

    logger.info("Bot is starting polling...")
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
