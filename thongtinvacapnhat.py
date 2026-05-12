"""
AI Multi-Agent News Bot — Patched
==================================
Fixes applied (from review):
 A. State lưu YYYY-MM-DD-HH thay vì chỉ slot → đúng qua ngày
 B. Scheduler chờ mốc tiếp theo TRƯỚC, không gửi ngay khi restart
 C. Anti-dup chỉ lưu hash bài thật sự được judge chọn
 D. sent_urls lưu list có thứ tự, cắt đúng entry mới nhất
 E. JSON parser robust: strip fence + regex tìm [ ] / { }
 F. Validate schema article trước khi gửi judge
 G. Normalize URL bỏ tracking params (utm_*, fbclid, gclid)
 H. safe_text() escape mọi mention Discord + @channel
 I. ssl=False bỏ, dùng default TLS verify
 J. Check EDITOR env khi startup
 K. asyncio.create_task thay bot.loop.create_task (discord.py mới)
 L. Recursive retry → iterative loop
"""

import asyncio
import importlib
import hashlib
import html as html_lib
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, unquote, urlencode, urljoin, urlparse, urlunparse

import aiohttp
import discord
from discord.ext import commands
from dotenv import load_dotenv

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# =========================================================
# ENV
# =========================================================

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
CHANNEL_ID    = int(os.getenv("CHANNEL_ID", "0") or 0)

# Agent 1 — Researcher (model rẻ, nhanh)
RESEARCH_API_BASE = os.getenv("RESEARCH_API_BASE", os.getenv("API_BASE", "")).rstrip("/")
RESEARCH_API_KEY  = os.getenv("RESEARCH_API_KEY",  os.getenv("API_KEY", "")).strip()
RESEARCH_MODEL    = os.getenv("RESEARCH_MODEL",    os.getenv("MODEL_NAME", "")).strip()

# Agent 2 — Judge/Editor (model mạnh)
EDITOR_API_BASE   = os.getenv("EDITOR_API_BASE",  RESEARCH_API_BASE).rstrip("/")
EDITOR_API_KEY    = os.getenv("EDITOR_API_KEY",   RESEARCH_API_KEY).strip()
EDITOR_MODEL      = os.getenv("EDITOR_MODEL",     RESEARCH_MODEL).strip()

# Tavily — legacy backend, khong con nam trong flow chinh Agent 1
TAVILY_API_KEY   = os.getenv("TAVILY_API_KEY", "").strip()
TAVILY_API_BASE  = os.getenv("TAVILY_API_BASE", "https://api.tavily.com").rstrip("/")
TAVILY_ENABLED   = bool(TAVILY_API_KEY) and os.getenv("TAVILY_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}

# Web Search compatible endpoint — tool search do Agent 1 truc tiep goi
WEB_SEARCH_API_BASE = os.getenv("WEB_SEARCH_API_BASE", RESEARCH_API_BASE).rstrip("/")
WEB_SEARCH_API_KEY  = os.getenv("WEB_SEARCH_API_KEY", RESEARCH_API_KEY).strip()
WEB_SEARCH_MODEL    = os.getenv("WEB_SEARCH_MODEL", "searxng").strip()
WEB_SEARCH_ENABLED  = bool(WEB_SEARCH_API_BASE and WEB_SEARCH_API_KEY) and os.getenv("WEB_SEARCH_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}

# SearXNG direct GET — legacy fallback neu compatible endpoint khong dung duoc
SEARXNG_BASE_URL = os.getenv("SEARXNG_BASE_URL", "").rstrip("/")
SEARXNG_ENABLED  = bool(SEARXNG_BASE_URL) and os.getenv("SEARXNG_ENABLED", "1").strip().lower() not in {"0", "false", "no", "off"}

# =========================================================
# CONFIG
# =========================================================

class Config:
    SEARCH_INTERVAL_HOURS = 12
    MAX_RETRIES           = int(os.getenv("MAX_RETRIES", "3") or 3)
    RETRY_DELAY           = float(os.getenv("RETRY_DELAY", "1.2") or 1.2)
    API_RETRY_JITTER      = float(os.getenv("API_RETRY_JITTER", "1.5") or 1.5)
    RETRY_MAX_DELAY       = float(os.getenv("RETRY_MAX_DELAY", "45") or 45)
    RETRY_STORM_JITTER    = float(os.getenv("RETRY_STORM_JITTER", "2.5") or 2.5)
    REQUEST_TIMEOUT       = float(os.getenv("REQUEST_TIMEOUT", "180") or 180)

    API_CONNECT_TIMEOUT   = float(os.getenv("API_CONNECT_TIMEOUT", "15") or 15)
    API_SOCK_READ_TIMEOUT = float(os.getenv("API_SOCK_READ_TIMEOUT", "180") or 180)
    SEARCH_TOTAL_TIMEOUT  = float(os.getenv("SEARCH_TOTAL_TIMEOUT", "35") or 35)
    SEARCH_CONNECT_TIMEOUT = float(os.getenv("SEARCH_CONNECT_TIMEOUT", "8") or 8)
    SEARCH_SOCK_READ_TIMEOUT = float(os.getenv("SEARCH_SOCK_READ_TIMEOUT", "25") or 25)

    MAX_PARALLEL_RESEARCH = int(os.getenv("MAX_PARALLEL_RESEARCH", "50"))
    MAX_PARALLEL_EDITOR   = int(os.getenv("MAX_PARALLEL_EDITOR", "50"))
    MAX_PARALLEL_SEARXNG  = int(os.getenv("MAX_PARALLEL_SEARXNG", "50"))
    RESEARCH_QUEUE_MAX    = int(os.getenv("RESEARCH_QUEUE_MAX", "24"))
    MAX_PLAIN_TEXT        = 1900
    DISCORD_DELAY         = 0.4

    TIMEZONE_OFFSET       = 7
    CACHE_EXPIRE          = 3600
    CACHE_CLEANUP_INTERVAL = 3600
    SEARCH_QUERY_CACHE_EXPIRE = int(os.getenv("SEARCH_QUERY_CACHE_EXPIRE", "600") or 600)

    TCP_LIMIT             = int(os.getenv("TCP_LIMIT", "80") or 80)
    TCP_LIMIT_PER_HOST    = int(os.getenv("TCP_LIMIT_PER_HOST", "12") or 12)
    TCP_KEEPALIVE         = float(os.getenv("TCP_KEEPALIVE", "45") or 45)
    TCP_ENABLE_CLEANUP_CLOSED = os.getenv("TCP_ENABLE_CLEANUP_CLOSED", "1").strip().lower() not in {"0", "false", "no", "off"}
    DNS_CACHE             = int(os.getenv("DNS_CACHE", "300") or 300)

    RESEARCH_MAX_TOKENS   = int(os.getenv("RESEARCH_MAX_TOKENS", "1800") or 1800)
    EDITOR_MAX_TOKENS     = int(os.getenv("EDITOR_MAX_TOKENS", "2000") or 2000)
    RESEARCH_TOOL_MAX_STEPS = int(os.getenv("RESEARCH_TOOL_MAX_STEPS", "4") or 4)

    STATE_FILE            = "bot_state.json"
    SENT_URLS_FILE        = "sent_urls.json"
    SENT_URLS_MAX         = 2000

    SLOTS                 = [6, 18]
    SCHEDULER_RETRY_DELAY = 300
    SCHEDULER_SLOT_MAX_RETRIES = int(os.getenv("SCHEDULER_SLOT_MAX_RETRIES", "3") or 3)
    BACKGROUND_TASK_RESTART_DELAY = 30

    TAVILY_MAX_RESULTS    = int(os.getenv("TAVILY_MAX_RESULTS", "4") or 4)
    TAVILY_SEARCH_DEPTH   = os.getenv("TAVILY_SEARCH_DEPTH", "basic").strip() or "basic"

    WEB_SEARCH_MAX_RESULTS = int(os.getenv("WEB_SEARCH_MAX_RESULTS", "15") or 15)
    WEB_SEARCH_TYPE       = os.getenv("WEB_SEARCH_TYPE", "web").strip() or "web"

    SEARXNG_MAX_RESULTS   = int(os.getenv("SEARXNG_MAX_RESULTS", "5") or 5)
    SEARXNG_CATEGORIES    = os.getenv("SEARXNG_CATEGORIES", "news,general").strip() or "news,general"
    SEARXNG_LANGUAGE      = os.getenv("SEARXNG_LANGUAGE", "all").strip() or "all"
    SEARXNG_TIME_RANGE    = os.getenv("SEARXNG_TIME_RANGE", "day").strip() or "day"
    SEARXNG_HTML_FALLBACK = os.getenv("SEARXNG_HTML_FALLBACK", "1").strip().lower() not in {"0", "false", "no", "off"}
    SEARXNG_USER_AGENT    = os.getenv(
        "SEARXNG_USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    ).strip()
    SEARXNG_JSON_FAIL_THRESHOLD = int(os.getenv("SEARXNG_JSON_FAIL_THRESHOLD", "5") or 5)
    SEARXNG_JSON_COOLDOWN = float(os.getenv("SEARXNG_JSON_COOLDOWN", "300") or 300)
    SEARXNG_HEALTH_INTERVAL = float(os.getenv("SEARXNG_HEALTH_INTERVAL", "60") or 60)
    SEARXNG_HEALTH_TIMEOUT = float(os.getenv("SEARXNG_HEALTH_TIMEOUT", "8") or 8)
    SEARXNG_MIN_DELAY     = float(os.getenv("SEARXNG_MIN_DELAY", "0.15") or 0.15)
    SEARXNG_MAX_DELAY     = float(os.getenv("SEARXNG_MAX_DELAY", "2.5") or 2.5)
    HTML_PARSE_MAX_BYTES  = int(os.getenv("HTML_PARSE_MAX_BYTES", "800000") or 800000)
    LOG_COOLDOWN_SECONDS  = float(os.getenv("LOG_COOLDOWN_SECONDS", "60") or 60)

    # Tracking params cần loại bỏ khi normalize URL
    TRACKING_PARAMS = {
        "utm_source", "utm_medium", "utm_campaign", "utm_term",
        "utm_content", "utm_id", "fbclid", "gclid", "msclkid",
        "ref", "source", "_ga",
    }

# =========================================================
# TOPIC GROUPS
# =========================================================

TOPIC_GROUPS: Dict[str, List[str]] = {
    "🤖 AI & Cong Nghe": [
        "AI news worldwide",
        "OpenAI ChatGPT updates",
        "Claude AI Anthropic",
        "Gemini AI Google",
        "DeepSeek AI",
        "AI agents automation",
        "LLM breakthrough",
        "robotics AI",
        "cybersecurity incident",
        "NVIDIA AMD Intel AI",
        "Apple Google Microsoft news",
        "Samsung technology",
        "Linux Windows update",
        "cloud quantum computing",
        "SpaceX NASA news",
        "Viet Nam AI cong nghe",
        "Viet Nam startup an ninh mang",
    ],
    "🎮 Gaming": [
        "gaming news",
        "Steam PlayStation Xbox Nintendo",
        "gacha game Genshin Honkai",
        "Wuthering Waves news",
        "Esports VCS LMHT",
    ],
    "📈 Crypto & Finance": [
        "Bitcoin Ethereum news",
        "crypto DeFi market",
        "VN-Index chung khoan",
        "stock market global",
        "gold USD exchange rate",
    ],
    "⚽ Sports": [
        "Premier League Champions League",
        "transfer news football",
        "Vietnam football",
        "Esports tournament",
    ],
    "🎬 Entertainment": [
        "Netflix Hollywood Disney news",
        "celebrity KDrama Kpop",
        "music industry news",
        "Viet Nam phim giai tri",
    ],
    "⛩️ Anime & Manga": [
        "anime manga news 2026",
        "cac bo anime moi trong mua nay, cac tap anime của cac bo vao hom nay",
        "anime adaptation light novel",
        "Japanese anime industry",
    ],
    "🏥 Health": [
        "medical breakthrough virus",
        "fitness nutrition longevity",
        "Viet Nam y te suc khoe",
    ],
    "🇻🇳 Viet Nam": [
        "Viet Nam kinh te giao duc",
        "Viet Nam phap luat moi truong",
        "Ha Noi TP HCM tin moi",
        "Viet Nam giao thong y te",
    ],
}

GROUP_COLORS: Dict[str, int] = {
    "🤖 AI & Cong Nghe":   0x00CFFF,
    "🎮 Gaming":            0x9966FF,
    "📈 Crypto & Finance":  0xF7D000,
    "⚽ Sports":             0x44FF88,
    "🎬 Entertainment":     0xFF9900,
    "⛩️ Anime & Manga":    0xFF69B4,
    "🏥 Health":            0x88FFCC,
    "🇻🇳 Viet Nam":         0xFF4444,
}

# =========================================================
# DISCORD BOT
# =========================================================

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)
NO_MENTIONS = discord.AllowedMentions.none()

# =========================================================
# GLOBALS
# =========================================================

CACHE: Dict[str, Dict[str, Any]] = {}
VN_TZ = timezone(timedelta(hours=Config.TIMEZONE_OFFSET))
research_semaphore = asyncio.Semaphore(Config.MAX_PARALLEL_RESEARCH)
editor_semaphore   = asyncio.Semaphore(Config.MAX_PARALLEL_EDITOR)
searxng_semaphore  = asyncio.Semaphore(Config.MAX_PARALLEL_SEARXNG)
state_lock         = asyncio.Lock()
sent_urls_lock     = asyncio.Lock()
digest_lock        = asyncio.Lock()
last_sent_key      = ""          # FIX A: lưu "YYYY-MM-DD-HH" thay vì chỉ slot int
last_processed_key = ""          # mốc cuối đã gửi thành công hoặc đã bỏ qua sau retry limit
scheduler_started  = False
cache_cleanup_started = False
searxng_json_blocked = False
search_query_cache: Dict[str, Dict[str, Any]] = {}
error_log_state: Dict[str, Dict[str, Any]] = {}

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
NO_RETRY_STATUS = {400, 401, 403, 404}

# =========================================================
# PRODUCTION SEARCH CONTROL
# =========================================================

class CircuitBreaker:
    """Circuit breaker nhẹ cho upstream search: CLOSED -> OPEN -> HALF_OPEN."""

    def __init__(self, name: str, fail_threshold: int, cooldown: float) -> None:
        self.name = name
        self.fail_threshold = max(1, fail_threshold)
        self.cooldown = max(1.0, cooldown)
        self.state = "CLOSED"
        self.fail_count = 0
        self.opened_at = 0.0
        self.last_error = ""
        self.last_success_at = 0.0
        self.last_failure_at = 0.0
        self._lock = asyncio.Lock()

    async def allow(self) -> bool:
        async with self._lock:
            if self.state == "OPEN":
                if time.time() - self.opened_at >= self.cooldown:
                    self.state = "HALF_OPEN"
                    return True
                return False
            return True

    async def record_success(self) -> None:
        async with self._lock:
            old_state = self.state
            self.state = "CLOSED"
            self.fail_count = 0
            self.last_error = ""
            self.last_success_at = time.time()
            if old_state != "CLOSED":
                log_with_cooldown("breaker_recover_" + self.name, logging.INFO, "%s breaker CLOSED", self.name)

    async def record_failure(self, reason: str, hard_open: bool = False) -> None:
        async with self._lock:
            self.fail_count += 1
            self.last_error = reason[:300]
            self.last_failure_at = time.time()
            if hard_open or self.fail_count >= self.fail_threshold:
                if self.state != "OPEN":
                    self.state = "OPEN"
                    self.opened_at = time.time()
                    log_with_cooldown(
                        "breaker_open_" + self.name,
                        logging.WARNING,
                        "%s breaker OPEN %ss after %d fail(s): %s",
                        self.name,
                        int(self.cooldown),
                        self.fail_count,
                        self.last_error,
                    )

    async def snapshot(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                "name": self.name,
                "state": self.state,
                "fail_count": self.fail_count,
                "cooldown_left": max(0, int(self.cooldown - (time.time() - self.opened_at))) if self.state == "OPEN" else 0,
                "last_error": self.last_error,
            }


searxng_json_breaker = CircuitBreaker(
    "SearXNG JSON",
    Config.SEARXNG_JSON_FAIL_THRESHOLD,
    Config.SEARXNG_JSON_COOLDOWN,
)


def log_with_cooldown(key: str, level: int, message: str, *args: Any, cooldown: Optional[float] = None) -> None:
    """Gộp log lặp để chống spam khi upstream lỗi liên tục."""
    now = time.time()
    cooldown = Config.LOG_COOLDOWN_SECONDS if cooldown is None else cooldown
    state = error_log_state.setdefault(key, {"last": 0.0, "suppressed": 0})
    elapsed = now - float(state.get("last", 0.0))
    if elapsed >= cooldown:
        suppressed = int(state.get("suppressed", 0))
        suffix = ""
        if suppressed:
            suffix = f" | suppressed={suppressed}"
        logger.log(level, message + suffix, *args)
        state["last"] = now
        state["suppressed"] = 0
    else:
        state["suppressed"] = int(state.get("suppressed", 0)) + 1


def compute_retry_delay(attempt: int, base: Optional[float] = None, retry_after: Optional[str] = None) -> float:
    """Exponential backoff + jitter + cap + anti retry storm."""
    if retry_after:
        try:
            return min(float(retry_after), Config.RETRY_MAX_DELAY)
        except Exception:
            pass
    base_delay = Config.RETRY_DELAY if base is None else base
    expo = base_delay * (2 ** max(0, attempt))
    jitter = random.uniform(0, Config.API_RETRY_JITTER + Config.RETRY_STORM_JITTER)
    return min(Config.RETRY_MAX_DELAY, expo + jitter)


def is_retryable_status(status: int) -> bool:
    return status in RETRYABLE_STATUS


def is_no_retry_status(status: int) -> bool:
    return status in NO_RETRY_STATUS


def api_timeout() -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(
        total=Config.REQUEST_TIMEOUT,
        connect=Config.API_CONNECT_TIMEOUT,
        sock_connect=Config.API_CONNECT_TIMEOUT,
        sock_read=Config.API_SOCK_READ_TIMEOUT,
    )


def search_timeout(total: Optional[float] = None) -> aiohttp.ClientTimeout:
    return aiohttp.ClientTimeout(
        total=Config.SEARCH_TOTAL_TIMEOUT if total is None else total,
        connect=Config.SEARCH_CONNECT_TIMEOUT,
        sock_connect=Config.SEARCH_CONNECT_TIMEOUT,
        sock_read=Config.SEARCH_SOCK_READ_TIMEOUT,
    )


def make_tcp_connector(limit: Optional[int] = None, per_host: Optional[int] = None) -> aiohttp.TCPConnector:
    """Connector dùng pooling, keepalive, DNS cache, cleanup closed sockets."""
    return aiohttp.TCPConnector(
        limit=Config.TCP_LIMIT if limit is None else limit,
        limit_per_host=Config.TCP_LIMIT_PER_HOST if per_host is None else per_host,
        ttl_dns_cache=Config.DNS_CACHE,
        keepalive_timeout=Config.TCP_KEEPALIVE,
        enable_cleanup_closed=Config.TCP_ENABLE_CLEANUP_CLOSED,
    )


def search_cache_key(kind: str, query: str, start: datetime, end: datetime) -> str:
    raw = f"{kind}:{query.strip().lower()}:{start.isoformat()}:{end.isoformat()}"
    return hashlib.md5(raw.encode()).hexdigest()


def get_search_cache(key: str) -> Optional[List[Dict[str, Any]]]:
    item = search_query_cache.get(key)
    if not item:
        return None
    try:
        if time.time() - float(item.get("ts", 0)) > Config.SEARCH_QUERY_CACHE_EXPIRE:
            search_query_cache.pop(key, None)
            return None
        data = item.get("data")
        return data if isinstance(data, list) else None
    except Exception:
        search_query_cache.pop(key, None)
        return None


def set_search_cache(key: str, data: List[Dict[str, Any]]) -> None:
    search_query_cache[key] = {"ts": time.time(), "data": merge_unique_articles(data)}
    if len(search_query_cache) > 500:
        for old_key in sorted(search_query_cache, key=lambda k: search_query_cache[k].get("ts", 0))[:100]:
            search_query_cache.pop(old_key, None)

# =========================================================
# TIME HELPERS
# =========================================================

def vn_now() -> datetime:
    return datetime.now(timezone.utc).astimezone(VN_TZ)

def format_range(start: datetime, end: datetime) -> str:
    return f"{start.strftime('%H:%M %d/%m/%Y')} -> {end.strftime('%H:%M %d/%m/%Y')}"

def slot_for(dt: datetime) -> int:
    """Tính slot gần nhất <= giờ hiện tại; nếu trước slot đầu, lấy slot cuối hôm trước."""
    hour = dt.hour
    best = None
    for slot in Config.SLOTS:
        if slot <= hour:
            if best is None or slot > best:
                best = slot
    if best is not None:
        return best
    return Config.SLOTS[-1]

def current_slot() -> int:
    return slot_for(vn_now())

def slot_start_time(dt: Optional[datetime] = None) -> datetime:
    dt = dt or vn_now()
    slot = slot_for(dt)
    if slot > dt.hour:
        dt = dt - timedelta(days=1)
    return dt.replace(hour=slot, minute=0, second=0, microsecond=0)

def slot_key(dt: Optional[datetime] = None) -> str:
    """Key dạng YYYY-MM-DD-HH, slot tính từ dt truyền vào."""
    return slot_start_time(dt or vn_now()).strftime("%Y-%m-%d-%H")

def parse_slot_key(key: str) -> Optional[datetime]:
    try:
        return datetime.strptime(key, "%Y-%m-%d-%H").replace(tzinfo=VN_TZ)
    except Exception:
        return None

def next_slot_after(dt: datetime) -> datetime:
    for slot in sorted(Config.SLOTS):
        candidate = dt.replace(hour=slot, minute=0, second=0, microsecond=0)
        if candidate > dt:
            return candidate
    return (dt + timedelta(days=1)).replace(
        hour=sorted(Config.SLOTS)[0],
        minute=0,
        second=0,
        microsecond=0,
    )

def next_slot_time(dt: Optional[datetime] = None) -> datetime:
    return next_slot_after(dt or vn_now())

def pending_slot_times(last_key: str, now: Optional[datetime] = None) -> List[datetime]:
    now = now or vn_now()
    current = slot_start_time(now)
    last_dt = parse_slot_key(last_key)
    if last_dt is None:
        return [current]

    due: List[datetime] = []
    candidate = next_slot_after(last_dt)
    while candidate <= current:
        due.append(candidate)
        candidate = next_slot_after(candidate)
    return due

async def wait_until_next_slot() -> None:
    nxt     = next_slot_time()
    seconds = max((nxt - vn_now()).total_seconds(), 0)
    logger.info("Waiting %.0fs until %s", seconds, nxt.strftime("%H:%M"))
    await asyncio.sleep(seconds)

# =========================================================
# PERSISTENT STATE  (FIX A)
# =========================================================

def atomic_write_json(path: str, data: Any) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp_path, path)

def load_state() -> None:
    global last_sent_key, last_processed_key
    try:
        with open(Config.STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            last_sent_key = str(data.get("last_sent_key", ""))
            last_processed_key = str(data.get("last_processed_key", last_sent_key))
    except Exception:
        last_sent_key = ""
        last_processed_key = ""

def save_state() -> None:
    try:
        atomic_write_json(
            Config.STATE_FILE,
            {
                "last_sent_key": last_sent_key,
                "last_processed_key": last_processed_key or last_sent_key,
            },
        )
    except Exception as e:
        logger.error("Loi luu state: %s", e)

# =========================================================
# ANTI-DUPLICATE MEMORY  (FIX C + D)
# =========================================================

def _normalize_url(url: str) -> str:
    """FIX G: bỏ tracking params trước khi hash."""
    try:
        parsed = urlparse(url.strip())
        qs     = parse_qs(parsed.query, keep_blank_values=False)
        clean  = {k: v for k, v in qs.items() if k.lower() not in Config.TRACKING_PARAMS}
        new_query = urlencode(clean, doseq=True)
        return urlunparse(parsed._replace(query=new_query)).lower()
    except Exception:
        return url.strip().lower()

def _url_hash(url: str) -> str:
    return hashlib.md5(_normalize_url(url).encode()).hexdigest()

def load_sent_urls() -> list:
    """Trả list (có thứ tự) thay vì set — FIX D."""
    try:
        with open(Config.SENT_URLS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return list(data) if isinstance(data, list) else []
    except Exception:
        return []

def save_sent_urls(hashes: list) -> None:
    """Giữ SENT_URLS_MAX entry mới nhất (cuối list = mới nhất). FIX D."""
    trimmed = hashes[-Config.SENT_URLS_MAX:]
    try:
        atomic_write_json(Config.SENT_URLS_FILE, trimmed)
    except Exception as e:
        logger.error("Loi luu sent_urls: %s", e)

def filter_new_articles(
    articles: List[Dict[str, Any]],
    sent_set: set,
) -> List[Dict[str, Any]]:
    new_articles: List[Dict[str, Any]] = []
    for art in articles:
        url = art.get("url", "")
        key = url if url else art.get("title", str(art))
        h = _url_hash(key)
        if h not in sent_set:
            sent_set.add(h)
            new_articles.append(art)
    return new_articles

def extract_sent_hashes_from_judged(judged: Dict[str, Any]) -> List[str]:
    """FIX C: chỉ hash bài nằm trong judged["groups"] — bài thật sự được chọn."""
    hashes: List[str] = []
    groups = judged.get("groups", {})
    if not isinstance(groups, dict):
        return hashes
    for articles in groups.values():
        if not isinstance(articles, list):
            continue
        for art in articles:
            if not isinstance(art, dict):
                continue
            url   = art.get("url", "")
            title = art.get("title", "")
            key   = url or title
            if key:
                hashes.append(_url_hash(key))
    return hashes

# =========================================================
# CACHE
# =========================================================

def make_cache_key(topic: str, start: datetime, end: datetime) -> str:
    raw = f"{topic}_{start.isoformat()}_{end.isoformat()}"
    return hashlib.md5(raw.encode()).hexdigest()

def get_cache(key: str) -> Optional[str]:
    item = CACHE.get(key)
    if not item:
        return None
    try:
        expired = time.time() - float(item["ts"]) > Config.CACHE_EXPIRE
    except Exception:
        expired = True
    if expired:
        CACHE.pop(key, None)
        return None
    return str(item["data"])

def set_cache(key: str, value: str) -> None:
    CACHE[key] = {"ts": time.time(), "data": value}

def cleanup_cache() -> int:
    now = time.time()
    expired = []
    for key, item in list(CACHE.items()):
        try:
            if now - float(item.get("ts", 0)) > Config.CACHE_EXPIRE:
                expired.append(key)
        except Exception:
            expired.append(key)
    for key in expired:
        CACHE.pop(key, None)
    return len(expired)

async def cache_cleanup_loop() -> None:
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            removed = cleanup_cache()
            if removed:
                logger.info("Cache cleanup: xoa %d muc het han", removed)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Cache cleanup loop loi")
        await asyncio.sleep(Config.CACHE_CLEANUP_INTERVAL)

# =========================================================
# JSON PARSER ROBUST  (FIX E)
# =========================================================

def strip_json_fence(raw: str) -> str:
    """Bỏ markdown code fence, trả raw text."""
    raw = raw.strip()
    if raw.startswith("```"):
        # lấy nội dung giữa hai fence
        parts = raw.split("```")
        raw   = parts[1] if len(parts) >= 2 else raw
        if raw.lower().startswith("json"):
            raw = raw[4:]
    return raw.strip()

def extract_first_json_value(raw: str, start_chars: str, expected_type: type) -> Optional[Any]:
    decoder = json.JSONDecoder()
    for i, ch in enumerate(raw):
        if ch not in start_chars:
            continue
        try:
            value, _ = decoder.raw_decode(raw[i:])
        except json.JSONDecodeError:
            continue
        if isinstance(value, expected_type):
            return value
    return None

def extract_json_array(raw: str) -> Optional[list]:
    """Tìm JSON array đầu tiên trong chuỗi, ngay cả khi có text thừa."""
    raw = strip_json_fence(raw)
    try:
        data = json.loads(raw)
        return data if isinstance(data, list) else None
    except Exception:
        pass
    parsed = extract_first_json_value(raw, "[", list)
    if isinstance(parsed, list):
        return parsed
    m = re.search(r"\[.*?\]", raw, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except Exception:
            pass
    return None

def extract_json_object(raw: str) -> Optional[dict]:
    """Tìm JSON object đầu tiên trong chuỗi."""
    raw = strip_json_fence(raw)
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        pass
    parsed = extract_first_json_value(raw, "{", dict)
    if isinstance(parsed, dict):
        return parsed
    m = re.search(r"\{.*?\}", raw, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except Exception:
            pass
    return None

# =========================================================
# ARTICLE VALIDATION  (FIX F)
# =========================================================

def valid_article(art: Any) -> bool:
    """Bài hợp lệ phải có title + summary tối thiểu."""
    return (
        isinstance(art, dict)
        and bool(art.get("title", "").strip())
        and bool(art.get("summary", "").strip())
    )

# =========================================================
# TEXT SANITIZER  (FIX H)
# =========================================================

def safe_text(s: Any) -> str:
    """Escape mọi mention Discord để bot không ping server/user/role."""
    escaped = discord.utils.escape_mentions(str(s))
    return re.sub(r"@(?=channel\b)", "@\u200b", escaped, flags=re.IGNORECASE)

# =========================================================
# STREAMING / JSON RESPONSE PARSER
# =========================================================

async def parse_openai_response(resp: aiohttp.ClientResponse) -> Dict[str, Any]:
    ct = resp.headers.get("Content-Type", "").lower()

    if "application/json" in ct:
        return await resp.json()

    if "text/event-stream" in ct:
        full_text = ""
        async for raw_line in resp.content:
            try:
                line = raw_line.decode("utf-8", errors="ignore").strip()
            except Exception:
                continue
            if not line or not line.startswith("data:"):
                continue
            data_str = line[5:].strip()
            if data_str == "[DONE]":
                break
            try:
                chunk   = json.loads(data_str)
                choices = chunk.get("choices", [])
                if not choices:
                    continue
                delta = choices[0].get("delta", {}).get("content")
                if delta is None:
                    delta = choices[0].get("message", {}).get("content", "")
                if delta:
                    full_text += delta
            except Exception:
                continue
        return {"choices": [{"message": {"content": full_text}}]}

    text = await resp.text()
    raise RuntimeError(f"Unsupported Content-Type: {ct}\n{text[:300]}")

# =========================================================
# GENERIC API CALL — iterative retry  (FIX L)
# =========================================================

async def _api_call(
    session: aiohttp.ClientSession,
    api_base: str,
    api_key: str,
    model: str,
    messages: List[Dict[str, str]],
    max_tokens: int,
) -> Optional[Dict[str, Any]]:
    """Iterative retry: chỉ retry 429/5xx + timeout/network; 401/403/404 không retry."""
    for attempt in range(Config.MAX_RETRIES + 1):
        try:
            started = time.perf_counter()
            async with session.post(
                f"{api_base}/chat/completions",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":       model,
                    "messages":    messages,
                    "temperature": 0.3,
                    "max_tokens":  max_tokens,
                    "stream":      False,
                },
                timeout=api_timeout(),
            ) as resp:
                latency_ms = (time.perf_counter() - started) * 1000
                if is_retryable_status(resp.status):
                    body = (await resp.text())[:300]
                    if attempt < Config.MAX_RETRIES:
                        delay = compute_retry_delay(attempt, retry_after=resp.headers.get("Retry-After"))
                        log_with_cooldown(
                            f"api_retry_{model}_{resp.status}",
                            logging.WARNING,
                            "API retry model=%s attempt=%s/%s delay=%.1fs status=%s latency=%.0fms body=%s",
                            model, attempt + 1, Config.MAX_RETRIES, delay, resp.status, latency_ms, body,
                        )
                        await asyncio.sleep(delay)
                        continue
                    log_with_cooldown(
                        f"api_retry_exhausted_{model}_{resp.status}",
                        logging.ERROR,
                        "API exhausted model=%s status=%s latency=%.0fms body=%s",
                        model, resp.status, latency_ms, body,
                    )
                    return None

                if resp.status != 200:
                    body = (await resp.text())[:500]
                    level = logging.ERROR if is_no_retry_status(resp.status) else logging.WARNING
                    log_with_cooldown(
                        f"api_no_retry_{model}_{resp.status}",
                        level,
                        "API no-retry model=%s status=%s latency=%.0fms body=%s",
                        model, resp.status, latency_ms, body,
                    )
                    return None

                return await parse_openai_response(resp)

        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            if attempt < Config.MAX_RETRIES:
                delay = compute_retry_delay(attempt)
                log_with_cooldown(
                    f"api_timeout_{model}",
                    logging.WARNING,
                    "API timeout retry model=%s attempt=%s/%s delay=%.1fs",
                    model, attempt + 1, Config.MAX_RETRIES, delay,
                )
                await asyncio.sleep(delay)
                continue
            log_with_cooldown(f"api_timeout_exhausted_{model}", logging.ERROR, "API timeout exhausted model=%s", model)
            return None
        except aiohttp.ClientError as e:
            if attempt < Config.MAX_RETRIES:
                delay = compute_retry_delay(attempt)
                log_with_cooldown(
                    f"api_client_{model}_{type(e).__name__}",
                    logging.WARNING,
                    "API client retry model=%s attempt=%s/%s delay=%.1fs error=%s",
                    model, attempt + 1, Config.MAX_RETRIES, delay, str(e)[:200],
                )
                await asyncio.sleep(delay)
                continue
            log_with_cooldown(f"api_client_exhausted_{model}", logging.ERROR, "API client exhausted model=%s error=%s", model, str(e)[:300])
            return None
        except Exception as e:
            log_with_cooldown(f"api_unexpected_{model}", logging.ERROR, "API unexpected model=%s error=%s", model, str(e)[:300])
            return None

    return None

# =========================================================
# SEARCH TOOLS / LEGACY BACKENDS: TAVILY + SEARXNG
# =========================================================

def source_from_url(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "") or "Tavily"
    except Exception:
        return "Tavily"


def compact_markdown_link(url: str, label: str = "Đọc bài viết") -> str:
    """Tạo link chữ xanh bấm được, không hiện URL dài."""
    clean_url = url.strip().strip("<>").rstrip(".,;:!?)]}”’»")
    if not clean_url:
        return ""
    source = source_from_url(clean_url)
    return f"[{label} - {source}]({clean_url})"

def importance_from_score(score: Any) -> int:
    try:
        value = float(score)
        if 0 <= value <= 1:
            value *= 100
        return max(0, min(100, int(value)))
    except Exception:
        return 50

async def tavily_search_topic(
    session: aiohttp.ClientSession,
    topic: str,
    group: str,
    start: datetime,
    end: datetime,
) -> Optional[List[Dict[str, Any]]]:
    if not TAVILY_ENABLED:
        return None

    days = max(1, int((end - start).total_seconds() // 86400) + 1)
    payload = {
        "api_key": TAVILY_API_KEY,
        "query": f"{topic} latest news {end.strftime('%Y-%m-%d')}",
        "topic": "news",
        "search_depth": Config.TAVILY_SEARCH_DEPTH,
        "max_results": Config.TAVILY_MAX_RESULTS,
        "days": days,
        "include_answer": False,
        "include_raw_content": False,
        "include_images": False,
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TAVILY_API_KEY}",
    }

    data: Optional[Dict[str, Any]] = None
    for attempt in range(Config.MAX_RETRIES + 1):
        try:
            async with session.post(
                f"{TAVILY_API_BASE}/search",
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT),
            ) as resp:
                if resp.status in {429, 500, 502, 503, 504} and attempt < Config.MAX_RETRIES:
                    delay = Config.RETRY_DELAY * (2 ** attempt)
                    logger.warning("Tavily retry %s/%s sau %.1fs (HTTP %s)", attempt + 1, Config.MAX_RETRIES, delay, resp.status)
                    await asyncio.sleep(delay)
                    continue
                if resp.status != 200:
                    logger.error("Tavily API %s: %s", resp.status, (await resp.text())[:300])
                    return None
                data = await resp.json()
                break
        except Exception as e:
            if attempt < Config.MAX_RETRIES:
                delay = Config.RETRY_DELAY * (2 ** attempt)
                logger.warning("Tavily loi retry %s/%s: %s", attempt + 1, Config.MAX_RETRIES, e)
                await asyncio.sleep(delay)
            else:
                logger.error("Tavily het retry: %s", e)
                return None

    if not isinstance(data, dict):
        return None

    articles: List[Dict[str, Any]] = []
    for item in data.get("results", []):
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        url = str(item.get("url", "")).strip()
        summary = str(item.get("content") or item.get("snippet") or "").strip()
        if item.get("published_date"):
            summary = f"{summary}\nNgay dang: {item.get('published_date')}".strip()
        art = {
            "title": title,
            "summary": summary,
            "url": url,
            "source": str(item.get("source") or source_from_url(url)).strip(),
            "topic": topic,
            "importance": importance_from_score(item.get("score", 0.5)),
            "_group": group,
        }
        if valid_article(art):
            articles.append(art)

    logger.info("Tavily: %s -> %d ket qua", topic, len(articles))
    return articles


def search_tool_enabled() -> bool:
    return WEB_SEARCH_ENABLED or SEARXNG_ENABLED


def web_search_url() -> str:
    base = WEB_SEARCH_API_BASE.rstrip("/")
    return base if base.endswith("/search") else f"{base}/search"


def web_search_articles_from_payload(
    payload: Any,
    clean_query: str,
    group: str,
    topic: str,
    start: datetime,
    end: datetime,
) -> List[Dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("choices"), list):
        try:
            content = payload["choices"][0].get("message", {}).get("content", "")
            parsed = extract_json_object(content) or extract_json_array(content)
            if parsed is not None:
                return web_search_articles_from_payload(parsed, clean_query, group, topic, start, end)
        except Exception:
            pass

    items: List[Any] = []
    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict):
        for key in ("results", "data", "items", "documents", "sources"):
            value = payload.get(key)
            if isinstance(value, list):
                items = value
                break
        if not items and isinstance(payload.get("result"), list):
            items = payload.get("result", [])

    articles: List[Dict[str, Any]] = []
    for item in items[:Config.WEB_SEARCH_MAX_RESULTS]:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or item.get("name") or item.get("headline") or "").strip()
        url = str(item.get("url") or item.get("link") or item.get("href") or item.get("source_url") or "").strip()
        summary = str(
            item.get("content")
            or item.get("snippet")
            or item.get("description")
            or item.get("summary")
            or item.get("text")
            or title
        ).strip()
        published = item.get("publishedDate") or item.get("published_date") or item.get("published") or item.get("date")
        if published:
            summary = f"{summary}\nNgay dang: {published}".strip()
        raw_source = item.get("source") or item.get("engine") or item.get("provider") or ""
        if isinstance(raw_source, dict):
            source = str(raw_source.get("name") or raw_source.get("title") or source_from_url(url)).strip()
        else:
            source = str(raw_source or source_from_url(url)).strip()
        art = {
            "title": title,
            "summary": summary,
            "url": url,
            "source": source,
            "topic": topic,
            "importance": importance_from_score(item.get("score", item.get("rank_score", 0.5))),
            "published_at": str(published or "").strip(),
            "search_query": clean_query,
            "time_window": format_range(start, end),
            "_group": group,
        }
        if valid_article(art) and url:
            articles.append(art)

    return merge_unique_articles(articles)


async def compatible_web_search_query(
    session: aiohttp.ClientSession,
    clean_query: str,
    group: str,
    topic: str,
    start: datetime,
    end: datetime,
) -> Optional[List[Dict[str, Any]]]:
    if not WEB_SEARCH_ENABLED:
        return None

    cache_key = search_cache_key("web", clean_query, start, end)
    cached = get_search_cache(cache_key)
    if cached is not None:
        logger.info("Web search cache hit: %s -> %d ket qua", clean_query, len(cached))
        return cached

    payload = {
        "model": WEB_SEARCH_MODEL,
        "query": clean_query,
        "search_type": Config.WEB_SEARCH_TYPE,
        "max_results": Config.WEB_SEARCH_MAX_RESULTS,
    }
    headers = {
        "Authorization": f"Bearer {WEB_SEARCH_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    for attempt in range(Config.MAX_RETRIES + 1):
        try:
            started = time.perf_counter()
            async with session.post(
                web_search_url(),
                headers=headers,
                json=payload,
                timeout=search_timeout(),
            ) as resp:
                body = await resp.text()
                latency_ms = (time.perf_counter() - started) * 1000
                if is_retryable_status(resp.status):
                    if attempt < Config.MAX_RETRIES:
                        delay = compute_retry_delay(attempt, retry_after=resp.headers.get("Retry-After"))
                        log_with_cooldown(
                            f"web_search_retry_{resp.status}",
                            logging.WARNING,
                            "Web search retry query=%s attempt=%s/%s delay=%.1fs status=%s latency=%.0fms body=%s",
                            clean_query, attempt + 1, Config.MAX_RETRIES, delay, resp.status, latency_ms, body[:200],
                        )
                        await asyncio.sleep(delay)
                        continue
                    log_with_cooldown(
                        f"web_search_exhausted_{resp.status}",
                        logging.ERROR,
                        "Web search exhausted query=%s status=%s latency=%.0fms body=%s",
                        clean_query, resp.status, latency_ms, body[:300],
                    )
                    return None
                if resp.status != 200:
                    level = logging.WARNING if is_no_retry_status(resp.status) else logging.ERROR
                    log_with_cooldown(
                        f"web_search_no_retry_{resp.status}",
                        level,
                        "Web search no-retry query=%s status=%s latency=%.0fms body=%s",
                        clean_query, resp.status, latency_ms, body[:300],
                    )
                    return None
                try:
                    data = json.loads(body)
                except Exception:
                    try:
                        data = await resp.json(content_type=None)
                    except Exception as e:
                        log_with_cooldown("web_search_json_malformed", logging.WARNING, "Web search JSON malformed query=%s error=%s", clean_query, str(e)[:200])
                        return None
                articles = web_search_articles_from_payload(data, clean_query, group, topic, start, end)
                set_search_cache(cache_key, articles)
                logger.info("Web search tool: %s -> %d ket qua latency=%.0fms", clean_query, len(articles), latency_ms)
                return articles
        except asyncio.CancelledError:
            raise
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            if attempt < Config.MAX_RETRIES:
                delay = compute_retry_delay(attempt)
                log_with_cooldown(
                    f"web_search_exc_{type(e).__name__}",
                    logging.WARNING,
                    "Web search retry query=%s attempt=%s/%s delay=%.1fs error=%s",
                    clean_query, attempt + 1, Config.MAX_RETRIES, delay, str(e)[:200],
                )
                await asyncio.sleep(delay)
                continue
            log_with_cooldown("web_search_exc_exhausted", logging.ERROR, "Web search exhausted query=%s error=%s", clean_query, str(e)[:300])
            return None
        except Exception as e:
            log_with_cooldown("web_search_unexpected", logging.ERROR, "Web search unexpected query=%s error=%s", clean_query, str(e)[:300])
            return None

    return None


def searxng_headers(kind: str = "json") -> Dict[str, str]:
    accept = "application/json" if kind == "json" else "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    return {
        "Accept": accept,
        "Accept-Language": "vi,en-US;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "User-Agent": Config.SEARXNG_USER_AGENT,
    }


def strip_html_fragment(value: str) -> str:
    value = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", value)
    value = re.sub(r"(?is)<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", html_lib.unescape(value)).strip()


def searxng_json_params(query: str) -> Dict[str, str]:
    return {
        "q": query,
        "format": "json",
        "categories": Config.SEARXNG_CATEGORIES,
        "language": Config.SEARXNG_LANGUAGE,
        "time_range": Config.SEARXNG_TIME_RANGE,
        "safesearch": "0",
    }


def searxng_html_params(query: str) -> Dict[str, str]:
    return {
        "q": query,
        "categories": Config.SEARXNG_CATEGORIES,
        "language": Config.SEARXNG_LANGUAGE,
        "time_range": Config.SEARXNG_TIME_RANGE,
        "safesearch": "0",
    }


def normalize_article_output(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for art in articles:
        if isinstance(art, dict) and art.get("url"):
            art["url"] = _normalize_url(str(art["url"]))
    return merge_unique_articles(articles)


def resolve_searxng_href(href: str) -> str:
    href = html_lib.unescape(href).strip()
    if not href:
        return ""
    if href.startswith("//"):
        href = "https:" + href
    if href.startswith("/"):
        parsed = urlparse(href)
        qs = parse_qs(parsed.query)
        for key in ("url", "q", "u"):
            if qs.get(key):
                candidate = unquote(qs[key][0]).strip()
                if candidate.startswith(("http://", "https://")):
                    return candidate
        href = urljoin(SEARXNG_BASE_URL + "/", href.lstrip("/"))
    return href


def searxng_articles_from_json(
    data: Dict[str, Any],
    clean_query: str,
    group: str,
    topic: str,
    start: datetime,
    end: datetime,
) -> List[Dict[str, Any]]:
    articles: List[Dict[str, Any]] = []
    items = data.get("results", []) if isinstance(data, dict) else []
    if not isinstance(items, list):
        return []
    for item in items[:Config.SEARXNG_MAX_RESULTS]:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        url = str(item.get("url", "")).strip()
        summary = str(
            item.get("content")
            or item.get("snippet")
            or item.get("description")
            or title
        ).strip()
        published = item.get("publishedDate") or item.get("published_date") or item.get("published")
        if published:
            summary = f"{summary}\nNgay dang: {published}".strip()
        source = str(item.get("source") or item.get("engine") or source_from_url(url)).strip()
        art = {
            "title": title,
            "summary": summary,
            "url": url,
            "source": source,
            "topic": topic,
            "importance": importance_from_score(item.get("score", 0.5)),
            "published_at": str(published or "").strip(),
            "search_query": clean_query,
            "time_window": format_range(start, end),
            "parser_mode": "json",
            "_group": group,
        }
        if valid_article(art) and url:
            articles.append(art)
    return normalize_article_output(articles)


def parse_searxng_html_results(
    page: str,
    clean_query: str,
    group: str,
    topic: str,
    start: datetime,
    end: datetime,
) -> List[Dict[str, Any]]:
    blocks = re.findall(
        r"(?is)<article\b[^>]*class=[\"'][^\"']*\bresult\b[^\"']*[\"'][^>]*>(.*?)</article>",
        page,
    )
    if not blocks:
        blocks = [page[m.start():m.start() + 2600] for m in re.finditer(r"(?is)<h3\b[^>]*>.*?</h3>", page)]

    articles: List[Dict[str, Any]] = []
    for block in blocks:
        link = re.search(r"(?is)<h3\b[^>]*>.*?<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>.*?</h3>", block)
        if not link:
            link = re.search(r"(?is)<a\b[^>]*class=[\"'][^\"']*result[^\"']*[\"'][^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", block)
        if not link:
            continue

        url = resolve_searxng_href(link.group(1))
        if not url.startswith(("http://", "https://")):
            continue

        title = strip_html_fragment(link.group(2))
        summary_match = re.search(
            r"(?is)<p\b[^>]*class=[\"'][^\"']*(?:content|snippet)[^\"']*[\"'][^>]*>(.*?)</p>",
            block,
        ) or re.search(r"(?is)<p\b[^>]*>(.*?)</p>", block)
        summary = strip_html_fragment(summary_match.group(1)) if summary_match else title
        source = source_from_url(url)
        art = {
            "title": title,
            "summary": summary,
            "url": url,
            "source": source,
            "topic": topic,
            "importance": 50,
            "published_at": "",
            "search_query": clean_query,
            "time_window": format_range(start, end),
            "_group": group,
        }
        if valid_article(art):
            articles.append(art)
        if len(articles) >= Config.SEARXNG_MAX_RESULTS:
            break

    return merge_unique_articles(articles)


async def searxng_search_html(
    session: aiohttp.ClientSession,
    clean_query: str,
    group: str,
    topic: str,
    start: datetime,
    end: datetime,
) -> Optional[List[Dict[str, Any]]]:
    params = {
        "q": clean_query,
        "categories": Config.SEARXNG_CATEGORIES,
        "language": Config.SEARXNG_LANGUAGE,
        "time_range": Config.SEARXNG_TIME_RANGE,
        "safesearch": "0",
    }

    for attempt in range(Config.MAX_RETRIES + 1):
        try:
            async with session.get(
                f"{SEARXNG_BASE_URL}/search",
                params=params,
                headers=searxng_headers("html"),
                timeout=aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT),
            ) as resp:
                body = await resp.text()
                if resp.status in {429, 500, 502, 503, 504} and attempt < Config.MAX_RETRIES:
                    delay = Config.RETRY_DELAY * (2 ** attempt) + random.uniform(0, Config.API_RETRY_JITTER)
                    logger.warning("SearXNG HTML retry %s/%s sau %.1fs (HTTP %s)", attempt + 1, Config.MAX_RETRIES, delay, resp.status)
                    await asyncio.sleep(delay)
                    continue
                if resp.status != 200:
                    logger.error("SearXNG HTML API %s: %s", resp.status, body[:300])
                    return None
                articles = parse_searxng_html_results(body, clean_query, group, topic, start, end)
                logger.info("SearXNG HTML tool: %s -> %d ket qua", clean_query, len(articles))
                return articles
        except Exception as e:
            if attempt < Config.MAX_RETRIES:
                delay = Config.RETRY_DELAY * (2 ** attempt) + random.uniform(0, Config.API_RETRY_JITTER)
                logger.warning("SearXNG HTML loi retry %s/%s: %s", attempt + 1, Config.MAX_RETRIES, e)
                await asyncio.sleep(delay)
            else:
                logger.error("SearXNG HTML het retry: %s", e)
                return None

    return None


async def searxng_search_query(
    session: aiohttp.ClientSession,
    query: str,
    group: str,
    topic: str,
    start: datetime,
    end: datetime,
) -> Optional[List[Dict[str, Any]]]:
    """Tool web search: Agent 1 tu sinh query, backend chi thuc thi tool call."""
    global searxng_json_blocked

    if not search_tool_enabled():
        return None

    clean_query = re.sub(r"\s+", " ", query).strip()[:300]
    if not clean_query:
        return []

    async with searxng_semaphore:
        await asyncio.sleep(random.uniform(0.15, 0.8))

        web_articles = await compatible_web_search_query(session, clean_query, group, topic, start, end)
        if isinstance(web_articles, list):
            return web_articles
        if not SEARXNG_ENABLED:
            return None

        if searxng_json_blocked and Config.SEARXNG_HTML_FALLBACK:
            return await searxng_search_html(session, clean_query, group, topic, start, end)

        params = {
            "q": clean_query,
            "format": "json",
            "categories": Config.SEARXNG_CATEGORIES,
            "language": Config.SEARXNG_LANGUAGE,
            "time_range": Config.SEARXNG_TIME_RANGE,
            "safesearch": "0",
        }

        data: Optional[Dict[str, Any]] = None
        for attempt in range(Config.MAX_RETRIES + 1):
            try:
                async with session.get(
                    f"{SEARXNG_BASE_URL}/search",
                    params=params,
                    headers=searxng_headers("json"),
                    timeout=aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT),
                ) as resp:
                    body = await resp.text()
                    if resp.status == 403 and Config.SEARXNG_HTML_FALLBACK:
                        searxng_json_blocked = True
                        logger.warning(
                            "SearXNG JSON 403. Co the search.formats chua bat json; chuyen sang HTML fallback. Body: %s",
                            body[:200],
                        )
                        return await searxng_search_html(session, clean_query, group, topic, start, end)
                    if resp.status in {429, 500, 502, 503, 504} and attempt < Config.MAX_RETRIES:
                        delay = Config.RETRY_DELAY * (2 ** attempt) + random.uniform(0, Config.API_RETRY_JITTER)
                        logger.warning("SearXNG JSON retry %s/%s sau %.1fs (HTTP %s)", attempt + 1, Config.MAX_RETRIES, delay, resp.status)
                        await asyncio.sleep(delay)
                        continue
                    if resp.status != 200:
                        logger.error("SearXNG JSON API %s: %s", resp.status, body[:300])
                        return None
                    try:
                        data = json.loads(body)
                    except Exception:
                        data = await resp.json(content_type=None)
                    break
            except Exception as e:
                if attempt < Config.MAX_RETRIES:
                    delay = Config.RETRY_DELAY * (2 ** attempt) + random.uniform(0, Config.API_RETRY_JITTER)
                    logger.warning("SearXNG JSON loi retry %s/%s: %s", attempt + 1, Config.MAX_RETRIES, e)
                    await asyncio.sleep(delay)
                else:
                    logger.error("SearXNG JSON het retry: %s", e)
                    return None

        if not isinstance(data, dict):
            return None

        articles = searxng_articles_from_json(data, clean_query, group, topic, start, end)
        logger.info("SearXNG JSON tool: %s -> %d ket qua", clean_query, len(articles))
        return articles


async def searxng_search_topic(
    session: aiohttp.ClientSession,
    topic: str,
    group: str,
    start: datetime,
    end: datetime,
) -> Optional[List[Dict[str, Any]]]:
    """Legacy wrapper. Flow chinh khong goi truc tiep ham nay nua."""
    query = f"{topic} latest news {end.strftime('%Y-%m-%d')}"
    return await searxng_search_query(session, query, group, topic, start, end)


def merge_unique_articles(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    seen = set()
    for art in articles:
        if not valid_article(art):
            continue
        key = art.get("url") or art.get("title") or str(art)
        h = _url_hash(str(key))
        if h in seen:
            continue
        seen.add(h)
        merged.append(art)
    return merged


async def search_topic_backends(
    session: aiohttp.ClientSession,
    topic: str,
    group: str,
    start: datetime,
    end: datetime,
) -> Optional[List[Dict[str, Any]]]:
    tasks = []
    names = []

    if TAVILY_ENABLED:
        tasks.append(tavily_search_topic(session, topic, group, start, end))
        names.append("Tavily")
    if SEARXNG_ENABLED:
        tasks.append(searxng_search_topic(session, topic, group, start, end))
        names.append("SearXNG")

    if not tasks:
        return None

    results = await asyncio.gather(*tasks, return_exceptions=True)
    articles: List[Dict[str, Any]] = []

    for name, result in zip(names, results):
        if isinstance(result, Exception):
            logger.error("%s search loi topic=%s: %s", name, topic, result)
            continue
        if isinstance(result, list):
            articles.extend(result)

    return merge_unique_articles(articles)

# =========================================================
# AGENT 1 — RESEARCHER
# =========================================================

RESEARCHER_SYSTEM = """Ban la Agent 1: autonomous research agent co quyen dung tool SearXNG.

Kien truc bat buoc:
- RESEARCH_MODEL la core reasoning model cua Agent 1, khong phai fallback.
- Backend khong tu search truoc. Backend chi thuc thi tool call khi ban yeu cau.
- Ban phai tu quyet dinh query, tu search, retry/refine query, gom ket qua, loc duplicate, danh gia nguon, tom tat, chuan hoa package.
- Chi lay tin nam trong rolling 12-hour window nguoi dung cung cap.
- Khong bia dat URL. Chi dung URL da xuat hien trong observation cua tool.
- Neu chua co observation hoac ket qua yeu, hay goi tool search truoc khi final.

Tool co san:
- searxng_search(query): tim web/news qua SearXNG.

Moi response CHI la JSON object, khong markdown, khong text thua.

De goi tool:
{
  "action": "search",
  "query": "query search cu the",
  "reason": "ly do ngan gon"
}

De ket thuc:
{
  "action": "final",
  "articles": [
    {
      "title": "Tieu de tin bang tieng Viet",
      "summary": "Tom tat 2 cau ngan gon bang tieng Viet, neu ro y nghia tin.",
      "url": "https://...",
      "source": "Ten nguon",
      "topic": "Chu de goc",
      "importance": 75,
      "source_quality": "high|medium|low",
      "why_important": "Ly do dang theo doi",
      "published_at": "neu co"
    }
  ]
}

Quy tac final:
- Chon 0-4 bai moi, noi bat, lien quan nhat.
- Deduplicate theo URL va noi dung.
- Uu tien nguon goc/bao lon/nguon chinh thuc.
- importance tu 0 den 100.
- Neu khong co tin phu hop khung gio: {"action":"final","articles":[]}."""


def parse_research_action(raw: str) -> Optional[Dict[str, Any]]:
    """Doc lenh JSON cua Agent 1. Chap nhan object moi, fallback array cu."""
    obj = extract_json_object(raw)
    if isinstance(obj, dict):
        if isinstance(obj.get("articles"), list) and not obj.get("action"):
            obj["action"] = "final"
        return obj

    arr = extract_json_array(raw)
    if isinstance(arr, list):
        return {"action": "final", "articles": arr}

    return None


def compact_tool_observations(articles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Rut gon output tool de dua lai cho Agent 1, tranh phinh context."""
    compact: List[Dict[str, Any]] = []
    for art in articles[:Config.SEARXNG_MAX_RESULTS]:
        compact.append(
            {
                "title": str(art.get("title", ""))[:220],
                "summary": str(art.get("summary", ""))[:500],
                "url": str(art.get("url", "")),
                "source": str(art.get("source", "")),
                "published_at": str(art.get("published_at", "")),
                "search_query": str(art.get("search_query", ""))[:220],
            }
        )
    return compact


def validate_research_package(
    articles: Any,
    group: str,
    topic: str,
    observed_articles: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Validate package Agent 1; chi nhan URL da thay tu SearXNG tool."""
    if not isinstance(articles, list):
        return []

    observed_by_url: Dict[str, Dict[str, Any]] = {}
    for art in observed_articles:
        if not isinstance(art, dict):
            continue
        url = str(art.get("url", "")).strip()
        if url:
            observed_by_url[_normalize_url(url)] = art

    valid: List[Dict[str, Any]] = []
    seen = set()
    for item in articles:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url", "")).strip()
        if not url:
            continue
        key = _normalize_url(url)
        observed = observed_by_url.get(key)
        if not observed or _url_hash(url) in seen:
            continue

        title = str(item.get("title") or observed.get("title") or "").strip()
        summary = str(item.get("summary") or observed.get("summary") or "").strip()
        source = str(item.get("source") or observed.get("source") or source_from_url(url)).strip()
        research_item: Dict[str, Any] = {
            "title": title,
            "summary": summary,
            "url": url,
            "source": source,
            "topic": str(item.get("topic") or topic).strip(),
            "importance": importance_from_score(item.get("importance", observed.get("importance", 50))),
            "source_quality": str(item.get("source_quality", "medium")).strip() or "medium",
            "why_important": str(item.get("why_important", "")).strip(),
            "published_at": str(item.get("published_at") or observed.get("published_at") or "").strip(),
            "_group": group,
        }
        if valid_article(research_item):
            seen.add(_url_hash(url))
            valid.append(research_item)

    return valid[:4]


async def research_topic(
    session: aiohttp.ClientSession,
    topic: str,
    group: str,
    start: datetime,
    end: datetime,
) -> List[Dict[str, Any]]:
    """Agent 1: autonomous tool-using researcher → structured research package."""
    cache_key = make_cache_key(f"research_agent1_{topic}", start, end)
    cached    = get_cache(cache_key)
    if cached:
        try:
            data = json.loads(cached)
            if isinstance(data, list):
                return data
        except Exception as e:
            logger.warning("Cache research bi hong topic=%s: %s", topic, e)

    if not RESEARCH_API_BASE or not RESEARCH_API_KEY or not RESEARCH_MODEL:
        logger.error("Agent 1 thieu RESEARCH_API_BASE/RESEARCH_API_KEY/RESEARCH_MODEL")
        return []
    if not search_tool_enabled():
        logger.error("Agent 1 can web search tool nhung WEB_SEARCH/SEARXNG chua bat")
        return []

    messages: List[Dict[str, str]] = [
        {"role": "system", "content": RESEARCHER_SYSTEM},
        {
            "role": "user",
            "content": (
                f"Chu de: {topic}\n"
                f"Nhom: {group}\n"
                f"Rolling 12-hour window gio VN: {format_range(start, end)}\n"
                f"Ngay ket thuc: {end.strftime('%Y-%m-%d')}\n"
                "Hay tu lap query va dung tool web_search. "
                "Sau khi co observation, refine/retry neu can, roi final structured research package."
            ),
        },
    ]
    observed_articles: List[Dict[str, Any]] = []
    searched_queries = set()

    async with research_semaphore:
        for step in range(Config.RESEARCH_TOOL_MAX_STEPS):
            result = await _api_call(
                session,
                RESEARCH_API_BASE,
                RESEARCH_API_KEY,
                RESEARCH_MODEL,
                messages=messages,
                max_tokens=Config.RESEARCH_MAX_TOKENS,
            )
            if not result:
                return []

            raw = result["choices"][0]["message"].get("content", "")
            messages.append({"role": "assistant", "content": raw})
            action = parse_research_action(raw)
            if not isinstance(action, dict):
                messages.append(
                    {
                        "role": "user",
                        "content": "JSON khong hop le. Hay tra ve object action=search hoac action=final dung schema.",
                    }
                )
                continue

            action_name = str(action.get("action", "")).strip().lower()
            if action_name == "search":
                query = re.sub(r"\s+", " ", str(action.get("query", "")).strip())[:300]
                if not query:
                    messages.append({"role": "user", "content": "Query rong. Hay tao query search cu the hon."})
                    continue
                if query.lower() in searched_queries:
                    messages.append(
                        {
                            "role": "user",
                            "content": f"Query da search roi: {query}. Hay refine query khac hoac final neu du du lieu.",
                        }
                    )
                    continue

                searched_queries.add(query.lower())
                tool_articles = await searxng_search_query(session, query, group, topic, start, end)
                if isinstance(tool_articles, list):
                    observed_articles.extend(tool_articles)
                    observed_articles = merge_unique_articles(observed_articles)
                    observation = {
                        "tool": "web_search",
                        "query": query,
                        "window": format_range(start, end),
                        "result_count": len(tool_articles),
                        "results": compact_tool_observations(tool_articles),
                    }
                else:
                    observation = {
                        "tool": "web_search",
                        "query": query,
                        "window": format_range(start, end),
                        "error": "Web search tool failed or disabled",
                        "results": [],
                    }

                messages.append(
                    {
                        "role": "user",
                        "content": "Tool observation JSON:\n" + json.dumps(observation, ensure_ascii=False),
                    }
                )
                continue

            if action_name == "final":
                valid = validate_research_package(action.get("articles"), group, topic, observed_articles)
                if valid:
                    set_cache(cache_key, json.dumps(valid, ensure_ascii=False))
                logger.info("Agent 1 tool researcher: %s -> %d bai sau %d buoc", topic, len(valid), step + 1)
                return valid

            messages.append(
                {
                    "role": "user",
                    "content": "Action khong hop le. Chi dung action=search hoac action=final.",
                }
            )

        if observed_articles:
            messages.append(
                {
                    "role": "user",
                    "content": (
                        "Da het so buoc tool. Hay final ngay, chi dung URL trong observations. "
                        "Tra ve action=final."
                    ),
                }
            )
            result = await _api_call(
                session,
                RESEARCH_API_BASE,
                RESEARCH_API_KEY,
                RESEARCH_MODEL,
                messages=messages,
                max_tokens=Config.RESEARCH_MAX_TOKENS,
            )
            if result:
                raw = result["choices"][0]["message"].get("content", "")
                action = parse_research_action(raw)
                if isinstance(action, dict):
                    valid = validate_research_package(action.get("articles"), group, topic, observed_articles)
                    if valid:
                        set_cache(cache_key, json.dumps(valid, ensure_ascii=False))
                    logger.info("Agent 1 tool researcher final forced: %s -> %d bai", topic, len(valid))
                    return valid

    return []

# =========================================================
# AGENT 2 — JUDGE / EDITOR
# =========================================================

EDITOR_SYSTEM = """Ban la Tong bien tap tin tuc AI.

Nhiem vu:
- Doc danh sach bai bao tho tu nhieu chu de (truong _group cho biet nhom)
- Danh gia tung bai theo 4 tieu chi: viral(0-100), impact(0-100), interesting(0-100), trustworthy(0-100)
- Tinh final_score = (viral*0.3 + impact*0.3 + interesting*0.25 + trustworthy*0.15)
- Loai bo: tin trung noi dung, clickbait, spam, quang cao, tin co final_score < 40
- Chon top 3 tin tot nhat moi nhom (dua theo truong _group)
- GIU NGUYEN url, title tu input (tuyet doi khong bia dat URL)
- summary co the viet lai ngan gon hon bang tieng Viet

Output CHI la JSON object, tuyet doi khong co text thua ben ngoai:
{
  "groups": {
    "Ten nhom": [
      {
        "title": "...",
        "summary": "Tom tat 2-3 cau tieng Viet.",
        "url": "https://...",
        "source": "...",
        "final_score": 87,
        "tags": ["Viral", "AI"]
      }
    ]
  },
  "highlight": "Mo ta 1 cau ve tin noi bat nhat toan bo digest."
}

Tags goi y (chon 1-3): Viral, AI, Canh bao, Moi, Game, Tai chinh, The gioi, Viet Nam, Suc khoe, Phim"""


def group_articles_for_editor(raw_articles: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Gom bài theo nhóm để Agent 2 chạy nhiều request song song."""
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for art in raw_articles:
        if not isinstance(art, dict):
            continue
        group_name = str(
            art.get("_group") or art.get("group") or art.get("topic") or "Khac"
        ).strip() or "Khac"
        grouped.setdefault(group_name, []).append(art)
    return grouped


async def judge_article_group(
    session: aiohttp.ClientSession,
    group_name: str,
    articles: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Agent 2: judge riêng 1 nhóm tin, dùng cho đa request/response."""
    if not articles:
        return None

    async with editor_semaphore:
        result = await _api_call(
            session,
            EDITOR_API_BASE,
            EDITOR_API_KEY,
            EDITOR_MODEL,
            messages=[
                {"role": "system", "content": EDITOR_SYSTEM},
                {
                    "role": "user",
                    "content": (
                        f"Chi danh gia nhom: {group_name}\n"
                        f"Co {len(articles)} bai bao tho duoi day.\n"
                        "Hay danh gia, loc va tra ve JSON chuan. "
                        "Truong groups chi gom dung nhom nay:\n\n"
                        + json.dumps(articles, ensure_ascii=False)
                    ),
                },
            ],
            max_tokens=Config.EDITOR_MAX_TOKENS,
        )

    if not result:
        return None

    raw    = result["choices"][0]["message"]["content"]
    judged = extract_json_object(raw)  # FIX E

    if not isinstance(judged, dict) or "groups" not in judged:
        logger.error("Judge nhom %s tra ve sai schema. Raw: %s", group_name, raw[:300])
        return None

    groups = judged.get("groups", {})
    if not isinstance(groups, dict):
        return None

    selected = groups.get(group_name)
    if selected is None and groups:
        selected = next(iter(groups.values()))
    if not isinstance(selected, list):
        return None

    return {
        "groups": {group_name: selected},
        "highlight": str(judged.get("highlight", "")).strip(),
    }


async def judge_news(
    session: aiohttp.ClientSession,
    raw_articles: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Agent 2: đa request/response theo từng nhóm tin → merge ranked JSON."""
    if not raw_articles:
        return None

    grouped = group_articles_for_editor(raw_articles)
    if not grouped:
        return None

    group_names = list(grouped.keys())
    tasks = [judge_article_group(session, name, grouped[name]) for name in group_names]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    merged_groups: Dict[str, List[Dict[str, Any]]] = {}
    highlights: List[str] = []

    for group_name, result in zip(group_names, results):
        if isinstance(result, Exception):
            logger.error("Judge nhom %s loi: %s", group_name, result)
            continue
        if not isinstance(result, dict):
            continue

        groups = result.get("groups", {})
        articles = groups.get(group_name, []) if isinstance(groups, dict) else []
        if isinstance(articles, list) and articles:
            merged_groups[group_name] = articles
            highlight = str(result.get("highlight", "")).strip()
            if highlight:
                highlights.append(highlight)

    if not merged_groups:
        return None

    logger.info("Agent 2: judge song song %d/%d nhom", len(merged_groups), len(grouped))
    return {
        "groups": merged_groups,
        "highlight": highlights[0] if highlights else "Tin noi bat da duoc chon loc tu cac nhom.",
    }

# =========================================================
# FULL PIPELINE: collect → filter → judge
# =========================================================

async def collect_news(start: datetime, end: datetime) -> List[Dict[str, Any]]:
    """Agent 1: chạy song song tất cả topics."""
    connector = aiohttp.TCPConnector(   # FIX I: bỏ ssl=False
        limit=Config.TCP_LIMIT,
        ttl_dns_cache=Config.DNS_CACHE,
    )
    async with aiohttp.ClientSession(connector=connector) as session:
        flat    = [(g, t) for g, topics in TOPIC_GROUPS.items() for t in topics]
        tasks   = [research_topic(session, t, g, start, end) for g, t in flat]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    all_articles: List[Dict[str, Any]] = []
    for res in results:
        if isinstance(res, list):
            all_articles.extend(res)

    logger.info("Agent 1: thu thap %d bai tu %d topics", len(all_articles), len(flat))
    return all_articles


async def run_pipeline(start: datetime, end: datetime) -> Optional[Dict[str, Any]]:
    """Collect → anti-dup filter → judge."""
    raw_articles = await collect_news(start, end)
    if not raw_articles:
        return None

    async with sent_urls_lock:
        # Lock giữ trọn read → filter/judge → write, tránh lost update giữa scheduler và !force/!news.
        sent_list = load_sent_urls()
        saved_sent_set = set(sent_list)
        seen_set = set(saved_sent_set)

        new_articles = filter_new_articles(raw_articles, seen_set)
        logger.info("Bai moi chua gui: %d / %d", len(new_articles), len(raw_articles))

        if not new_articles:
            logger.info("Tat ca tin da duoc gui truoc do.")
            return None

        connector = aiohttp.TCPConnector(limit=20)  # FIX I
        async with aiohttp.ClientSession(connector=connector) as session:
            judged = await judge_news(session, new_articles)

        if judged:
            # FIX C: chỉ lưu hash bài thật sự được judge chọn
            new_hashes = extract_sent_hashes_from_judged(judged)
            appended = 0
            # FIX D+8: append có kiểm soát, không duplicate trong batch.
            for h in new_hashes:
                if h not in saved_sent_set:
                    sent_list.append(h)
                    saved_sent_set.add(h)
                    appended += 1
            save_sent_urls(sent_list)
            logger.info("Luu %d/%d hash moi vao sent_urls", appended, len(new_hashes))

        return judged

# =========================================================
# SPLIT MESSAGE
# =========================================================

def _inside_span(spans: List[tuple], idx: int) -> bool:
    return any(start < idx < end for start, end in spans)

def _markdown_cut_is_safe(text: str, idx: int) -> bool:
    spans = [(m.start(), m.end()) for m in re.finditer(r"https?://\S+", text)]
    spans.extend((m.start(), m.end()) for m in re.finditer(r"\[[^\]\n]+\]\([^)]+\)", text))
    if _inside_span(spans, idx):
        return False
    if 0 < idx < len(text) and text[idx - 1] == "*" and text[idx] == "*":
        return False
    prefix = text[:idx]
    return prefix.count("**") % 2 == 0 and prefix.count("`") % 2 == 0

def _safe_split_index(text: str, max_len: int) -> int:
    candidates = set()
    for sep in ("\n\n", "\n", " "):
        pos = text.rfind(sep, 0, max_len + 1)
        while pos > 0:
            candidates.add(pos + len(sep))
            pos = text.rfind(sep, 0, pos)
    candidates.add(max_len)

    for idx in sorted(candidates, reverse=True):
        if 0 < idx <= max_len and _markdown_cut_is_safe(text, idx):
            return idx

    logger.warning("Phai cat message tai %d ky tu; khong tim duoc diem markdown-safe", max_len)
    return max_len

def _split_long_block(block: str, max_len: int) -> List[str]:
    chunks: List[str] = []
    rest = block
    while len(rest) > max_len:
        cut = _safe_split_index(rest, max_len)
        chunks.append(rest[:cut].rstrip())
        rest = rest[cut:].lstrip()
    if rest:
        chunks.append(rest)
    return chunks

def split_message(text: str, max_len: int = Config.MAX_PLAIN_TEXT) -> List[str]:
    if max_len <= 0 or len(text) <= max_len:
        return [text]

    chunks: List[str] = []
    current = ""
    for block in text.split("\n\n"):
        candidate = block if not current else current + "\n\n" + block
        if len(candidate) <= max_len:
            current = candidate
            continue

        if current:
            chunks.append(current)
            current = ""

        if len(block) > max_len:
            chunks.extend(_split_long_block(block, max_len))
        else:
            current = block

    if current:
        chunks.append(current)
    return chunks

# =========================================================
# DISCORD RENDERER  (FIX H: safe_text)
# Python render từ JSON — không để AI tự viết markdown Discord
# =========================================================

DISPLAY_ICON_RE = re.compile(
    "["
    "\U0001F300-\U0001FAFF"
    "\U00002700-\U000027BF"
    "\U00002600-\U000026FF"
    "]+",
    flags=re.UNICODE,
)


def clean_display_label(value: Any) -> str:
    """Bỏ emoji/icon trang trí khỏi text hiển thị Discord."""
    text = DISPLAY_ICON_RE.sub("", safe_text(value))
    return re.sub(r"\s{2,}", " ", text).strip(" -|•")


def render_tag(tag: str) -> str:
    return clean_display_label(tag)


def render_article(art: Dict[str, Any], rank: int) -> str:
    title   = safe_text(art.get("title",   "Khong co tieu de"))
    summary = safe_text(art.get("summary", ""))
    url     = str(art.get("url", "")).strip()
    source  = safe_text(art.get("source",  ""))
    score   = art.get("final_score", 0)
    tags    = art.get("tags", [])

    lines = [f"**{rank}. {title}**"]
    clean_tags = [render_tag(t) for t in tags if str(t).strip()]
    if clean_tags:
        lines.append(f"Phân loại: {', '.join(clean_tags[:3])}")
    if summary:
        lines.append(summary)
    meta = []
    if source:
        meta.append(f"Nguồn: _{source}_")
    if score:
        meta.append(f"Điểm: {int(score)}")
    if meta:
        lines.append(" • ".join(meta))
    if url:
        lines.append(compact_markdown_link(url))

    return "\n".join(lines)


async def send_ranked_digest(
    channel: discord.abc.Messageable,
    start: datetime,
    end: datetime,
) -> None:
    judged       = await run_pipeline(start, end)
    total_topics = sum(len(v) for v in TOPIC_GROUPS.values())

    if not judged:
        await channel.send(
            embed=discord.Embed(
                title="Tin mới",
                description=(
                    f"**{format_range(start, end)}**\n"
                    "Khong tim thay tin moi trong khung thoi gian nay."
                ),
                color=0xAAAAAA,
                timestamp=datetime.now(timezone.utc),
            ),
            allowed_mentions=NO_MENTIONS,
        )
        return

    groups    = judged.get("groups", {})
    highlight = safe_text(judged.get("highlight", ""))
    n_groups  = len(groups)
    n_arts    = sum(len(v) for v in groups.values() if isinstance(v, list))

    header = discord.Embed(
        title="Tin mới tổng hợp",
        description=(
            f"**{format_range(start, end)}**\n"
            f"{n_groups} nhóm • {n_arts} tin chọn lọc"
            + (f"\n\n_{highlight}_" if highlight else "")
        ),
        color=0x2F80ED,
        timestamp=datetime.now(timezone.utc),
    )
    header.set_footer(
        text=f"Multi-Agent | {total_topics} topics | 06:00 & 18:00 VN | Anti-dup ON"
    )
    await channel.send(embed=header, allowed_mentions=NO_MENTIONS)
    await asyncio.sleep(0.8)

    for group_name, articles in groups.items():
        if not isinstance(articles, list) or not articles:
            continue

        await channel.send(
            embed=discord.Embed(
                title=clean_display_label(group_name) or "Tin mới",
                color=GROUP_COLORS.get(group_name, 0xAAAAAA),
            ),
            allowed_mentions=NO_MENTIONS,
        )

        rendered  = [render_article(art, i + 1) for i, art in enumerate(articles)]
        full_text = "\n\n".join(rendered)

        for chunk in split_message(full_text):
            await channel.send(
                chunk,
                allowed_mentions=NO_MENTIONS,
                suppress_embeds=True,
            )
            await asyncio.sleep(Config.DISCORD_DELAY)

        await asyncio.sleep(0.5)

    logger.info("Gui xong: %d nhom, %d tin | %s", n_groups, n_arts, format_range(start, end))

# =========================================================
# CHANNEL RESOLVE
# =========================================================

async def get_target_channel() -> Optional[discord.abc.Messageable]:
    channel = bot.get_channel(CHANNEL_ID)
    if channel:
        return channel
    try:
        return await bot.fetch_channel(CHANNEL_ID)
    except Exception as e:
        logger.error("Khong lay duoc channel %s: %s", CHANNEL_ID, e)
        return None

# =========================================================
# SCHEDULER  (FIX A + B + catch-up + retry)
# FIX B: chờ mốc tiếp theo TRƯỚC, không gửi ngay khi restart
# =========================================================

async def load_state_locked() -> None:
    async with state_lock:
        load_state()

async def get_last_sent_key_locked() -> str:
    async with state_lock:
        load_state()
        return last_sent_key

async def get_last_processed_key_locked() -> str:
    async with state_lock:
        load_state()
        return last_processed_key or last_sent_key

async def mark_slot_sent(key: str) -> None:
    global last_sent_key, last_processed_key
    async with state_lock:
        last_sent_key = key
        last_processed_key = key
        save_state()

async def mark_slot_skipped(key: str) -> None:
    global last_processed_key
    async with state_lock:
        if not last_processed_key or key > last_processed_key:
            last_processed_key = key
        save_state()

async def process_due_slots(channel: discord.abc.Messageable) -> None:
    while not bot.is_closed():
        processed_key = await get_last_processed_key_locked()
        due_slots = pending_slot_times(processed_key)
        if not due_slots:
            return

        slot_time = due_slots[0]
        key = slot_key(slot_time)
        success = False
        max_attempts = max(1, Config.SCHEDULER_SLOT_MAX_RETRIES)

        for attempt in range(1, max_attempts + 1):
            if bot.is_closed():
                return

            async with digest_lock:
                latest_processed_key = await get_last_processed_key_locked()
                if latest_processed_key and key <= latest_processed_key:
                    success = True
                    break

                start = slot_time - timedelta(hours=Config.SEARCH_INTERVAL_HOURS)
                logger.info(
                    "Tu dong gui moc %s lan %d/%d | %s",
                    key, attempt, max_attempts, format_range(start, slot_time),
                )
                try:
                    await send_ranked_digest(channel, start, slot_time)
                    await mark_slot_sent(key)
                    success = True
                    break
                except Exception as e:
                    logger.exception("Loi gui digest moc %s lan %d/%d: %s", key, attempt, max_attempts, e)

            if attempt < max_attempts:
                logger.warning("Retry moc %s sau %ss", key, Config.SCHEDULER_RETRY_DELAY)
                await asyncio.sleep(Config.SCHEDULER_RETRY_DELAY)

        if success:
            continue

        logger.error("Bo qua moc %s sau %d lan loi; chuyen sang moc tiep theo", key, max_attempts)
        await mark_slot_skipped(key)

async def auto_scheduler() -> None:
    await bot.wait_until_ready()
    await load_state_locked()

    while not bot.is_closed():
        try:
            channel = await get_target_channel()
            if not channel:
                logger.error("Scheduler khong co channel; retry sau %ss", Config.BACKGROUND_TASK_RESTART_DELAY)
                await asyncio.sleep(Config.BACKGROUND_TASK_RESTART_DELAY)
                continue
            await wait_until_next_slot()
            await process_due_slots(channel)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Auto scheduler loop loi")
            await asyncio.sleep(Config.BACKGROUND_TASK_RESTART_DELAY)

async def supervised_background_task(name: str, coro_factory) -> None:
    while not bot.is_closed():
        try:
            await coro_factory()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Background task %s chet; restart sau %ss",
                name,
                Config.BACKGROUND_TASK_RESTART_DELAY,
            )
        if not bot.is_closed():
            await asyncio.sleep(Config.BACKGROUND_TASK_RESTART_DELAY)

# =========================================================
# COMMANDS
# =========================================================

@bot.command(name="news", aliases=["tin"])
async def cmd_news(ctx: commands.Context):
    if ctx.channel.id != CHANNEL_ID:
        await ctx.send(f"Chi hoat dong trong kenh <#{CHANNEL_ID}>", allowed_mentions=NO_MENTIONS)
        return
    async with ctx.typing():
        async with digest_lock:
            now   = vn_now()
            start = now - timedelta(hours=Config.SEARCH_INTERVAL_HOURS)
            await send_ranked_digest(ctx.channel, start, now)


@bot.command(name="ping")
async def cmd_ping(ctx: commands.Context):
    await ctx.send(f"Pong! `{round(bot.latency * 1000)}ms`", allowed_mentions=NO_MENTIONS)


@bot.command(name="status")
async def cmd_status(ctx: commands.Context):
    vn   = vn_now()
    nxt  = next_slot_time()
    wait = int((nxt - vn).total_seconds() // 60)

    embed = discord.Embed(title="Bot Status", color=0x00FF88, timestamp=datetime.now(timezone.utc))
    embed.add_field(name="Gio VN",         value=vn.strftime("%H:%M:%S %d/%m/%Y"),             inline=False)
    embed.add_field(name="Moc hien tai",   value=f"{current_slot()}:00",                       inline=True)
    embed.add_field(name="Moc tiep theo",  value=f"{nxt.strftime('%H:%M')} (~{wait}p)",        inline=True)
    saved_key = await get_last_sent_key_locked()
    async with sent_urls_lock:
        sent_count = len(load_sent_urls())
    embed.add_field(name="Da gui moc",     value=saved_key or "Chua co",                       inline=True)  # FIX A
    embed.add_field(name="Nhom/Chu de",    value=f"{len(TOPIC_GROUPS)} / {sum(len(v) for v in TOPIC_GROUPS.values())}", inline=True)
    embed.add_field(name="URLs da nho",    value=f"{sent_count} bai",                          inline=True)
    embed.add_field(name="Research Parallel", value=str(Config.MAX_PARALLEL_RESEARCH),         inline=True)
    embed.add_field(name="Editor Parallel",   value=str(Config.MAX_PARALLEL_EDITOR),           inline=True)
    embed.add_field(name="Researcher",     value=f"`{RESEARCH_MODEL}`",                        inline=True)
    embed.add_field(name="Editor",         value=f"`{EDITOR_MODEL}`",                          inline=True)
    embed.add_field(name="Research API",   value=RESEARCH_API_BASE or "Chua dat",              inline=False)
    embed.add_field(name="Agent 1 Flow",   value="Tool-using researcher -> Web Search",        inline=False)
    embed.add_field(name="Web Search",     value="ON" if WEB_SEARCH_ENABLED else "OFF",        inline=True)
    embed.add_field(name="SearXNG Fallback", value="ON" if SEARXNG_ENABLED else "OFF",         inline=True)
    embed.add_field(name="Tavily",         value="Legacy" if TAVILY_ENABLED else "OFF",        inline=True)
    await ctx.send(embed=embed, allowed_mentions=NO_MENTIONS)


@bot.command(name="force", aliases=["forcenews"])
async def cmd_force(ctx: commands.Context):
    """Gửi ngay không chờ mốc — dùng để test."""
    if ctx.channel.id != CHANNEL_ID:
        await ctx.send(f"Chi hoat dong trong kenh <#{CHANNEL_ID}>", allowed_mentions=NO_MENTIONS)
        return
    async with ctx.typing():
        async with digest_lock:
            now   = vn_now()
            start = now - timedelta(hours=Config.SEARCH_INTERVAL_HOURS)
            await send_ranked_digest(ctx.channel, start, now)
            await mark_slot_sent(slot_key(now))   # FIX A


@bot.command(name="clearmem", aliases=["clear"])
async def cmd_clearmem(ctx: commands.Context):
    """Xóa memory anti-duplicate."""
    if ctx.channel.id != CHANNEL_ID:
        await ctx.send(f"Chi hoat dong trong kenh <#{CHANNEL_ID}>", allowed_mentions=NO_MENTIONS)
        return
    try:
        async with sent_urls_lock:
            save_sent_urls([])
        await ctx.send("Da xoa memory anti-duplicate. Bot se tim lai tin tu dau.", allowed_mentions=NO_MENTIONS)
    except Exception as e:
        await ctx.send(f"Loi: {e}", allowed_mentions=NO_MENTIONS)

# =========================================================
# EVENTS
# =========================================================

@bot.event
async def on_ready():
    global scheduler_started, cache_cleanup_started
    total = sum(len(v) for v in TOPIC_GROUPS.values())
    logger.info("Bot online: %s | %s nhom | %s chu de", bot.user, len(TOPIC_GROUPS), total)
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name=f"{total} topics | 06:00 & 18:00 VN",
        )
    )
    if not scheduler_started:
        scheduler_started = True
        asyncio.create_task(supervised_background_task("auto_scheduler", auto_scheduler))   # FIX K
        logger.info("Auto scheduler started")
    if not cache_cleanup_started:
        cache_cleanup_started = True
        asyncio.create_task(supervised_background_task("cache_cleanup_loop", cache_cleanup_loop))
        logger.info("Cache cleanup started")


@bot.event
async def on_command_error(ctx: commands.Context, error: Exception):
    if isinstance(error, commands.CommandNotFound):
        await ctx.send("Lenh khong ton tai. Dung: `!news` `!ping` `!status` `!force` `!clearmem`", allowed_mentions=NO_MENTIONS)
    else:
        logger.error("Command error: %s", error)
        await ctx.send(f"Loi: {str(error)[:200]}", allowed_mentions=NO_MENTIONS)

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("Thieu DISCORD_TOKEN trong .env")
    if not CHANNEL_ID:
        raise SystemExit("Thieu CHANNEL_ID trong .env")
    if not RESEARCH_API_BASE:
        raise SystemExit("Thieu RESEARCH_API_BASE (hoac API_BASE) trong .env")
    if not RESEARCH_API_KEY:
        raise SystemExit("Thieu RESEARCH_API_KEY (hoac API_KEY) trong .env")
    if not RESEARCH_MODEL:
        raise SystemExit("Thieu RESEARCH_MODEL (hoac MODEL_NAME) trong .env")
    if not search_tool_enabled():
        raise SystemExit("Thieu WEB_SEARCH_API_BASE/WEB_SEARCH_API_KEY hoac SEARXNG_BASE_URL; Agent 1 can web search tool")
    if not EDITOR_API_BASE:   # FIX J
        raise SystemExit("Thieu EDITOR_API_BASE (hoac API_BASE) trong .env")
    if not EDITOR_API_KEY:    # FIX J
        raise SystemExit("Thieu EDITOR_API_KEY (hoac API_KEY) trong .env")

    logger.info("Agent 1 flow: RESEARCH_MODEL -> web_search tool -> structured package")
    logger.info("Web search compatible endpoint: %s", "ON" if WEB_SEARCH_ENABLED else "OFF")
    logger.info("Tavily legacy backend: %s", "ON" if TAVILY_ENABLED else "OFF")
    logger.info("SearXNG fallback: %s", "ON" if SEARXNG_ENABLED else "OFF")
    logger.info("Starting Multi-Agent News Bot...")
    bot.run(DISCORD_TOKEN)
