import asyncio
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

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
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0") or 0)

API_BASE = os.getenv("API_BASE", "").rstrip("/")
API_KEY = os.getenv("API_KEY", "").strip()
MODEL_NAME = os.getenv("MODEL_NAME", "gpt-4o-mini").strip()

# =========================================================
# CONFIG
# =========================================================

class Config:
    SEARCH_INTERVAL_HOURS = 6
    MAX_RETRIES = 5
    RETRY_DELAY = 2
    REQUEST_TIMEOUT = 180

    MAX_PARALLEL_REQUESTS = 20
    MAX_DISCORD_LENGTH = 1900
    DISCORD_DELAY = 0.4

    TIMEZONE_OFFSET = 7
    CACHE_EXPIRE = 3600

    TCP_LIMIT = 100
    DNS_CACHE = 300

    MAX_TOKENS = 1800
    STATE_FILE = "bot_state.json"
    SLOTS = [0, 6, 12, 18]

# =========================================================
# TOPICS
# =========================================================

TOPIC_GROUPS: Dict[str, List[str]] = {
    "🤖 AI & Công Nghệ": [
        "AI news worldwide",
        "OpenAI news",
        "ChatGPT updates",
        "Claude AI",
        "Gemini AI",
        "DeepSeek AI",
        "AI agents",
        "AI automation",
        "machine learning",
        "LLM breakthrough",
        "robotics AI",
        "cybersecurity AI",
        "NVIDIA AI",
        "AMD AI",
        "Intel AI",
        "Apple news",
        "Google news",
        "Microsoft news",
        "Samsung technology",
        "Linux news",
        "Windows update",
        "cloud computing",
        "datacenter technology",
        "quantum computing",
        "SpaceX news",
        "NASA news",
        "science breakthrough",
        "Việt Nam AI",
        "Việt Nam công nghệ",
        "Việt Nam startup công nghệ",
        "Việt Nam an ninh mạng",
    ],
    "🎮 Gaming": [
        "gaming news",
        "Steam game news",
        "PlayStation news",
        "Xbox news",
        "Nintendo news",
        "gacha game news",
        "Wuthering Waves news",
        "Genshin Impact news",
        "Honkai Star Rail news",
        "Esports news",
        "VCS LMHT",
    ],
    "📈 Crypto & Finance": [
        "Bitcoin news",
        "Ethereum news",
        "crypto market",
        "DeFi news",
        "VN-Index",
        "stock market news",
        "global economy",
        "gold price",
        "USD exchange rate",
    ],
    "⚽ Sports": [
        "football news",
        "Premier League",
        "Champions League",
        "transfer news",
        "Vietnam football",
        "Esports tournament",
    ],
    "🎬 Entertainment": [
        "Netflix releases",
        "Hollywood news",
        "Disney movie news",
        "celebrity news",
        "KDrama news",
        "Kpop news",
        "music industry news",
    ],
    "⛩️ Anime & Manga": [
        "anime news",
        "manga news",
        "One Piece news",
        "Jujutsu Kaisen news",
        "anime adaptation",
        "light novel adaptation",
        "Japanese anime industry",
    ],
    "🏥 Health": [
        "health news",
        "medical breakthrough",
        "virus outbreak",
        "fitness trend",
        "nutrition research",
        "longevity research",
    ],
    "🇻🇳 Việt Nam": [
        "Việt Nam kinh tế",
        "Việt Nam giáo dục",
        "Việt Nam pháp luật",
        "Việt Nam môi trường",
        "Việt Nam giao thông",
        "Việt Nam y tế",
        "Hà Nội tin mới",
        "TP HCM tin mới",
    ],
}

GROUP_COLORS: Dict[str, int] = {
    "🤖 AI & Công Nghệ": 0x00CFFF,
    "🎮 Gaming": 0x9966FF,
    "📈 Crypto & Finance": 0xF7D000,
    "⚽ Sports": 0x44FF88,
    "🎬 Entertainment": 0xFF9900,
    "⛩️ Anime & Manga": 0xFF69B4,
    "🏥 Health": 0x88FFCC,
    "🇻🇳 Việt Nam": 0xFF4444,
}

# =========================================================
# DISCORD
# =========================================================

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# =========================================================
# GLOBALS
# =========================================================

CACHE: Dict[str, Dict[str, Any]] = {}
semaphore = asyncio.Semaphore(Config.MAX_PARALLEL_REQUESTS)
last_sent_slot = -1
scheduler_started = False

# =========================================================
# TIME HELPERS
# =========================================================

def vn_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=Config.TIMEZONE_OFFSET)

def format_range(start: datetime, end: datetime) -> str:
    return f"{start.strftime('%H:%M %d/%m/%Y')} → {end.strftime('%H:%M %d/%m/%Y')}"

def current_slot() -> int:
    hour = vn_now().hour
    for slot in reversed(Config.SLOTS):
        if hour >= slot:
            return slot
    return 18

def next_slot_time() -> datetime:
    now = vn_now()
    cur = current_slot()
    for slot in Config.SLOTS:
        if slot > cur:
            return now.replace(hour=slot, minute=0, second=0, microsecond=0)
    return (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

async def wait_until_next_slot() -> None:
    nxt = next_slot_time()
    seconds = max((nxt - vn_now()).total_seconds(), 0)
    logger.info("Waiting %.0fs", seconds)
    await asyncio.sleep(seconds)

# =========================================================
# STATE
# =========================================================

def load_state() -> None:
    global last_sent_slot
    try:
        with open(Config.STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            last_sent_slot = int(data.get("last_sent_slot", -1))
    except Exception:
        last_sent_slot = -1

def save_state() -> None:
    try:
        with open(Config.STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"last_sent_slot": last_sent_slot}, f)
    except Exception as e:
        logger.error("Lỗi lưu state: %s", e)

# =========================================================
# CACHE
# =========================================================

def make_cache_key(topic: str, start: datetime, end: datetime) -> str:
    raw = f"{topic}_{start.isoformat()}_{end.isoformat()}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()

def get_cache(key: str) -> Optional[str]:
    item = CACHE.get(key)
    if not item:
        return None
    if time.time() - float(item["time"]) > Config.CACHE_EXPIRE:
        CACHE.pop(key, None)
        return None
    return str(item["data"])

def set_cache(key: str, value: str) -> None:
    CACHE[key] = {"time": time.time(), "data": value}

# =========================================================
# RESPONSE PARSER
# =========================================================

async def parse_openai_response(resp: aiohttp.ClientResponse) -> Dict[str, Any]:
    content_type = resp.headers.get("Content-Type", "").lower()
    logger.info("Content-Type: %s", content_type)

    if "application/json" in content_type:
        return await resp.json()

    if "text/event-stream" in content_type:
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
                chunk = json.loads(data_str)
                choices = chunk.get("choices", [])
                if not choices:
                    continue
                choice = choices[0]
                delta = choice.get("delta", {}).get("content")
                if delta is None:
                    delta = choice.get("message", {}).get("content", "")
                if delta:
                    full_text += delta
            except Exception:
                continue

        return {"choices": [{"message": {"content": full_text}}]}

    text = await resp.text()
    raise RuntimeError(f"Unsupported Content-Type: {content_type}\n{text[:300]}")

# =========================================================
# API CALL
# =========================================================

async def make_api_call(
    session: aiohttp.ClientSession,
    messages: List[Dict[str, str]],
    retry: int = 0,
) -> Optional[Dict[str, Any]]:
    async with semaphore:
        try:
            payload = {
                "model": MODEL_NAME,
                "messages": messages,
                "temperature": 0.4,
                "max_tokens": Config.MAX_TOKENS,
                "stream": False,
            }

            async with session.post(
                f"{API_BASE}/chat/completions",
                headers={
                    "Authorization": f"Bearer {API_KEY}",
                    "Content-Type": "application/json",
                },
                json=payload,
                timeout=aiohttp.ClientTimeout(
                    total=Config.REQUEST_TIMEOUT,
                    sock_read=Config.REQUEST_TIMEOUT,
                ),
            ) as resp:
                if resp.status in {429, 500, 502, 503, 504}:
                    if retry < Config.MAX_RETRIES:
                        delay = Config.RETRY_DELAY * (2**retry)
                        logger.warning(
                            "Retry %s/%s sau %.1fs — status %s",
                            retry + 1,
                            Config.MAX_RETRIES,
                            delay,
                            resp.status,
                        )
                        await asyncio.sleep(delay)
                        return await make_api_call(session, messages, retry + 1)
                    logger.error("Hết retry sau %s lần (status %s)", Config.MAX_RETRIES, resp.status)
                    return None

                if resp.status != 200:
                    logger.error("API Error %s: %s", resp.status, await resp.text())
                    return None

                return await parse_openai_response(resp)

        except asyncio.TimeoutError:
            logger.warning("Timeout retry %s/%s", retry + 1, Config.MAX_RETRIES)
            if retry < Config.MAX_RETRIES:
                await asyncio.sleep(Config.RETRY_DELAY * (2**retry))
                return await make_api_call(session, messages, retry + 1)
            return None
        except Exception as e:
            logger.error("API Error: %s", e)
            if retry < Config.MAX_RETRIES:
                await asyncio.sleep(Config.RETRY_DELAY * (2**retry))
                return await make_api_call(session, messages, retry + 1)
            return None

# =========================================================
# TOPIC SEARCH
# =========================================================

async def search_topic(
    session: aiohttp.ClientSession,
    topic: str,
    start: datetime,
    end: datetime,
) -> str:
    cache_key = make_cache_key(topic, start, end)
    cached = get_cache(cache_key)
    if cached:
        return cached

    time_range = format_range(start, end)

    messages = [
        {
            "role": "system",
            "content": (
                "Bạn là AI tổng hợp tin tức. Trả lời tiếng Việt. "
                "Chỉ nêu 2-3 tin nổi bật nhất, ngắn gọn, rõ ràng, markdown đẹp."
            ),
        },
        {
            "role": "user",
            "content": f"""
Tìm tin mới nhất về chủ đề: {topic}

Khung thời gian (giờ Việt Nam): {time_range}

Yêu cầu:
- 2-3 tin nổi bật
- Mỗi tin gồm: tiêu đề, tóm tắt 2 câu, link nguồn
- Nếu không có tin phù hợp, trả về đúng chuỗi: KHÔNG CÓ TIN
""".strip(),
        },
    ]

    result = await make_api_call(session, messages)
    if not result:
        return ""

    try:
        content = result["choices"][0]["message"]["content"].strip()
    except Exception:
        return ""

    if "KHÔNG CÓ TIN" in content or len(content) < 50:
        return ""

    set_cache(cache_key, content)
    return content

# =========================================================
# BUILD DIGEST
# =========================================================

async def build_group_digest(
    group_name: str,
    topics: List[str],
    start: datetime,
    end: datetime,
    session: aiohttp.ClientSession,
) -> Dict[str, str]:
    tasks = [search_topic(session, topic, start, end) for topic in topics]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    valid: List[str] = []
    seen = set()

    for r in results:
        if not isinstance(r, str) or len(r) < 50:
            continue
        key = r[:120].lower()
        if key in seen:
            continue
        seen.add(key)
        valid.append(r)

    return {
        "group": group_name,
        "content": "\n\n---\n\n".join(valid),
    }

async def build_all_digest(start: datetime, end: datetime) -> List[Dict[str, str]]:
    connector = aiohttp.TCPConnector(
        limit=Config.TCP_LIMIT,
        ttl_dns_cache=Config.DNS_CACHE,
        ssl=False,
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            build_group_digest(group, topics, start, end, session)
            for group, topics in TOPIC_GROUPS.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    final: List[Dict[str, str]] = []
    for r in results:
        if isinstance(r, dict) and r.get("content"):
            final.append(r)
    return final

# =========================================================
# MESSAGE SPLIT
# =========================================================

def split_message(text: str, max_len: int = Config.MAX_DISCORD_LENGTH) -> List[str]:
    if len(text) <= max_len:
        return [text]

    chunks: List[str] = []
    current = ""

    for block in text.split("\n\n"):
        candidate = block if not current else current + "\n\n" + block
        if len(candidate) <= max_len:
            current = candidate
        else:
            if current:
                chunks.append(current)
            if len(block) > max_len:
                for i in range(0, len(block), max_len):
                    chunks.append(block[i:i + max_len])
                current = ""
            else:
                current = block

    if current:
        chunks.append(current)

    return chunks

# =========================================================
# SEND DIGEST
# =========================================================

async def send_digest(channel: discord.abc.Messageable, start: datetime, end: datetime) -> None:
    data = await build_all_digest(start, end)
    total_topics = sum(len(v) for v in TOPIC_GROUPS.values())

    header = discord.Embed(
        title="📰 TECH DIGEST",
        description=(
            f"🕐 **{format_range(start, end)}**\n"
            f"📂 **{len(data)}/{len(TOPIC_GROUPS)} nhóm**\n"
            f"📌 **{total_topics} chủ đề**"
        ),
        color=0x00FFCC,
        timestamp=datetime.utcnow(),
    )
    await channel.send(embed=header)
    await asyncio.sleep(0.8)

    if not data:
        await channel.send("⚠️ Không tìm thấy tin mới trong khung thời gian này.")
        return

    for item in data:
        group = item["group"]
        content = item["content"]
        if not content:
            continue

        await channel.send(
            embed=discord.Embed(
                title=group,
                color=GROUP_COLORS.get(group, 0xAAAAAA),
            )
        )

        for chunk in split_message(content):
            await channel.send(chunk)
            await asyncio.sleep(Config.DISCORD_DELAY)

    logger.info("✅ Đã gửi digest cho %s → %s", start, end)

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
        logger.error("Không lấy được channel %s: %s", CHANNEL_ID, e)
        return None

# =========================================================
# SCHEDULER
# =========================================================

async def auto_scheduler() -> None:
    global last_sent_slot

    await bot.wait_until_ready()
    channel = await get_target_channel()
    if not channel:
        return

    load_state()

    while not bot.is_closed():
        slot = current_slot()
        if slot != last_sent_slot:
            now = vn_now()
            start = now - timedelta(hours=Config.SEARCH_INTERVAL_HOURS)
            logger.info("📢 Tự động gửi mốc %s:00", slot)
            try:
                await send_digest(channel, start, now)
                last_sent_slot = slot
                save_state()
            except Exception as e:
                logger.error("Lỗi gửi digest: %s", e)

        await wait_until_next_slot()

# =========================================================
# COMMANDS
# =========================================================

@bot.command(name="news", aliases=["tin"])
async def news(ctx: commands.Context):
    if ctx.channel.id != CHANNEL_ID:
        await ctx.send(f"❌ Chỉ hoạt động trong kênh <#{CHANNEL_ID}>")
        return

    async with ctx.typing():
        now = vn_now()
        start = now - timedelta(hours=Config.SEARCH_INTERVAL_HOURS)
        await send_digest(ctx.channel, start, now)

@bot.command(name="ping")
async def ping(ctx: commands.Context):
    await ctx.send(f"🏓 Pong! `{round(bot.latency * 1000)}ms`")

@bot.command(name="status")
async def status(ctx: commands.Context):
    vn = vn_now()
    nxt = next_slot_time()
    wait_minutes = int((nxt - vn).total_seconds() // 60)

    embed = discord.Embed(
        title="🤖 Bot Status",
        color=0x00FF88,
        timestamp=datetime.utcnow(),
    )
    embed.add_field(name="Giờ VN", value=vn.strftime("%H:%M:%S %d/%m/%Y"), inline=False)
    embed.add_field(name="Mốc hiện tại", value=f"{current_slot()}:00", inline=True)
    embed.add_field(name="Mốc tiếp theo", value=f"{nxt.strftime('%H:%M')} (~{wait_minutes}p)", inline=True)
    embed.add_field(name="Đã gửi mốc", value=str(last_sent_slot), inline=True)
    embed.add_field(name="Nhóm / Chủ đề", value=f"{len(TOPIC_GROUPS)} / {sum(len(v) for v in TOPIC_GROUPS.values())}", inline=True)
    embed.add_field(name="Parallel", value=str(Config.MAX_PARALLEL_REQUESTS), inline=True)
    embed.add_field(name="API Base", value=API_BASE or "Chưa đặt", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="force", aliases=["forcenews"])
async def force(ctx: commands.Context):
    if ctx.channel.id != CHANNEL_ID:
        await ctx.send(f"❌ Chỉ hoạt động trong kênh <#{CHANNEL_ID}>")
        return

    async with ctx.typing():
        now = vn_now()
        start = now - timedelta(hours=Config.SEARCH_INTERVAL_HOURS)
        await send_digest(ctx.channel, start, now)

        global last_sent_slot
        last_sent_slot = current_slot()
        save_state()

# =========================================================
# EVENTS
# =========================================================

@bot.event
async def on_ready():
    global scheduler_started

    total_topics = sum(len(v) for v in TOPIC_GROUPS.values())
    logger.info("✅ Bot online: %s | %s nhóm | %s chủ đề", bot.user, len(TOPIC_GROUPS), total_topics)

    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name=f"{total_topics} chủ đề | mỗi 6h",
        )
    )

    if not scheduler_started:
        scheduler_started = True
        bot.loop.create_task(auto_scheduler())
        logger.info("Auto scheduler started")

@bot.event
async def on_command_error(ctx: commands.Context, error: Exception):
    if isinstance(error, commands.CommandNotFound):
        await ctx.send("❓ Lệnh không tồn tại. Dùng `!news`, `!ping`, `!status`, `!force`")
    else:
        logger.error("Command error: %s", error)
        await ctx.send(f"⚠️ Lỗi: {str(error)[:200]}")

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        raise SystemExit("Thiếu DISCORD_TOKEN trong .env")
    if not CHANNEL_ID:
        raise SystemExit("Thiếu CHANNEL_ID trong .env")
    if not API_BASE:
        raise SystemExit("Thiếu API_BASE trong .env")
    if not API_KEY:
        raise SystemExit("Thiếu API_KEY trong .env")

    logger.info("Starting bot...")
    bot.run(DISCORD_TOKEN)
