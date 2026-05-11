import discord
from discord.ext import commands
from dotenv import load_dotenv

import aiohttp
import asyncio
import os
import json
import logging
import hashlib
import time

from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# =========================================================
# LOGGING
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            "bot.log",
            encoding="utf-8"
        )
    ]
)

logger = logging.getLogger(__name__)

# =========================================================
# ENV
# =========================================================

load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))

API_BASE = os.getenv("API_BASE", "")
API_KEY = os.getenv("API_KEY", "")
MODEL_NAME = os.getenv(
    "MODEL_NAME",
    "gpt-4o-mini"
)

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

    MAX_TOKENS = 2000

    STATE_FILE = "bot_state.json"

    SLOTS = [0, 6, 12, 18]

# =========================================================
# TOPICS
# =========================================================

TOPIC_GROUPS = {

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

        "NVIDIA AI",
        "AMD AI",
        "Intel AI",

        "Windows update",
        "Linux news",

        "cloud computing",
        "datacenter technology",

        "SpaceX news",
        "NASA news",
    ],

    "🎮 Gaming": [

        "gaming news",
        "Steam game news",
        "PlayStation news",
        "Xbox news",

        "Wuthering Waves news",
        "Genshin Impact news",
    ],

    "📈 Crypto & Finance": [

        "Bitcoin news",
        "Ethereum news",
        "crypto market",

        "VN-Index",
        "global economy",
    ],

    "⚽ Sports": [

        "football news",
        "Champions League",
        "transfer news",
    ],

    "🎬 Entertainment": [

        "Netflix releases",
        "Hollywood news",
        "KDrama news",
        "Kpop news",
    ]
}

GROUP_COLORS = {

    "🤖 AI & Công Nghệ": 0x00CFFF,
    "🎮 Gaming": 0x9966FF,
    "📈 Crypto & Finance": 0xF7D000,
    "⚽ Sports": 0x44FF88,
    "🎬 Entertainment": 0xFF9900,
}

# =========================================================
# DISCORD
# =========================================================

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents
)

# =========================================================
# GLOBALS
# =========================================================

CACHE: Dict[str, Dict[str, Any]] = {}

semaphore = asyncio.Semaphore(
    Config.MAX_PARALLEL_REQUESTS
)

last_sent_slot = -1

# =========================================================
# TIME
# =========================================================

def vn_now():

    return datetime.utcnow() + timedelta(
        hours=Config.TIMEZONE_OFFSET
    )

def format_range(start, end):

    return (
        f"{start.strftime('%H:%M %d/%m/%Y')} "
        f"→ "
        f"{end.strftime('%H:%M %d/%m/%Y')}"
    )

def current_slot():

    hour = vn_now().hour

    for slot in reversed(Config.SLOTS):

        if hour >= slot:
            return slot

    return 18

def next_slot_time():

    now = vn_now()

    cur = current_slot()

    for slot in Config.SLOTS:

        if slot > cur:

            return now.replace(
                hour=slot,
                minute=0,
                second=0,
                microsecond=0
            )

    return (
        now + timedelta(days=1)
    ).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

async def wait_until_next_slot():

    nxt = next_slot_time()

    seconds = max(
        (nxt - vn_now()).total_seconds(),
        0
    )

    logger.info(
        f"Waiting {seconds:.0f}s"
    )

    await asyncio.sleep(seconds)

# =========================================================
# STATE
# =========================================================

def load_state():

    global last_sent_slot

    try:

        with open(
            Config.STATE_FILE,
            "r"
        ) as f:

            data = json.load(f)

            last_sent_slot = data.get(
                "last_sent_slot",
                -1
            )

    except:
        last_sent_slot = -1

def save_state():

    try:

        with open(
            Config.STATE_FILE,
            "w"
        ) as f:

            json.dump(
                {
                    "last_sent_slot":
                    last_sent_slot
                },
                f
            )

    except Exception as e:

        logger.error(e)

# =========================================================
# CACHE
# =========================================================

def make_cache_key(
    topic,
    start,
    end
):

    raw = f"{topic}_{start}_{end}"

    return hashlib.md5(
        raw.encode()
    ).hexdigest()

def get_cache(key):

    if key not in CACHE:
        return None

    item = CACHE[key]

    if (
        time.time()
        - item["time"]
        > Config.CACHE_EXPIRE
    ):

        del CACHE[key]

        return None

    return item["data"]

def set_cache(
    key,
    value
):

    CACHE[key] = {

        "time": time.time(),
        "data": value
    }

# =========================================================
# SSE PARSER
# =========================================================

async def parse_openai_response(resp):

    content_type = (
        resp.headers.get(
            "Content-Type",
            ""
        ).lower()
    )

    logger.info(
        f"Content-Type: {content_type}"
    )

    # JSON
    if "application/json" in content_type:

        return await resp.json()

    # STREAM
    elif "text/event-stream" in content_type:

        full_text = ""

        async for raw_line in resp.content:

            try:

                line = raw_line.decode(
                    "utf-8",
                    errors="ignore"
                ).strip()

            except:
                continue

            if not line:
                continue

            if not line.startswith("data:"):
                continue

            data_str = line[5:].strip()

            if data_str == "[DONE]":
                break

            try:

                chunk = json.loads(data_str)

                choices = chunk.get(
                    "choices",
                    []
                )

                if not choices:
                    continue

                choice = choices[0]

                delta = (
                    choice.get("delta", {})
                    .get("content")
                )

                if delta is None:

                    delta = (
                        choice.get("message", {})
                        .get("content", "")
                    )

                if delta:
                    full_text += delta

            except:
                continue

        return {

            "choices": [

                {
                    "message": {
                        "content": full_text
                    }
                }
            ]
        }

    else:

        text = await resp.text()

        raise Exception(
            f"Unsupported Content-Type: "
            f"{content_type}\n{text[:300]}"
        )

# =========================================================
# API CALL
# =========================================================

async def make_api_call(
    session,
    messages,
    retry=0
):

    async with semaphore:

        try:

            payload = {

                "model": MODEL_NAME,
                "messages": messages,
                "temperature": 0.4,
                "max_tokens": Config.MAX_TOKENS,
                "stream": False
            }

            async with session.post(

                f"{API_BASE}/chat/completions",

                headers={

                    "Authorization":
                    f"Bearer {API_KEY}",

                    "Content-Type":
                    "application/json"
                },

                json=payload,

                timeout=aiohttp.ClientTimeout(
                    total=Config.REQUEST_TIMEOUT,
                    sock_read=Config.REQUEST_TIMEOUT
                )

            ) as resp:

                if resp.status in [
                    429,
                    500,
                    502,
                    503,
                    504
                ]:

                    if retry < Config.MAX_RETRIES:

                        delay = (
                            Config.RETRY_DELAY
                            * (2 ** retry)
                        )

                        logger.warning(
                            f"Retry {retry+1} "
                            f"after {delay}s "
                            f"(status {resp.status})"
                        )

                        await asyncio.sleep(delay)

                        return await make_api_call(
                            session,
                            messages,
                            retry + 1
                        )

                    return None

                if resp.status != 200:

                    logger.error(
                        f"API Error {resp.status}"
                    )

                    logger.error(
                        await resp.text()
                    )

                    return None

                return await parse_openai_response(
                    resp
                )

        except asyncio.TimeoutError:

            logger.warning(
                f"Timeout retry {retry+1}"
            )

            if retry < Config.MAX_RETRIES:

                await asyncio.sleep(
                    Config.RETRY_DELAY
                    * (2 ** retry)
                )

                return await make_api_call(
                    session,
                    messages,
                    retry + 1
                )

            return None

        except Exception as e:

            logger.error(
                f"API Error: {e}"
            )

            if retry < Config.MAX_RETRIES:

                await asyncio.sleep(
                    Config.RETRY_DELAY
                    * (2 ** retry)
                )

                return await make_api_call(
                    session,
                    messages,
                    retry + 1
                )

            return None

# =========================================================
# SEARCH TOPIC
# =========================================================

async def search_topic(
    session,
    topic,
    start,
    end
):

    cache_key = make_cache_key(
        topic,
        start,
        end
    )

    cached = get_cache(cache_key)

    if cached:
        return cached

    time_range = format_range(
        start,
        end
    )

    messages = [

        {
            "role": "system",

            "content": """
Bạn là AI săn tin tức toàn cầu.

Yêu cầu:
- Trả lời tiếng Việt
- Tìm tin HOT nhất
- Markdown đẹp
- Ưu tiên nguồn uy tín
"""
        },

        {
            "role": "user",

            "content": f"""
Tìm tin mới nhất về:

{topic}

Khung giờ:
{time_range}

Yêu cầu:
- 2-3 tin nổi bật
- tiêu đề
- tóm tắt ngắn
- link nguồn
- markdown
"""
        }
    ]

    result = await make_api_call(
        session,
        messages
    )

    if not result:
        return ""

    try:

        content = (
            result["choices"][0]
            ["message"]["content"]
        )

        if len(content) < 50:
            return ""

        set_cache(
            cache_key,
            content
        )

        return content

    except:
        return ""

# =========================================================
# BUILD GROUP
# =========================================================

async def build_group_digest(
    group_name,
    topics,
    start,
    end,
    session
):

    tasks = [

        search_topic(
            session,
            topic,
            start,
            end
        )

        for topic in topics
    ]

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True
    )

    valid = []

    seen = set()

    for r in results:

        if not isinstance(r, str):
            continue

        if len(r) < 50:
            continue

        key = r[:100].lower()

        if key in seen:
            continue

        seen.add(key)

        valid.append(r)

    return {

        "group": group_name,

        "content":
        "\n\n---\n\n".join(valid)
    }

# =========================================================
# BUILD ALL
# =========================================================

async def build_all_digest(
    start,
    end
):

    connector = aiohttp.TCPConnector(
        limit=Config.TCP_LIMIT,
        ttl_dns_cache=Config.DNS_CACHE,
        ssl=False
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        tasks = [

            build_group_digest(
                group,
                topics,
                start,
                end,
                session
            )

            for group, topics
            in TOPIC_GROUPS.items()
        ]

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

    final = []

    for r in results:

        if not isinstance(r, dict):
            continue

        if not r["content"]:
            continue

        final.append(r)

    return final

# =========================================================
# SPLIT
# =========================================================

def split_message(text):

    chunks = []

    while len(text) > Config.MAX_DISCORD_LENGTH:

        split_at = text.rfind(
            "\n",
            0,
            Config.MAX_DISCORD_LENGTH
        )

        if split_at == -1:
            split_at = Config.MAX_DISCORD_LENGTH

        chunks.append(text[:split_at])

        text = text[split_at:]

    chunks.append(text)

    return chunks

# =========================================================
# SEND DIGEST
# =========================================================

async def send_digest(
    channel,
    start,
    end
):

    data = await build_all_digest(
        start,
        end
    )

    total_topics = sum(
        len(v)
        for v in TOPIC_GROUPS.values()
    )

    header = discord.Embed(

        title="📰 TECH DIGEST",

        description=(

            f"🕐 {format_range(start, end)}\n"

            f"📂 {len(data)} nhóm\n"

            f"📌 {total_topics} chủ đề"
        ),

        color=0x00ffcc,

        timestamp=datetime.utcnow()
    )

    await channel.send(embed=header)

    await asyncio.sleep(1)

    for item in data:

        group = item["group"]

        content = item["content"]

        if not content:
            continue

        embed = discord.Embed(

            title=group,

            color=GROUP_COLORS.get(
                group,
                0xaaaaaa
            )
        )

        await channel.send(embed=embed)

        chunks = split_message(
            content
        )

        for chunk in chunks:

            await channel.send(chunk)

            await asyncio.sleep(
                Config.DISCORD_DELAY
            )

# =========================================================
# AUTO SCHEDULER
# =========================================================

async def auto_scheduler():

    global last_sent_slot

    await bot.wait_until_ready()

    channel = bot.get_channel(
        CHANNEL_ID
    )

    if not channel:

        logger.error(
            "Channel not found"
        )

        return

    while not bot.is_closed():

        slot = current_slot()

        if slot != last_sent_slot:

            end = vn_now()

            start = end - timedelta(
                hours=Config.SEARCH_INTERVAL_HOURS
            )

            logger.info(
                f"Sending slot {slot}"
            )

            try:

                await send_digest(
                    channel,
                    start,
                    end
                )

                last_sent_slot = slot

                save_state()

            except Exception as e:

                logger.error(e)

        await wait_until_next_slot()

# =========================================================
# COMMANDS
# =========================================================

@bot.command(name="news")
async def news(ctx):

    if ctx.channel.id != CHANNEL_ID:

        await ctx.send(
            f"❌ Chỉ dùng trong <#{CHANNEL_ID}>"
        )

        return

    async with ctx.typing():

        now = vn_now()

        start = now - timedelta(hours=6)

        await send_digest(
            ctx.channel,
            start,
            now
        )

@bot.command(name="ping")
async def ping(ctx):

    await ctx.send(
        f"🏓 {round(bot.latency * 1000)}ms"
    )

@bot.command(name="status")
async def status(ctx):

    total_topics = sum(
        len(v)
        for v in TOPIC_GROUPS.values()
    )

    embed = discord.Embed(

        title="🤖 Bot Status",

        color=0x00ff00,

        timestamp=datetime.utcnow()
    )

    embed.add_field(
        name="Latency",
        value=f"{round(bot.latency * 1000)}ms",
        inline=True
    )

    embed.add_field(
        name="Parallel",
        value=str(
            Config.MAX_PARALLEL_REQUESTS
        ),
        inline=True
    )

    embed.add_field(
        name="Topics",
        value=str(total_topics),
        inline=True
    )

    embed.add_field(
        name="Model",
        value=MODEL_NAME,
        inline=False
    )

    await ctx.send(embed=embed)

@bot.command(name="force")
async def force(ctx):

    async with ctx.typing():

        now = vn_now()

        start = now - timedelta(hours=6)

        await send_digest(
            ctx.channel,
            start,
            now
        )

# =========================================================
# EVENTS
# =========================================================

@bot.event
async def on_ready():

    total_topics = sum(
        len(v)
        for v in TOPIC_GROUPS.values()
    )

    logger.info(

        f"✅ Bot online: {bot.user} | "
        f"{len(TOPIC_GROUPS)} nhóm | "
        f"{total_topics} chủ đề"
    )

    await bot.change_presence(

        activity=discord.Activity(

            type=discord.ActivityType.watching,

            name=f"{total_topics} chủ đề"
        )
    )

    bot.loop.create_task(
        auto_scheduler()
    )

@bot.event
async def on_command_error(
    ctx,
    error
):

    logger.error(error)

    if isinstance(
        error,
        commands.CommandNotFound
    ):

        await ctx.send(
            "❌ Command không tồn tại"
        )

        return

    await ctx.send(
        f"⚠️ {str(error)[:300]}"
    )

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    if not DISCORD_TOKEN:

        raise Exception(
     =====================
# API CALL
# =========================================================

async def make_api_call(
    session,
    messages,
    retry=0
):

    async with semaphore:

        try:

            payload = {
                "model": MODEL_NAME,
                "messages": messages,
                "temperature": 0.4,
                "max_tokens": Config.MAX_TOKENS,
                "stream": False
            }

            async with session.post(
                f"{API_BASE}/chat/completions",
                headers={
                    "Authorization": f"Bearer {API_KEY}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=aiohttp.ClientTimeout(
                    total=Config.REQUEST_TIMEOUT,
                    sock_read=Config.REQUEST_TIMEOUT
                )
            ) as resp:

                # =========================================
                # RETRY STATUS
                # =========================================

                if resp.status in [429, 500, 502, 503, 504]:

                    if retry < Config.MAX_RETRIES:

                        delay = (
                            Config.RETRY_DELAY
                            * (2 ** retry)
                        )

                        logger.warning(
                            f"Retry {retry+1} "
                            f"after {delay}s "
                            f"(status {resp.status})"
                        )

                        await asyncio.sleep(delay)

                        return await make_api_call(
                            session,
                            messages,
                            retry + 1
                        )

                    return None

                if resp.status != 200:

                    logger.error(
                        f"API Error {resp.status}"
                    )

                    logger.error(
                        await resp.text()
                    )

                    return None

                return await parse_openai_response(resp)

        except asyncio.TimeoutError:

            logger.warning(
                f"Timeout retry {retry+1}"
            )

            if retry < Config.MAX_RETRIES:

                await asyncio.sleep(
                    Config.RETRY_DELAY
                    * (2 ** retry)
                )

                return await make_api_call(
                    session,
                    messages,
                    retry + 1
                )

            return None

        except Exception as e:

            logger.error(f"API Error: {e}")

            if retry < Config.MAX_RETRIES:

                await asyncio.sleep(
                    Config.RETRY_DELAY
                    * (2 ** retry)
                )

                return await make_api_call(
                    session,
                    messages,
                    retry + 1
                )

            return None

# =========================================================
# SEARCH ONE TOPIC
# =========================================================

async def search_topic(
    session,
    topic,
    start,
    end
):

    cache_key = make_cache_key(
        topic,
        start,
        end
    )

    cached = get_cache(cache_key)

    if cached:
        return cached

    time_range = format_range(start, end)

    messages = [

        {
            "role": "system",
            "content": """
Bạn là AI săn tin tức toàn cầu.

Yêu cầu:
- Trả lời tiếng Việt
- Tìm tin HOT nhất
- Ưu tiên nguồn uy tín
- Markdown đẹp
"""
        },

        {
            "role": "user",
            "content": f"""
Tìm tin mới nhất về:

{topic}

Khung giờ:
{time_range}

Yêu cầu:
- 2-3 tin nổi bật
- tiêu đề
- tóm tắt ngắn
- link nguồn
- markdown
"""
        }
    ]

    result = await make_api_call(
        session,
        messages
    )

    if not result:
        return ""

    try:

        content = (
            result["choices"][0]
            ["message"]["content"]
        )

        if len(content) < 50:
            return ""

        set_cache(
            cache_key,
            content
        )

        return content

    except:
        return ""

# =========================================================
# BUILD GROUP DIGEST
# =========================================================

async def build_group_digest(
    group_name,
    topics,
    start,
    end,
    session
):

    tasks = [

        search_topic(
            session,
            topic,
            start,
            end
        )

        for topic in topics
    ]

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True
    )

    valid = []

    seen = set()

    for r in results:

        if not isinstance(r, str):
            continue

        if len(r) < 50:
            continue

        key = r[:100].lower()

        if key in seen:
            continue

        seen.add(key)

        valid.append(r)

    return {
        "group": group_name,
        "content": "\n\n---\n\n".join(valid)
    }

# =========================================================
# BUILD ALL DIGEST
# =========================================================

async def build_all_digest(
    start,
    end
):

    connector = aiohttp.TCPConnector(
        limit=Config.TCP_LIMIT,
        ttl_dns_cache=Config.DNS_CACHE,
        ssl=False
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        tasks = [

            build_group_digest(
                group,
                topics,
                start,
                end,
                session
            )

            for group, topics
            in TOPIC_GROUPS.items()
        ]

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

    final = []

    for r in results:

        if not isinstance(r, dict):
            continue

        if not r["content"]:
            continue

        final.append(r)

    return final

# =========================================================
# SPLIT MESSAGE
# =========================================================

def split_message(text):

    chunks = []

    while len(text) > Config.MAX_DISCORD_LENGTH:

        split_at = text.rfind(
            "\n",
            0,
            Config.MAX_DISCORD_LENGTH
        )

        if split_at == -1:
            split_at = Config.MAX_DISCORD_LENGTH

        chunks.append(text[:split_at])

        text = text[split_at:]

    chunks.append(text)

    return chunks

# =========================================================
# SEND DIGEST
# =========================================================

async def send_digest(
    channel,
    start,
    end
):

    data = await build_all_digest(
        start,
        end
    )

    total_topics = sum(
        len(v)
        for v in TOPIC_GROUPS.values()
    )

    header = discord.Embed(
        title="📰 TECH DIGEST",
        description=(
            f"🕐 {format_range(start, end)}\n"
            f"📂 {len(data)} nhóm\n"
            f"📌 {total_topics} chủ đề"
        ),
        color=0x00ffcc,
        timestamp=datetime.utcnow()
    )

    await channel.send(embed=header)

    await asyncio.sleep(1)

    # =====================================================
    # SEND GROUPS
    # =====================================================

    for item in data:

        group = item["group"]

        content = item["content"]

        if not content:
            continue

        embed = discord.Embed(
            title=group,
            color=GROUP_COLORS.get(
                group,
                0xaaaaaa
            )
        )

        await channel.send(embed=embed)

        chunks = split_message(content)

        for chunk in chunks:

            await channel.send(chunk)

            await asyncio.sleep(
                Config.DISCORD_DELAY
            )

# =========================================================
# AUTO SCHEDULER
# =========================================================

async def auto_scheduler():

    global last_sent_slot

    await bot.wait_until_ready()

    channel = bot.get_channel(CHANNEL_ID)

    if not channel:

        logger.error(
            "Channel not found"
        )

        return

    while not bot.is_closed():

        slot = current_slot()

        if slot != last_sent_slot:

            end = vn_now()

            start = end - timedelta(
                hours=Config.SEARCH_INTERVAL_HOURS
            )

            logger.info(
                f"Sending slot {slot}"
            )

            try:

                await send_digest(
                    channel,
                    start,
                    end
                )

                last_sent_slot = slot

                save_state()

            except Exception as e:

                logger.error(e)

        await wait_until_next_slot()

# =========================================================
# COMMANDS
# =========================================================

@bot.command(name="news")
async def news(ctx):

    if ctx.channel.id != CHANNEL_ID:

        await ctx.send(
            f"❌ Chỉ dùng trong <#{CHANNEL_ID}>"
        )

        return

    async with ctx.typing():

        now = vn_now()

        start = now - timedelta(hours=6)

        await send_digest(
            ctx.channel,
            start,
            now
        )

@bot.command(name="ping")
async def ping(ctx):

    await ctx.send(
        f"🏓 {round(bot.latency * 1000)}ms"
    )

@bot.command(name="status")
async def status(ctx):

    total_topics = sum(
        len(v)
        for v in TOPIC_GROUPS.values()
    )

    embed = discord.Embed(
        title="🤖 Bot Status",
        color=0x00ff00,
        timestamp=datetime.utcnow()
    )

    embed.add_field(
        name="Latency",
   
            return now.replace(
                hour=slot,
                minute=0,
                second=0,
                microsecond=0
            )

    return (
        now + timedelta(days=1)
    ).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

async def wait_until_next_slot():

    nxt = next_slot_time()

    seconds = max(
        (nxt - vn_now()).total_seconds(),
        0
    )

    logger.info(f"Waiting {seconds:.0f}s")

    await asyncio.sleep(seconds)

# =========================================================
# SSE PARSER
# =========================================================

async def parse_openai_response(resp):

    content_type = (
        resp.headers.get(
            "Content-Type",
            ""
        ).lower()
    )

    logger.info(f"Content-Type: {content_type}")

    # =====================================================
    # JSON
    # =====================================================

    if "application/json" in content_type:

        return await resp.json()

    # =====================================================
    # STREAM
    # =====================================================

    elif "text/event-stream" in content_type:

        full_text = ""

        async for raw_line in resp.content:

            try:
                line = raw_line.decode(
                    "utf-8",
                    errors="ignore"
                ).strip()

            except:
                continue

            if not line:
                continue

            if not line.startswith("data:"):
                continue

            data_str = line[5:].strip()

            if data_str == "[DONE]":
                break

            try:

                chunk = json.loads(data_str)

                choices = chunk.get(
                    "choices",
                    []
                )

                if not choices:
                    continue

                choice = choices[0]

                delta = (
                    choice.get("delta", {})
                    .get("content")
                )

                if delta is None:

                    delta = (
                        choice.get("message", {})
                        .get("content", "")
                    )

                if delta:
                    full_text += delta

            except Exception:
                continue

        return {
            "choices": [
                {
                    "message": {
                        "content": full_text
                    }
                }
            ]
        }

    else:

        text = await resp.text()

        raise Exception(
            f"Unsupported content type: "
            f"{content_type}\n{text[:500]}"
        )

# =========================================================
# RETRY REQUEST
# =========================================================

async def make_api_call(
    session,
    messages,
    retry=0
):

    async with semaphore:

        try:

            payload = {
                "model": MODEL_NAME,
                "messages": messages,
                "temperature": 0.4,
                "max_tokens": 2000,
                "stream": False
            }

            async with session.post(
                f"{API_BASE}/chat/completions",
                headers={
                    "Authorization": f"Bearer {API_KEY}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=aiohttp.ClientTimeout(
                    total=Config.REQUEST_TIMEOUT,
                    sock_read=Config.REQUEST_TIMEOUT
                )
            ) as resp:

                if resp.status == 429:

                    if retry < Config.MAX_RETRIES:

                        delay = (
                            Config.RETRY_DELAY
                            * (2 ** retry)
                        )

                        logger.warning(
                            f"429 retry {retry+1}"
                        )

                        await asyncio.sleep(delay)

                        return await make_api_call(
                            session,
                            messages,
                            retry + 1
                        )

                    return None

                if resp.status >= 500:

                    if retry < Config.MAX_RETRIES:

                        delay = (
                            Config.RETRY_DELAY
                            * (2 ** retry)
                        )

                        logger.warning(
                            f"5xx retry {retry+1}"
                        )

                        await asyncio.sleep(delay)

                        return await make_api_call(
                            session,
                            messages,
                            retry + 1
                        )

                    return None

                if resp.status != 200:

                    logger.error(
                        f"API Error {resp.status}"
                    )

                    logger.error(await resp.text())

                    return None

                return await parse_openai_response(resp)

        except asyncio.TimeoutError:

            logger.warning("Timeout")

            if retry < Config.MAX_RETRIES:

                await asyncio.sleep(
                    Config.RETRY_DELAY * (2 ** retry)
                )

                return await make_api_call(
                    session,
                    messages,
                    retry + 1
                )

            return None

        except Exception as e:

            logger.error(f"API Error: {e}")

            if retry < Config.MAX_RETRIES:

                await asyncio.sleep(
                    Config.RETRY_DELAY * (2 ** retry)
                )

                return await make_api_call(
                    session,
                    messages,
                    retry + 1
                )

            return None

# =========================================================
# SEARCH ONE TOPIC
# =========================================================

async def search_topic(
    session,
    topic,
    start,
    end
):

    time_range = format_range(start, end)

    messages = [

        {
            "role": "system",
            "content": (
                "Bạn là AI săn tin tức toàn cầu. "
                "Trả lời tiếng Việt. "
                "Tìm tin HOT nhất."
            )
        },

        {
            "role": "user",
            "content": f"""
Tìm tin mới nhất về:

{topic}

Khung giờ:
{time_range}

Yêu cầu:
- 2-3 tin nổi bật
- tiêu đề
- tóm tắt ngắn
- link nguồn
- markdown
"""
        }
    ]

    result = await make_api_call(
        session,
        messages
    )

    if not result:
        return ""

    try:

        content = (
            result["choices"][0]
            ["message"]["content"]
        )

        if len(content) < 50:
            return ""

        return content

    except:
        return ""

# =========================================================
# BUILD DIGEST
# =========================================================

async def build_digest(start, end):

    connector = aiohttp.TCPConnector(
        limit=50,
        ssl=False
    )

    async with aiohttp.ClientSession(
        connector=connector
    ) as session:

        tasks = [

            search_topic(
                session,
                topic,
                start,
                end
            )

            for topic in TOPICS
        ]

        results = await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

    valid = []

    seen = set()

    for r in results:

        if not isinstance(r, str):
            continue

        if len(r) < 50:
            continue

        key = r[:100].lower()

        if key in seen:
            continue

        seen.add(key)

        valid.append(r)

    if not valid:

        return "⚠️ Không có tin mới."

    return "\n\n---\n\n".join(valid)

# =========================================================
# SPLIT
# =========================================================

def split_message(text):

    chunks = []

    while len(text) > Config.MAX_DISCORD_LENGTH:

        split_at = text.rfind(
            "\n",
            0,
            Config.MAX_DISCORD_LENGTH
        )

        if split_at == -1:
            split_at = Config.MAX_DISCORD_LENGTH

        chunks.append(text[:split_at])

        text = text[split_at:]

    chunks.append(text)

    return chunks

# =========================================================
# SEND DIGEST
# =========================================================

async def send_digest(channel, start, end):

    digest = await build_digest(
        start,
        end
    )

    header = discord.Embed(
        title="📰 TECH DIGEST",
        description=format_range(start, end),
        color=0x00ffcc,
        timestamp=datetime.utcnow()
    )

    await channel.send(embed=header)

    chunks = split_message(digest)

    for chunk in chunks:

        await channel.send(chunk)

        await asyncio.sleep(
            Config.DISCORD_DELAY
        )

# =========================================================
# AUTO SCHEDULER
# =========================================================

async def auto_scheduler():

    global last_sent_slot

    await bot.wait_until_ready()

    channel = bot.get_channel(CHANNEL_ID)

    if not channel:

        logger.error("Channel not found")

        return

    while not bot.is_closed():

        slot = current_slot()

        if slot != last_sent_slot:

            end = vn_now()

            start = end - timedelta(
                hours=Config.SEARCH_INTERVAL_HOURS
            )

            logger.info(
                f"Sending slot {slot}"
            )

            try:

                await send_digest(
                    channel,
                    start,
                    end
                )

                last_sent_slot = slot

            except Exception as e:

                logger.error(e)

        await wait_until_next_slot()

# =========================================================
# COMMANDS
# =========================================================

@bot.command(name="news")
async def news(ctx):

    if ctx.channel.id != CHANNEL_ID:

        await ctx.send("❌ Wrong channel")

        return

    async with ctx.typing():

        now = vn_now()

        start = now - timedelta(hours=6)

        await send_digest(
            ctx.channel,
            start,
            now
        )

@bot.command(name="ping")
async def ping(ctx):

    await ctx.send(
        f"🏓 {round(bot.latency * 1000)}ms"
    )

@bot.command(name="status")
async def status(ctx):

    embed = discord.Embed(
        title="Bot Status",
        color=0x00ff00
    )

    embed.add_field(
        name="Latency",
        value=f"{round(bot.latency * 1000)}ms"
    )

    embed.add_field(
        name="Parallel",
        value=str(
            Config.MAX_PARALLEL_REQUESTS
        )
    )

    embed.add_field(
        name="Model",
        value=MODEL_NAME,
        inline=False
    )

    await ctx.send(embed=embed)

# =========================================================
# READY
# =========================================================

@bot.event
async def on_ready():

    logger.info(
        f"Bot online: {bot.user}"
    )

    bot.loop.create_task(
        auto_scheduler()
    )

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":

    if not DISCORD_TOKEN:

        raise Exception(
            "Missing DISCORD_TOKEN"
        )

    if not API_KEY:

        raise Exception(
            "Missing API_KEY"
        )

    if not API_BASE:

        raise Exception(
            "Missing API_BASE"
        )

    bot.run(DISCORD_TOKEN)raise_for_status()
            data = await resp.json()
            return data["choices"][0]["message"]["content"]

    result = await with_retry(_call)
    if result and "KHÔNG CÓ TIN" not in result and len(result) > 50:
        return result
    return ""

# =========================
# SEARCH ONE TOPIC (TAVILY+GROQ hoặc FALLBACK)
# =========================
_semaphore = asyncio.Semaphore(Config.SEMAPHORE_LIMIT)

async def search_one_topic(
    session: aiohttp.ClientSession,
    topic: str,
    start: datetime,
    end: datetime,
) -> str:
    async with _semaphore:
        if TAVILY_API_KEY and GROQ_API_KEY:
            search_data = await search_tavily(session, topic, start, end)
            if search_data and search_data.get("results"):
                return await summarize_groq(session, search_data, start, end, topic)
        if API_BASE and API_KEY:
            return await summarize_compatible(session, topic, start, end)
        return ""

# =========================
# BUILD DIGEST THEO NHÓM
# =========================
async def build_digest_grouped(start: datetime, end: datetime) -> dict[str, str]:
    """
    Trả về dict {group_name: content_text}.
    Chạy parallel toàn bộ topics, dedup kết quả trùng.
    """
    async with aiohttp.ClientSession() as session:
        # Flatten: (group_name, topic) để gather 1 lần duy nhất
        flat: list[tuple[str, str]] = [
            (group, topic)
            for group, topics in TOPIC_GROUPS.items()
            for topic in topics
        ]
        tasks = [search_one_topic(session, topic, start, end) for _, topic in flat]
        results = await asyncio.gather(*tasks)

    # Gom về nhóm + dedup
    group_contents: dict[str, list[str]] = {g: [] for g in TOPIC_GROUPS}
    seen_keys: set[str] = set()

    for (group, _), content in zip(flat, results):
        if not content or len(content) < 50:
            continue
        dedup_key = content[:60].strip().lower()
        if dedup_key in seen_keys:
            continue
        seen_keys.add(dedup_key)
        group_contents[group].append(content)

    return {
        group: "\n\n".join(articles)
        for group, articles in group_contents.items()
        if articles
    }

# =========================
# SPLIT CONTENT
# =========================
def split_content(text: str, max_len: int = Config.MAX_PLAIN_TEXT_LENGTH) -> list[str]:
    """
    Split tại ranh giới bài (\\n\\n), không bao giờ cắt giữa bài.
    Hard-split chỉ khi 1 bài đơn lẻ vượt max_len.
    """
    if len(text) <= max_len:
        return [text]

    chunks: list[str] = []
    current = ""
    for article in text.split("\n\n"):
        candidate = (current + "\n\n" + article).strip() if current else article
        if len(candidate) <= max_len:
            current = candidate
        else:
            if current:
                chunks.append(current)
            if len(article) > max_len:
                for i in range(0, len(article), max_len):
                    chunks.append(article[i : i + max_len])
                current = ""
            else:
                current = article
    if current:
        chunks.append(current)
    return chunks

# =========================
# GỬI DISCORD THEO NHÓM
# =========================
async def send_grouped_digest(channel: discord.TextChannel, start: datetime, end: datetime):
    group_data = await build_digest_grouped(start, end)
    time_str    = format_time_range(start, end)
    total_topics = sum(len(v) for v in TOPIC_GROUPS.values())
    active_groups = len(group_data)
    total_articles = sum(c.count("\n\n") + 1 for c in group_data.values())

    # ── 1. Header embed tổng hợp ──────────────────────────────────
    header = discord.Embed(
        title="📰 TỔNG HỢP TIN TỨC",
        description=(
            f"🕐 **{time_str}** (giờ VN)\n"
            f"📂 **{active_groups}/{len(TOPIC_GROUPS)} nhóm**  •  "
            f"📄 **~{total_articles} bài**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━"
        ),
        color=0x00FFCC,
        timestamp=datetime.utcnow(),
    )
    header.set_footer(text=f"🤖 Cập nhật mỗi 6 giờ | {total_topics} chủ đề | Tavily + Groq")
    await channel.send(embed=header)
    await asyncio.sleep(0.5)

    # ── 2. Từng nhóm: embed tiêu đề + plain text nội dung ─────────
    for group_name, content in group_data.items():
        color = GROUP_COLORS.get(group_name, 0xAAAAAA)

        # Embed nhỏ làm header nhóm
        group_embed = discord.Embed(title=group_name, color=color)
        await channel.send(embed=group_embed)

        # Plain text, split an toàn
        for chunk in split_content(content):
            await channel.send(chunk)
            await asyncio.sleep(0.3)

        await asyncio.sleep(0.5)

    logger.info(f"✅ Đã gửi {active_groups} nhóm / {total_articles} bài cho {time_str}")

# =========================
# DISCORD BOT
# =========================
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# =========================
# TASK TỰ ĐỘNG
# =========================
async def auto_news_scheduler():
    await bot.wait_until_ready()
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        logger.error(f"Không tìm thấy channel {CHANNEL_ID}")
        return

    state = load_state()

    while not bot.is_closed():
        current_slot = get_current_slot()
        if current_slot != state["last_sent_slot"]:
            start, end = get_slot_range(current_slot)
            logger.info(f"📢 Tự động gửi tin mốc {current_slot}:00")
            try:
                await send_grouped_digest(channel, start, end)
                state["last_sent_slot"] = current_slot
                save_state(state)
            except Exception as e:
                logger.error(f"Lỗi gửi digest: {e}")
        await wait_until_next_slot()

# =========================
# COMMANDS
# =========================
@bot.command(name="news", aliases=["tin"])
async def cmd_news(ctx: commands.Context):
    if ctx.channel.id != CHANNEL_ID:
        await ctx.send(f"❌ Chỉ hoạt động trong kênh <#{CHANNEL_ID}>")
        return
    async with ctx.typing():
        msg = await ctx.send("🔍 Đang tổng hợp tin tức 6 giờ qua…")
        now   = get_vn_now()
        start = now - timedelta(hours=Config.SEARCH_INTERVAL_HOURS)
        await msg.delete()
        await send_grouped_digest(ctx.channel, start, now)


@bot.command(name="ping")
async def cmd_ping(ctx: commands.Context):
    await ctx.send(f"🏓 Pong! `{round(bot.latency * 1000)}ms`")


@bot.command(name="status")
async def cmd_status(ctx: commands.Context):
    state   = load_state()
    vn_now  = get_vn_now()
    next_t  = get_next_slot_time()
    wait_m  = int((next_t - vn_now).total_seconds() // 60)

    embed = discord.Embed(title="🤖 Bot Status", color=0x00FF88, timestamp=datetime.utcnow())
    embed.add_field(name="Giờ VN",          value=vn_now.strftime("%H:%M:%S %d/%m/%Y"), inline=False)
    embed.add_field(name="Mốc hiện tại",    value=f"{get_current_slot()}:00",           inline=True)
    embed.add_field(name="Mốc tiếp theo",   value=f"{next_t.strftime('%H:%M')} (~{wait_m}p)", inline=True)
    embed.add_field(name="Đã gửi mốc",      value=str(state["last_sent_slot"]),         inline=True)
    embed.add_field(name="Nhóm / Chủ đề",   value=f"{len(TOPIC_GROUPS)} / {sum(len(v) for v in TOPIC_GROUPS.values())}", inline=True)
    embed.add_field(name="Tavily + Groq",   value="✅" if (TAVILY_API_KEY and GROQ_API_KEY) else "❌", inline=True)
    embed.add_field(name="Fallback API",    value="✅" if (API_BASE and API_KEY) else "❌",             inline=True)
    await ctx.send(embed=embed)


@bot.command(name="forcenews", aliases=["force"])
async def cmd_force(ctx: commands.Context):
    """Gửi ngay không cần chờ mốc — dùng để test."""
    if ctx.channel.id != CHANNEL_ID:
        await ctx.send(f"❌ Chỉ hoạt động trong kênh <#{CHANNEL_ID}>")
        return
    async with ctx.typing():
        msg   = await ctx.send("⚡ Force gửi tin tức…")
        now   = get_vn_now()
        start = now - timedelta(hours=Config.SEARCH_INTERVAL_HOURS)
        await msg.delete()
        await send_grouped_digest(ctx.channel, start, now)
        # Cập nhật state để tránh gửi trùng ở mốc tiếp theo
        state = load_state()
        state["last_sent_slot"] = get_current_slot()
        save_state(state)


# =========================
# ON READY
# =========================
@bot.event
async def on_ready():
    total_topics = sum(len(v) for v in TOPIC_GROUPS.values())
    logger.info(f"✅ Bot online: {bot.user} | {len(TOPIC_GROUPS)} nhóm | {total_topics} chủ đề")
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name=f"{total_topics} chủ đề | mỗi 6h",
        )
    )
    bot.loop.create_task(auto_news_scheduler())


# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        logger.error("❌ Thiếu DISCORD_TOKEN trong .env")
        raise SystemExit(1)

    if not CHANNEL_ID:
        logger.error("❌ Thiếu CHANNEL_ID trong .env")
        raise SystemExit(1)

    bot.run(DISCORD_TOKEN)

# =========================
# XÁC ĐỊNH THỜI GIAN HIỆN TẠI (TỪ INTERNET)
# =========================
async def get_current_time_from_web() -> str:
    """Lấy thời gian hiện tại từ một API bên ngoài hoặc từ Gemini (có search)"""
    # Cách đơn giản: dùng datetime hệ thống (đã offset VN)
    vn_now = get_vn_now()
    return vn_now.strftime("%A, %d/%m/%Y %H:%M:%S")
    # Nếu muốn chính xác tuyệt đối, có thể gọi API worldtime, nhưng không cần thiết

# =========================
# API COMPATIBLE (FALLBACK) - HỖ TRỢ SONG SONG
# =========================
async def make_api_call(session: aiohttp.ClientSession, messages: list, retry_count: int = 0) -> Optional[Dict[str, Any]]:
    if not API_KEY or not API_BASE:
        return None
    payload = {
        "model": MODEL_NAME,
        "messages": messages,
        "temperature": Config.TEMPERATURE,
        "max_tokens": 2000,
        "stream": False
    }
    try:
        async with session.post(
            f"{API_BASE}/chat/completions",
            headers={"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"},
            json=payload,
            timeout=Config.API_TIMEOUT_SECONDS
        ) as resp:
            if resp.status == 200:
                return await resp.json()
            elif resp.status == 429 and retry_count < Config.MAX_RETRIES - 1:
                await asyncio.sleep(Config.RETRY_DELAY_SECONDS * (retry_count + 1))
                return await make_api_call(session, messages, retry_count + 1)
            else:
                logger.error(f"API Error {resp.status}: {await resp.text()}")
                return None
    except Exception as e:
        logger.error(f"API call failed: {e}")
        return None

async def search_single_topic_compatible(session: aiohttp.ClientSession, topic: str, start: datetime, end: datetime) -> str:
    """Tìm kiếm một chủ đề bằng API compatible (không dùng Tavily)"""
    time_range_str = format_time_range(start, end)
    system_prompt = f"""Bạn là AI săn tin công nghệ, có khả năng tìm kiếm internet.
Yêu cầu: Phản hồi bằng TIẾNG VIỆT. Tìm tin tức liên quan đến chủ đề: {topic}
Khung giờ: {time_range_str} (giờ Việt Nam).
- Ưu tiên nguồn uy tín: TechCrunch, The Verge, Reuters, Bloomberg, VnExpress, Vietnam+,...
- Mỗi tin cần có: tiêu đề, tóm tắt 2 câu, link nguồn.
- Nếu không có tin, trả lời 'Không tìm thấy tin mới cho {topic}'.

Định dạng markdown, emoji."""
    user_prompt = f"Tìm tin tức {topic} trong {time_range_str}."
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    result = await make_api_call(session, messages)
    if result and "choices" in result:
        content = result["choices"][0]["message"]["content"]
        return content if len(content) > 50 else ""
    return ""

async def get_news_compatible_parallel(start: datetime, end: datetime) -> str:
    """Chạy song song nhiều chủ đề với API compatible, sau đó tổng hợp"""
    async with aiohttp.ClientSession() as session:
        tasks = []
        for topic in Config.SEARCH_TOPICS:
            tasks.append(search_single_topic_compatible(session, topic, start, end))
        results = await asyncio.gather(*tasks)
        # Lọc bỏ các kết quả rỗng
        valid_results = [r for r in results if r and not r.startswith("Không tìm thấy")]
        if not valid_results:
            return "⚠️ Không tìm thấy tin tức nào trong 6 giờ qua."
        # Gộp các kết quả lại (có thể gửi thẳng)
        combined = "\n\n---\n\n".join(valid_results)
        # Cắt nếu quá dài
        if len(combined) > 3900:
            combined = combined[:3900] + "..."
        return combined

# =========================
# TAVILY + GROQ (CHÍNH) - HỖ TRỢ SONG SONG
# =========================
async def search_tavily(session: aiohttp.ClientSession, query: str, start: datetime, end: datetime) -> Optional[Dict[str, Any]]:
    if not TAVILY_API_KEY:
        return None
    try:
        start_utc = start - timedelta(hours=Config.TIMEZONE_OFFSET)
        after_ts = int(start_utc.timestamp())
        payload = {
            "api_key": TAVILY_API_KEY,
            "query": f"{query} after:{after_ts}",
            "search_depth": "advanced",
            "max_results": 6,
            "include_answer": True
        }
        async with session.post("https://api.tavily.com/search", json=payload, timeout=30) as resp:
            return await resp.json()
    except Exception as e:
        logger.error(f"Tavily error: {e}")
        return None

async def summarize_groq(session: aiohttp.ClientSession, search_data: Dict[str, Any], start: datetime, end: datetime, topic: str) -> str:
    if not GROQ_API_KEY or not search_data:
        return ""
    time_range_str = format_time_range(start, end)
    system_prompt = f"""Bạn là chuyên gia tổng hợp tin công nghệ (tiếng Việt). Dựa vào dữ liệu tìm kiếm cho chủ đề "{topic}" trong khung {time_range_str}, hãy:
- Chọn 2-3 tin HOT nhất (có thể ít hơn nếu không đủ).
- Mỗi tin: tiêu đề, tóm tắt 2 câu, link nguồn.
- Trình bày markdown, có emoji.
- Nếu không có tin: trả về chuỗi rỗng."""
    user_content = f"Khung giờ: {time_range_str}\nTổng quan: {search_data.get('answer', '')}\n"
    for idx, res in enumerate(search_data.get('results', [])[:6], 1):
        user_content += f"\n{idx}. {res['title']}\n   {res['content'][:400]}\n   🔗 {res['url']}\n"
    try:
        async with session.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_content}],
                "temperature": 0.5,
                "max_tokens": 1000
            },
            timeout=60
        ) as resp:
            data = await resp.json()
            return data["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"Groq error: {e}")
        return ""

async def search_single_topic_tavily(session: aiohttp.ClientSession, topic: str, start: datetime, end: datetime) -> str:
    """Tìm kiếm một chủ đề bằng Tavily + Groq"""
    res = await search_tavily(session, topic, start, end)
    if res and res.get('results'):
        summary = await summarize_groq(session, res, start, end, topic)
        return summary if summary else ""
    return ""

async def get_news_tavily_groq_parallel(start: datetime, end: datetime) -> str:
    """Chạy song song nhiều chủ đề với Tavily+Groq, tổng hợp kết quả"""
    async with aiohttp.ClientSession() as session:
        tasks = []
        for topic in Config.SEARCH_TOPICS:
            tasks.append(search_single_topic_tavily(session, topic, start, end))
        results = await asyncio.gather(*tasks)
        valid = [r for r in results if r and len(r) > 50]
        if not valid:
            return ""
        combined = "\n\n---\n\n".join(valid)
        return combined if len(combined) > 100 else ""

# =========================
# FALLBACK TAVILY ONLY (KHÔNG AI)
# =========================
async def get_news_tavily_only_parallel(start: datetime, end: datetime) -> str:
    if not TAVILY_API_KEY:
        return "⚠️ Bot đang bảo trì. Vui lòng thử lại sau."
    async with aiohttp.ClientSession() as session:
        tasks = []
        for topic in Config.SEARCH_TOPICS:
            tasks.append(search_tavily(session, topic, start, end))
        search_results = await asyncio.gather(*tasks)
        all_articles = ""
        for res in search_results:
            if res and res.get('results'):
                for item in res.get('results', [])[:2]:
                    all_articles += f"\n📰 {item['title']}\n{item['content'][:300]}\n🔗 {item['url']}\n---\n"
        if all_articles:
            time_range_str = format_time_range(start, end)
            return f"# 🌍 TECH DIGEST (không AI)\nKhung: {time_range_str}\n{all_articles}"
        return f"⚠️ Không tìm thấy tin tức trong {format_time_range(start, end)}."

# =========================
# MAIN DIGEST - TỔNG HỢP
# =========================
async def build_digest(start: datetime, end: datetime) -> str:
    logger.info(f"Build digest from {start} to {end}")
    # 1. Thử Tavily+Groq song song
    if TAVILY_API_KEY and GROQ_API_KEY:
        try:
            news = await get_news_tavily_groq_parallel(start, end)
            if news and len(news) > 200:
                return news
        except Exception as e:
            logger.error(f"Tavily+Groq parallel error: {e}")
    # 2. Thử API compatible song song
    if API_BASE and API_KEY:
        try:
            news = await get_news_compatible_parallel(start, end)
            if news and len(news) > 100:
                return news
        except Exception as e:
            logger.error(f"API compatible parallel error: {e}")
    # 3. Fallback Tavily only (không AI)
    if TAVILY_API_KEY:
        return await get_news_tavily_only_parallel(start, end)
    return f"⚠️ Bot đang bảo trì. Không thể lấy tin cho khung {format_time_range(start, end)}."

# =========================
# TASK TỰ ĐỘNG THEO MỐC 6 GIỜ
# =========================
async def auto_news_scheduler():
    global last_sent_slot
    await bot.wait_until_ready()
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        logger.error(f"Không tìm thấy channel {CHANNEL_ID}")
        return
    while not bot.is_closed():
        current_slot = get_current_slot()
        if current_slot != last_sent_slot:
            start, end = get_slot_range(current_slot)
            logger.info(f"Tự động gửi mốc {current_slot}:00 → {start.strftime('%H:%M %d/%m')} - {end.strftime('%H:%M %d/%m')}")
            digest = await build_digest(start, end)
            if len(digest) > Config.MAX_DISCORD_MESSAGE_LENGTH:
                digest = digest[:Config.MAX_DISCORD_MESSAGE_LENGTH - 100] + "\n\n... (cắt)"
            embed = discord.Embed(
                title=f"🚀 TECH DIGEST • {start.strftime('%H:%M')} – {end.strftime('%H:%M')} (giờ VN)",
                description=digest,
                color=0x00ffcc,
                timestamp=datetime.utcnow()
            )
            embed.set_footer(text=f"🤖 Mốc {current_slot}:00 | Nguồn: Đa luồng (Tavily/Groq hoặc API Compatible)")
            try:
                await channel.send(embed=embed)
                last_sent_slot = current_slot
                logger.info(f"Đã gửi thành công mốc {current_slot}:00")
            except Exception as e:
                logger.error(f"Lỗi gửi tin: {e}")
        await wait_until_next_slot()

# =========================
# DISCORD COMMANDS
# =========================
@bot.command(name="news", aliases=["tin"])
async def get_news(ctx):
    if ctx.channel.id != CHANNEL_ID:
        await ctx.send(f"❌ Chỉ hoạt động trong kênh <#{CHANNEL_ID}>")
        return
    async with ctx.typing():
        msg = await ctx.send("🔍 **Đang quét tin tức 6 giờ gần nhất...**")
        now = get_vn_now()
        start = now - timedelta(hours=Config.SEARCH_INTERVAL_HOURS)
        end = now
        digest = await build_digest(start, end)
        if len(digest) > 1900:
            parts = [digest[i:i+1900] for i in range(0, len(digest), 1900)]
            await msg.delete()
            for i, part in enumerate(parts):
                await ctx.send(f"```markdown\n{part}\n```" if i == 0 else part)
        else:
            await msg.edit(content=f"```markdown\n{digest}\n```")

@bot.command(name="ping")
async def ping(ctx):
    if ctx.channel.id != CHANNEL_ID:
        await ctx.send(f"❌ Chỉ hoạt động trong kênh <#{CHANNEL_ID}>")
        return
    await ctx.send(f"🏓 Pong! `{round(bot.latency * 1000)}ms`")

@bot.command(name="status")
async def bot_status(ctx):
    if ctx.channel.id != CHANNEL_ID:
        await ctx.send(f"❌ Chỉ hoạt động trong kênh <#{CHANNEL_ID}>")
        return
    vn_now = get_vn_now()
    current_slot = get_current_slot()
    next_slot_time = get_next_slot_time()
    embed = discord.Embed(title="🤖 Bot Status", color=0x00ff00, timestamp=datetime.utcnow())
    embed.add_field(name="Giờ VN hiện tại", value=vn_now.strftime('%H:%M:%S %d/%m/%Y'), inline=False)
    embed.add_field(name="Mốc hiện tại", value=f"{current_slot}:00", inline=True)
    embed.add_field(name="Mốc tiếp theo", value=next_slot_time.strftime('%H:%M'), inline=True)
    embed.add_field(name="Đã gửi mốc", value=last_sent_slot if last_sent_slot != -1 else "Chưa", inline=True)
    embed.add_field(name="Tavily API", value="✅" if TAVILY_API_KEY else "❌", inline=True)
    embed.add_field(name="Groq API", value="✅" if GROQ_API_KEY else "❌", inline=True)
    embed.add_field(name="API Compatible", value="✅" if (API_BASE and API_KEY) else "❌", inline=True)
    embed.add_field(name="Latency", value=f"`{round(bot.latency * 1000)}ms`", inline=True)
    await ctx.send(embed=embed)

@bot.event
async def on_ready():
    logger.info(f"✅ Bot online: {bot.user} (ID: {bot.user.id})")
    logger.info(f"Channel: {CHANNEL_ID}")
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="Tin theo mốc 6h"))
    bot.loop.create_task(auto_news_scheduler())
    logger.info("Auto news scheduler started (mốc 0,6,12,18 giờ VN, interval 6h, song song nhiều luồng)")

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CommandNotFound):
        await ctx.send("❓ Lệnh không tồn tại. Dùng `!news`, `!ping`, `!status`")
    else:
        logger.error(f"Command error: {error}")
        await ctx.send(f"⚠️ Lỗi: {str(error)[:100]}")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        logger.error("Thiếu DISCORD_TOKEN")
        exit(1)
    logger.info("Starting bot...")
    bot.run(DISCORD_TOKEN)
