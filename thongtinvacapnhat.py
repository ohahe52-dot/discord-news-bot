import discord
from discord.ext import commands
from dotenv import load_dotenv
import aiohttp
import asyncio
import os
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

# =========================
# LOGGING SETUP
# =========================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =========================
# LOAD ENV
# =========================
load_dotenv()

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID"))

# API compatible (fallback)
API_BASE = os.getenv("API_BASE")
API_KEY = os.getenv("API_KEY")
MODEL_NAME = os.getenv("MODEL_NAME")

# Tavily + Groq (chính)
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

# =========================
# CONFIGURATION
# =========================
class Config:
    SEARCH_INTERVAL_HOURS = 3          # mốc 3 giờ
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 5
    API_TIMEOUT_SECONDS = 60
    MAX_DISCORD_MESSAGE_LENGTH = 3900
    TEMPERATURE = 0.3

    # Múi giờ Việt Nam (UTC+7)
    TIMEZONE_OFFSET = 7

    # Các mốc giờ cố định (0,3,6,9,12,15,18,21) theo giờ VN
    SLOTS = [0, 3, 6, 9, 12, 15, 18, 21]

# =========================
# DISCORD BOT
# =========================
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Lưu mốc đã gửi (trong RAM, có thể mất khi restart nhưng không ảnh hưởng nhiều)
last_sent_slot = -1

# =========================
# UTILS: THỜI GIAN VN
# =========================
def get_vn_now() -> datetime:
    """Trả về datetime hiện tại theo múi giờ UTC+7 (naive)"""
    return datetime.utcnow() + timedelta(hours=Config.TIMEZONE_OFFSET)

def format_time_range(start: datetime, end: datetime) -> str:
    """Format thời gian dùng cho prompt: 'HH:MM DD/MM/YYYY'"""
    return f"{start.strftime('%H:%M %d/%m/%Y')} → {end.strftime('%H:%M %d/%m/%Y')}"

def get_current_slot() -> int:
    """Trả về mốc giờ (0-21) hiện tại theo giờ VN"""
    now = get_vn_now()
    hour = now.hour
    for slot in sorted(Config.SLOTS, reverse=True):
        if hour >= slot:
            return slot
    return Config.SLOTS[-1]

def get_slot_range(slot: int) -> tuple:
    """
    Với một mốc giờ (0,3,...,21), trả về (start_time, end_time) naive (VN timezone)
    Ví dụ slot=6 -> start=03:00, end=06:00 cùng ngày.
    slot=0 -> start=21:00 hôm qua, end=00:00 hôm nay.
    """
    now = get_vn_now()
    end = now.replace(hour=slot, minute=0, second=0, microsecond=0)
    if slot == 0:
        start = end - timedelta(days=1, hours=3)  # 21:00 hôm qua
    else:
        start = end - timedelta(hours=3)
    # Đảm bảo start không lớn hơn end (trường hợp gần mốc)
    if start > end:
        start = end - timedelta(hours=3)
    return start, end

def get_next_slot_time() -> datetime:
    """Thời gian của mốc tiếp theo (VN)"""
    now = get_vn_now()
    current_slot = get_current_slot()
    for slot in Config.SLOTS:
        if slot > current_slot:
            next_slot = slot
            break
    else:
        next_slot = 0
        return now.replace(hour=next_slot, minute=0, second=0, microsecond=0) + timedelta(days=1)
    return now.replace(hour=next_slot, minute=0, second=0, microsecond=0)

async def wait_until_next_slot():
    """Sleep đến mốc tiếp theo"""
    next_time = get_next_slot_time()
    now = get_vn_now()
    wait_sec = (next_time - now).total_seconds()
    if wait_sec < 0:
        wait_sec = 0
    logger.info(f"Chờ {wait_sec:.0f}s đến mốc {next_time.strftime('%H:%M')}")
    await asyncio.sleep(wait_sec)

# =========================
# API COMPATIBLE (FALLBACK) – có truyền thời gian
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

async def get_news_compatible(start: datetime, end: datetime) -> str:
    """Dùng API compatible, tìm tin trong khoảng start → end (VN time)"""
    async with aiohttp.ClientSession() as session:
        time_range_str = format_time_range(start, end)
        system_prompt = f"""Bạn là AI săn tin công nghệ, có khả năng tìm kiếm internet.
Yêu cầu: Tìm các bài báo/tin tức công nghệ được công bố trong khung giờ CHÍNH XÁC sau:
Khoảng thời gian: {time_range_str} (giờ Việt Nam, UTC+7)
Chỉ lấy tin có thời gian đăng nằm trong khoảng này. Ưu tiên tin nóng về AI, phần cứng, robot, bảo mật, không gian.
Mỗi tin cần có: tiêu đề, tóm tắt 2-3 câu, link nguồn.
Trình bày markdown với emoji, rõ ràng."""
        user_prompt = f"Hãy tìm tin công nghệ mới nhất trong khoảng {time_range_str}. Đảm bảo mỗi tin đều có link thật."
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        result = await make_api_call(session, messages)
        if result and "choices" in result:
            content = result["choices"][0]["message"]["content"]
            return content if len(content) > 100 else ""
        return ""

# =========================
# TAVILY + GROQ (ƯU TIÊN) – truyền khoảng thời gian cụ thể
# =========================
async def search_tavily(session: aiohttp.ClientSession, query: str, start: datetime, end: datetime) -> Optional[Dict[str, Any]]:
    if not TAVILY_API_KEY:
        return None
    try:
        # Chuyển start thành timestamp (giây)
        # Lưu ý: start, end là naive VN time, nhưng Tavily hiểu UTC.
        # Ta trừ đi 7h để đưa về UTC (vì start, end đang là VN)
        start_utc = start - timedelta(hours=Config.TIMEZONE_OFFSET)
        after_ts = int(start_utc.timestamp())
        payload = {
            "api_key": TAVILY_API_KEY,
            "query": f"{query} after:{after_ts}",
            "search_depth": "advanced",
            "max_results": 8,
            "include_answer": True
        }
        async with session.post("https://api.tavily.com/search", json=payload, timeout=30) as resp:
            return await resp.json()
    except Exception as e:
        logger.error(f"Tavily error: {e}")
        return None

async def summarize_groq(session: aiohttp.ClientSession, search_data: Dict[str, Any], start: datetime, end: datetime) -> str:
    if not GROQ_API_KEY or not search_data:
        return ""
    time_range_str = format_time_range(start, end)
    system_prompt = f"""Bạn là chuyên gia tổng hợp tin công nghệ. Dựa vào dữ liệu tìm kiếm bên dưới (từ Tavily), hãy:
- Chọn 3-5 tin HOT nhất được đăng trong khung thời gian: {time_range_str} (giờ Việt Nam).
- Mỗi tin cần có: tiêu đề, tóm tắt 2 câu ngắn gọn, link nguồn.
- Trình bày bằng markdown, có emoji, dễ đọc.
- Nếu không có tin nào trong khung giờ này, trả lời: '📭 Không tìm thấy tin nổi bật trong khoảng {time_range_str}.' """
    user_content = f"Khung giờ yêu cầu: {time_range_str}\n"
    user_content += f"Tổng quan Tavily: {search_data.get('answer', '')}\n"
    for idx, res in enumerate(search_data.get('results', [])[:8], 1):
        user_content += f"\n{idx}. {res['title']}\n   {res['content'][:400]}\n   🔗 {res['url']}\n"
    try:
        async with session.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_content}],
                "temperature": 0.5,
                "max_tokens": 1200
            },
            timeout=60
        ) as resp:
            data = await resp.json()
            return data["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"Groq error: {e}")
        return ""

async def get_news_tavily_groq(start: datetime, end: datetime) -> str:
    """Lấy tin bằng Tavily+Groq trong khoảng thời gian start→end"""
    async with aiohttp.ClientSession() as session:
        topics = ["technology breakthrough", "AI news", "GPU CPU release", "cybersecurity", "space tech"]
        search_result = None
        for topic in topics:
            res = await search_tavily(session, topic, start, end)
            if res and res.get('results'):
                search_result = res
                break
            await asyncio.sleep(1)
        if not search_result:
            return ""
        summary = await summarize_groq(session, search_result, start, end)
        return summary if summary else ""

async def get_news_tavily_only(start: datetime, end: datetime) -> str:
    """Fallback chỉ Tavily (không AI)"""
    if not TAVILY_API_KEY:
        return "⚠️ Bot đang bảo trì. Vui lòng thử lại sau 30 phút."
    async with aiohttp.ClientSession() as session:
        topics = ["technology news", "AI breakthrough", "hardware release"]
        all_articles = ""
        for topic in topics:
            res = await search_tavily(session, topic, start, end)
            if res and res.get('results'):
                for item in res.get('results', [])[:3]:
                    all_articles += f"\n📰 {item['title']}\n{item['content'][:300]}\n🔗 {item['url']}\n---\n"
            await asyncio.sleep(1)
        if all_articles:
            time_range_str = format_time_range(start, end)
            return f"# 🌍 TECH DIGEST (Tạm thời - không AI)\nKhung: {time_range_str}\n\n{all_articles}"
        return f"⚠️ Không tìm thấy tin tức trong khoảng {format_time_range(start, end)}."

# =========================
# MAIN DIGEST – NHẬN start, end
# =========================
async def build_digest(start: datetime, end: datetime) -> str:
    """Tổng hợp tin trong khoảng thời gian start→end, ưu tiên Tavily+Groq"""
    logger.info(f"Build digest from {start} to {end}")
    if TAVILY_API_KEY and GROQ_API_KEY:
        try:
            news = await get_news_tavily_groq(start, end)
            if news and len(news) > 100:
                return news
        except Exception as e:
            logger.error(f"Tavily+Groq error: {e}")
    if API_BASE and API_KEY:
        news = await get_news_compatible(start, end)
        if news and len(news) > 100:
            return news
    if TAVILY_API_KEY:
        return await get_news_tavily_only(start, end)
    return f"⚠️ Bot đang bảo trì. Không thể lấy tin cho khung {format_time_range(start, end)}."

# =========================
# TASK TỰ ĐỘNG THEO MỐC 3 GIỜ CỐ ĐỊNH
# =========================
async def auto_news_scheduler():
    """Chạy nền: đợi đến từng mốc giờ, gửi tin cho khung 3 giờ vừa qua"""
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
            logger.info(f"Tự động gửi tin cho mốc {current_slot}:00 → khoảng {start.strftime('%H:%M %d/%m')} - {end.strftime('%H:%M %d/%m')}")
            digest = await build_digest(start, end)
            if len(digest) > Config.MAX_DISCORD_MESSAGE_LENGTH:
                digest = digest[:Config.MAX_DISCORD_MESSAGE_LENGTH - 100] + "\n\n... (cắt)"
            embed = discord.Embed(
                title=f"🚀 TECH DIGEST • {start.strftime('%H:%M')} – {end.strftime('%H:%M')} (giờ VN)",
                description=digest,
                color=0x00ffcc,
                timestamp=datetime.utcnow()
            )
            embed.set_footer(text=f"🤖 Mốc {current_slot}:00 | Nguồn: Tavily+Groq")
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
    """Lệnh thủ công: tìm tin trong 3 giờ qua (từ lúc gọi lệnh)"""
    if ctx.channel.id != CHANNEL_ID:
        await ctx.send(f"❌ Chỉ hoạt động trong kênh <#{CHANNEL_ID}>")
        return
    async with ctx.typing():
        msg = await ctx.send("🔍 **Đang quét tin tức 3 giờ gần nhất...**")
        now = get_vn_now()
        start = now - timedelta(hours=3)
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
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="Tin theo mốc 3h"))
    bot.loop.create_task(auto_news_scheduler())
    logger.info("Auto news scheduler started (mốc 0,3,6,9,12,15,18,21 giờ VN)")

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
