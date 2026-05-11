import discord
from discord.ext import commands
from dotenv import load_dotenv
import aiohttp
import asyncio
import os
import logging
import random
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List

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
    SEARCH_INTERVAL_HOURS = 6           # *** ĐÃ ĐỔI THÀNH 6 GIỜ ***
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 5
    API_TIMEOUT_SECONDS = 90
    MAX_DISCORD_MESSAGE_LENGTH = 3900
    TEMPERATURE = 0.3
    TIMEZONE_OFFSET = 7

    # Các mốc giờ cố định (0, 6, 12, 18) theo giờ VN
    SLOTS = [0, 6, 12, 18]

    # Danh sách các chủ đề tìm kiếm đa dạng (sẽ chạy song song)
    SEARCH_TOPICS = [
        "AI news worldwide last 6 hours",
        "AI news Vietnam last 6 hours",
        "OpenAI announcement today",
        "Google AI news today",
        "Meta AI news today",
        "Microsoft AI news today",
        "NVIDIA AI news today",
        "artificial intelligence breakthrough",
        "Việt Nam AI trí tuệ nhân tạo mới nhất",
        "AI robotics hardware release"
    ]

# =========================
# DISCORD BOT
# =========================
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

last_sent_slot = -1

# =========================
# UTILS: THỜI GIAN VN
# =========================
def get_vn_now() -> datetime:
    return datetime.utcnow() + timedelta(hours=Config.TIMEZONE_OFFSET)

def format_time_range(start: datetime, end: datetime) -> str:
    return f"{start.strftime('%H:%M %d/%m/%Y')} → {end.strftime('%H:%M %d/%m/%Y')}"

def get_current_slot() -> int:
    hour = get_vn_now().hour
    # Tìm mốc gần nhất nhưng không vượt quá giờ hiện tại
    for slot in sorted(Config.SLOTS, reverse=True):
        if hour >= slot:
            return slot
    return Config.SLOTS[-1]

def get_slot_range(slot: int):
    """Trả về khoảng thời gian 6 giờ tương ứng với mốc (slot)"""
    now = get_vn_now()
    end = now.replace(hour=slot, minute=0, second=0, microsecond=0)
    # Nếu mốc là 0 (nửa đêm), start là 18h hôm qua
    if slot == 0:
        start = end - timedelta(hours=6)
    else:
        start = end - timedelta(hours=6)
    # Xử lý trường hợp gần mốc
    if start > end:
        start = end - timedelta(hours=6)
    return start, end

def get_next_slot_time():
    now = get_vn_now()
    current_slot = get_current_slot()
    for slot in Config.SLOTS:
        if slot > current_slot:
            return now.replace(hour=slot, minute=0, second=0, microsecond=0)
    # Nếu đã qua mốc cuối (18h), chuyển sang mốc 0h ngày mai
    next_day = now + timedelta(days=1)
    return next_day.replace(hour=0, minute=0, second=0, microsecond=0)

async def wait_until_next_slot():
    next_time = get_next_slot_time()
    now = get_vn_now()
    wait_sec = (next_time - now).total_seconds()
    if wait_sec < 0:
        wait_sec = 0
    logger.info(f"Chờ {wait_sec:.0f}s đến mốc {next_time.strftime('%H:%M')}")
    await asyncio.sleep(wait_sec)

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
