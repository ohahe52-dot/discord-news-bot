import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
import aiohttp
import asyncio
import os
import logging
import random
from datetime import datetime
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

# Tavily + Groq (chính)
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")

# OpenAI Compatible API (fallback)
API_BASE = os.getenv("API_BASE")
API_KEY = os.getenv("API_KEY")
MODEL_NAME = os.getenv("MODEL_NAME", "DeepSeekV2/deepseek-v4-pro-search")

# =========================
# CONFIGURATION
# =========================
class Config:
    SEARCH_INTERVAL_HOURS = 3
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 5
    API_TIMEOUT_SECONDS = 60
    MAX_DISCORD_MESSAGE_LENGTH = 3900
    TEMPERATURE = 0.3

# =========================
# DISCORD BOT
# =========================
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# =========================
# TAVILY + GROQ (CHÍNH, CÓ SEARCH THẬT)
# =========================
async def search_tavily(session: aiohttp.ClientSession, query: str) -> Optional[Dict[str, Any]]:
    """Tìm kiếm tin tức bằng Tavily API (chính xác, có link)"""
    if not TAVILY_API_KEY:
        return None
    try:
        # Chỉ lấy tin trong 3 giờ qua
        three_hours_ago = int(datetime.now().timestamp() - 10800)
        payload = {
            "api_key": TAVILY_API_KEY,
            "query": f"{query} after:{three_hours_ago}",
            "search_depth": "advanced",
            "max_results": 6,
            "include_answer": True
        }
        async with session.post("https://api.tavily.com/search", json=payload, timeout=30) as resp:
            if resp.status == 200:
                return await resp.json()
            else:
                logger.error(f"Tavily status {resp.status}")
                return None
    except Exception as e:
        logger.error(f"Tavily error: {e}")
        return None

async def summarize_groq(session: aiohttp.ClientSession, search_data: Dict[str, Any]) -> str:
    """Dùng Groq (Llama) để tóm tắt tin tức"""
    if not GROQ_API_KEY or not search_data:
        return ""
    
    system_prompt = """Bạn là chuyên gia tổng hợp tin công nghệ. Dựa vào dữ liệu bên dưới, hãy:
- Chọn 3-5 tin HOT nhất (ưu tiên tin trong 3 giờ qua)
- Mỗi tin: tiêu đề, tóm tắt 2-3 câu, kèm link nguồn thật
- Trình bày bằng markdown, có emoji, dễ đọc
- Nếu không có tin mới: trả lời "📭 Chưa có tin nổi bật trong 3 giờ qua."
- Không được bịa link, chỉ dùng link từ dữ liệu cung cấp."""
    
    user_content = f"Thời gian hiện tại: {datetime.now().strftime('%H:%M %d/%m/%Y')}\n"
    user_content += f"Tổng quan: {search_data.get('answer', 'Không có tóm tắt')}\n\n"
    for idx, res in enumerate(search_data.get('results', [])[:8], 1):
        user_content += f"{idx}. **{res['title']}**\n   {res['content'][:400]}\n   🔗 {res['url']}\n\n"
    
    try:
        async with session.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}"},
            json={
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_content}
                ],
                "temperature": 0.5,
                "max_tokens": 1200
            },
            timeout=60
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data["choices"][0]["message"]["content"]
            else:
                logger.error(f"Groq status {resp.status}")
                return ""
    except Exception as e:
        logger.error(f"Groq summarizer error: {e}")
        return ""

async def get_news_tavily_groq() -> str:
    """Lấy tin tức qua Tavily + Groq (phương thức chính)"""
    async with aiohttp.ClientSession() as session:
        # Các chủ đề tìm kiếm đa dạng
        topics = [
            "latest AI breakthrough news",
            "new GPU CPU announcement today",
            "technology innovation this hour",
            "cybersecurity incident report"
        ]
        search_result = None
        for topic in topics:
            result = await search_tavily(session, topic)
            if result and result.get('results'):
                search_result = result
                break
            await asyncio.sleep(1)
        
        if not search_result:
            logger.warning("Tavily returned no results")
            return ""
        
        summary = await summarize_groq(session, search_result)
        if summary and len(summary) > 100:
            return summary
        return ""

# =========================
# FALLBACK: API COMPATIBLE (MODEL SEARCH CŨ)
# =========================
async def make_api_call(
    session: aiohttp.ClientSession,
    messages: list,
    retry_count: int = 0
) -> Optional[Dict[str, Any]]:
    """Gọi API compatible (dùng khi Tavily+Groq lỗi)"""
    if not API_BASE or not API_KEY:
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
            headers={
                "Authorization": f"Bearer {API_KEY}",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=Config.API_TIMEOUT_SECONDS
        ) as resp:
            if resp.status == 200:
                return await resp.json()
            elif resp.status == 429 and retry_count < Config.MAX_RETRIES - 1:
                await asyncio.sleep(Config.RETRY_DELAY_SECONDS * (retry_count + 1))
                return await make_api_call(session, messages, retry_count + 1)
            else:
                logger.error(f"API fallback error {resp.status}")
                return None
    except Exception as e:
        logger.error(f"API fallback exception: {e}")
        return None

async def get_news_fallback() -> str:
    """Dùng API compatible (model search cũ) làm fallback"""
    if not API_BASE or not API_KEY:
        return "⚠️ Bot chưa được cấu hình đầy đủ. Vui lòng thêm API_BASE và API_KEY."
    
    current_time = datetime.now()
    system_prompt = f"""Bạn là AI tổng hợp tin công nghệ.
    THỜI GIAN: {current_time.strftime('%d/%m/%Y %H:%M')}
    Hãy tìm tin tức mới nhất trong 3 giờ qua (từ internet nếu có thể).
    Output markdown, mỗi tin có tiêu đề, tóm tắt, link nguồn.
    Nếu không có tin mới, nói "Chưa có tin nổi bật." """
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": "Tìm tin công nghệ hot nhất 3 giờ qua."}
    ]
    
    async with aiohttp.ClientSession() as session:
        result = await make_api_call(session, messages)
        if result and "choices" in result:
            return result["choices"][0]["message"]["content"]
        return "⚠️ Hiện không thể tổng hợp tin tức. Vui lòng thử lại sau."

# =========================
# HÀM CHÍNH BUILD DIGEST
# =========================
async def build_digest() -> str:
    """Ưu tiên Tavily+Groq, nếu lỗi thì fallback API compatible"""
    logger.info("Building news digest...")
    
    # Thử Tavily + Groq nếu có key
    if TAVILY_API_KEY and GROQ_API_KEY:
        tg_news = await get_news_tavily_groq()
        if tg_news and len(tg_news) > 100:
            logger.info("Success using Tavily+Groq")
            return tg_news
        else:
            logger.warning("Tavily+Groq returned nothing, falling back to API compatible")
    else:
        logger.info("Tavily or Groq key missing, using API compatible only")
    
    # Fallback
    fallback = await get_news_fallback()
    return fallback if fallback else "⚠️ Không thể lấy tin tức lúc này."

# =========================
# DISCORD TASKS
# =========================
@tasks.loop(hours=Config.SEARCH_INTERVAL_HOURS)
async def auto_news():
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        logger.error(f"Channel {CHANNEL_ID} not found")
        return
    try:
        loading = await channel.send("🌐 **AI đang quét tin tức toàn cầu...**")
        digest = await build_digest()
        if len(digest) > Config.MAX_DISCORD_MESSAGE_LENGTH:
            digest = digest[:Config.MAX_DISCORD_MESSAGE_LENGTH - 100] + "\n... (còn tiếp)"
        
        embed = discord.Embed(
            title="🚀 **TECH DIGEST**",
            description=digest,
            color=0x00ffcc,
            timestamp=datetime.now()
        )
        embed.set_footer(text=f"🤖 Cập nhật mỗi {Config.SEARCH_INTERVAL_HOURS} giờ | Nguồn: Tavily+Groq (ưu tiên)")
        await loading.delete()
        await channel.send(embed=embed)
        logger.info("Auto news sent")
    except Exception as e:
        logger.error(f"Auto news error: {e}")
        await channel.send(f"⚠️ Lỗi: {str(e)[:200]}")

@auto_news.before_loop
async def before_auto():
    await bot.wait_until_ready()
    logger.info("Bot ready, auto news starts in 10s")
    await asyncio.sleep(10)

@bot.command(name="news", aliases=["tin"])
async def get_news(ctx):
    if ctx.channel.id != CHANNEL_ID:
        await ctx.send(f"❌ Chỉ hoạt động trong kênh <#{CHANNEL_ID}>")
        return
    async with ctx.typing():
        msg = await ctx.send("🔍 Đang tìm kiếm...")
        digest = await build_digest()
        if len(digest) > 1900:
            digest = digest[:1900] + "..."
        await msg.edit(content=digest)

@bot.command(name="ping")
async def ping(ctx):
    if ctx.channel.id != CHANNEL_ID:
        await ctx.send(f"❌ Chỉ hoạt động trong kênh <#{CHANNEL_ID}>")
        return
    await ctx.send(f"🏓 Pong! `{round(bot.latency * 1000)}ms`")

@bot.command(name="status")
async def status(ctx):
    if ctx.channel.id != CHANNEL_ID:
        await ctx.send(f"❌ Chỉ hoạt động trong kênh <#{CHANNEL_ID}>")
        return
    embed = discord.Embed(title="🤖 Bot Status", color=0x00ff00, timestamp=datetime.now())
    embed.add_field(name="Model chính", value="Tavily + Groq (Llama 3.3)", inline=True)
    embed.add_field(name="Fallback", value=f"`{MODEL_NAME or 'Không có'}`", inline=True)
    embed.add_field(name="Interval", value=f"{Config.SEARCH_INTERVAL_HOURS}h", inline=True)
    embed.add_field(name="Latency", value=f"{round(bot.latency * 1000)}ms", inline=True)
    await ctx.send(embed=embed)

@bot.event
async def on_ready():
    logger.info(f"✅ Bot online: {bot.user} (ID: {bot.user.id})")
    logger.info(f"Tavily key: {'Có' if TAVILY_API_KEY else 'Không'}")
    logger.info(f"Groq key: {'Có' if GROQ_API_KEY else 'Không'}")
    if not auto_news.is_running():
        auto_news.start()
    await bot.change_presence(activity=discord.Game(name="!news | 3h tự động"))

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        logger.error("Thiếu DISCORD_TOKEN")
        exit(1)
    bot.run(DISCORD_TOKEN)
