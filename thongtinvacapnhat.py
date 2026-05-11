import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv
import aiohttp
import asyncio
import os
import logging
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

# OpenAI Compatible API
API_BASE = os.getenv("API_BASE")
API_KEY = os.getenv("API_KEY")
MODEL_NAME = "MODEL_NAME"  # Model mới, nhanh hơn

# Optional: Tavily fallback (nếu model search lỗi)
TAVILY_API_KEY = os.getenv("TAVILY_API_KEY", "")

# =========================
# CONFIGURATION
# =========================
class Config:
    SEARCH_INTERVAL_HOURS = 3
    MAX_RETRIES = 3
    RETRY_DELAY_SECONDS = 5
    API_TIMEOUT_SECONDS = 60
    MAX_DISCORD_MESSAGE_LENGTH = 3900
    TEMPERATURE = 0.3  # Thấp hơn để factual, tránh ảo
    
    # Topics đa dạng hóa
    SEARCH_TOPICS = [
        "AI breakthrough today",
        "latest artificial intelligence news 2025",
        "new GPU technology release",
        "new CPU processor announcement", 
        "technology breakthrough this week",
        "robotics innovation news",
        "quantum computing progress",
        "NVIDIA AMD Intel latest news",
        "OpenAI Google DeepMind Anthropic updates",
        "smartphone launch news",
        "cybersecurity incident report",
        "future tech inventions",
        "space technology news",
        "biotechnology breakthrough",
        "green energy technology"
    ]

# =========================
# DISCORD BOT
# =========================
intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents)

# =========================
# API UTILITIES
# =========================
async def make_api_call(
    session: aiohttp.ClientSession,
    messages: list,
    retry_count: int = 0
) -> Optional[Dict[str, Any]]:
    """Gọi API với retry logic"""
    
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
                data = await resp.json()
                return data
            elif resp.status == 429:  # Rate limit
                logger.warning(f"Rate limited, retry {retry_count + 1}/{Config.MAX_RETRIES}")
                if retry_count < Config.MAX_RETRIES - 1:
                    await asyncio.sleep(Config.RETRY_DELAY_SECONDS * (retry_count + 1))
                    return await make_api_call(session, messages, retry_count + 1)
            else:
                error_text = await resp.text()
                logger.error(f"API Error {resp.status}: {error_text}")
                return None
                
    except asyncio.TimeoutError:
        logger.error(f"API timeout after {Config.API_TIMEOUT_SECONDS}s")
        return None
    except Exception as e:
        logger.error(f"API call failed: {str(e)}")
        return None

# =========================
# DIRECT AI SEARCH (Model built-in search)
# =========================
async def get_news_direct() -> str:
    """Dùng model deepseek-v4-pro-search-nothinking để search trực tiếp"""
    
    current_time = datetime.now()
    time_str = current_time.strftime('%H:%M')
    date_str = current_time.strftime('%d/%m/%Y')
    
    system_prompt = f"""Bạn là AI chuyên săn tin công nghệ với khả năng tìm kiếm internet realtime.

THỜI GIAN HIỆN TẠI: {date_str} {time_str}
YÊU CẦU: Chỉ lấy tin tức trong 3 GIỜ GẦN NHẤT

QUY TRÌNH BẮT BUỘC:
1. Sử dụng chức năng search để tìm tin mới nhất
2. Kiểm tra timestamp của từng bài báo
3. Chỉ giữ lại tin có thời gian ≤ 3 giờ

LĨNH VỰC ƯU TIÊN (theo thứ tự quan trọng):
🔥 AI/ML (OpenAI, Google DeepMind, Anthropic, Meta AI)
💻 Phần cứng (GPU: NVIDIA/AMD/Intel, CPU mới)
🤖 Robotics & Automation
📱 Smartphone & Mobile tech
🔐 Cybersecurity breaches & patches
⚛️ Quantum computing
🚀 Space tech

FORMAT OUTPUT:

# 🌍 TECH BREAKING NEWS
*Cập nhật: {time_str} - {date_str}*

## 🚨 1. [TIÊU ĐỀ BÀI BÁO]
📝 **Tóm tắt:** [2-3 câu ngắn gọn]
💡 **Tại sao quan trọng:** [1 câu giải thích tác động]
🔗 **Nguồn:** [Link hợp lệ]

## 🚨 2. [TIÊU ĐỀ BÀI BÁO]
...

---
🤖 AI tự động tổng hợp từ internet
📊 Tần suất: Mỗi {Config.SEARCH_INTERVAL_HOURS} giờ
"""

    user_prompt = f"""Tìm và tổng hợp tin tức công nghệ MỚI NHẤT trong 3 giờ qua (từ {current_time.strftime('%H:%M')} đến bây giờ).

QUAN TRỌNG:
- PHẢI kiểm tra thời gian đăng bài
- Bỏ qua tin cũ hơn 3 giờ
- Chọn 3-5 tin HOT nhất
- Mỗi tin PHẢI có link nguồn thật
- Thông tin chính xác, không bịa đặt

Hãy bắt đầu search ngay!"""

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ]
    
    async with aiohttp.ClientSession() as session:
        result = await make_api_call(session, messages)
        
        if result and "choices" in result and len(result["choices"]) > 0:
            content = result["choices"][0]["message"]["content"]
            
            # Thêm footer nếu chưa có
            if "---" not in content[-100:]:
                content += f"\n\n---\n✅ Cập nhật lần cuối: {current_time.strftime('%H:%M:%S %d/%m/%Y')}"
            
            return content
        else:
            # Fallback nếu API lỗi
            logger.warning("API call failed, using fallback mode")
            return await get_news_fallback()

# =========================
# FALLBACK: Tavily + AI Summarizer
# =========================
async def search_web(session: aiohttp.ClientSession, query: str) -> Dict[str, Any]:
    """Fallback search bằng Tavily API"""
    if not TAVILY_API_KEY:
        return {}
    
    try:
        async with session.post(
            "https://api.tavily.com/search",
            json={
                "api_key": TAVILY_API_KEY,
                "query": query,
                "search_depth": "basic",
                "max_results": 5,
                "include_answer": False
            },
            timeout=30
        ) as resp:
            return await resp.json()
    except Exception as e:
        logger.error(f"Tavily search error: {e}")
        return {}

async def summarize_with_ai(session: aiohttp.ClientSession, articles_text: str) -> str:
    """Dùng AI để tóm tắt kết quả search"""
    system_prompt = """Tổng hợp tin tức từ dữ liệu được cung cấp.
    Chọn 3-5 tin quan trọng nhất.
    Format markdown với emoji, tiêu đề, tóm tắt, link nguồn."""
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": f"Dữ liệu tin tức:\n{articles_text}"}
    ]
    
    result = await make_api_call(session, messages)
    
    if result and "choices" in result:
        return result["choices"][0]["message"]["content"]
    return "⚠️ Không thể tổng hợp tin tức lúc này."

async def get_news_fallback() -> str:
    """Fallback khi model search không hoạt động"""
    if not TAVILY_API_KEY:
        return "⚠️ Bot đang bảo trì. Vui lòng thử lại sau 30 phút."
    
    async with aiohttp.ClientSession() as session:
        # Random topics
        import random
        topics = random.sample(Config.SEARCH_TOPICS, 3)
        all_articles = ""
        
        for topic in topics:
            result = await search_web(session, topic)
            for item in result.get("results", [])[:3]:
                all_articles += f"""
TITLE: {item.get('title', '')}
CONTENT: {item.get('content', '')[:500]}
SOURCE: {item.get('url', '')}
---"""
            await asyncio.sleep(1)
        
        if all_articles:
            return await summarize_with_ai(session, all_articles)
        return "⚠️ Không tìm thấy tin tức mới trong 3 giờ qua."

# =========================
# MAIN DIGEST FUNCTION
# =========================
async def build_digest() -> str:
    """Xây dựng bản tin chính"""
    logger.info("Starting news digest build")
    
    try:
        # Thử dùng model search trước
        news_content = await get_news_direct()
        
        # Validate content
        if len(news_content) < 100 or "error" in news_content.lower():
            logger.warning("Direct search returned short/error content, using fallback")
            news_content = await get_news_fallback()
        
        return news_content
        
    except Exception as e:
        logger.error(f"Unexpected error in build_digest: {e}")
        return await get_news_fallback()

# =========================
# DISCORD TASKS
# =========================
@tasks.loop(hours=Config.SEARCH_INTERVAL_HOURS)
async def auto_news():
    """Tự động gửi tin mỗi 3 giờ"""
    
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        logger.error(f"Channel {CHANNEL_ID} not found")
        return
    
    try:
        # Gửi message loading
        loading_msg = await channel.send("🌐 **AI đang quét internet toàn cầu...**\n⏳ Vui lòng chờ trong giây lát (10-30s)")
        
        # Lấy tin tức
        digest = await build_digest()
        
        # Giới hạn độ dài cho Discord
        if len(digest) > Config.MAX_DISCORD_MESSAGE_LENGTH:
            digest = digest[:Config.MAX_DISCORD_MESSAGE_LENGTH - 100] + "\n\n... (tiếp tục ở tin sau)"
        
        # Tạo embed đẹp
        embed = discord.Embed(
            title="🚀 **TECH DIGEST** 🚀",
            description=digest,
            color=0x00ffcc,
            timestamp=datetime.now()
        )
        
        embed.set_author(
            name="AI News Bot",
            icon_url="https://cdn.discordapp.com/emojis/1045075958298386542.png"
        )
        embed.set_footer(
            text=f"🤖 Cập nhật mỗi {Config.SEARCH_INTERVAL_HOURS} giờ | Model: {MODEL_NAME}"
        )
        
        # Xóa loading message và gửi kết quả
        await loading_msg.delete()
        await channel.send(embed=embed)
        
        logger.info(f"Successfully sent news digest at {datetime.now()}")
        
    except Exception as e:
        logger.error(f"Auto news error: {e}")
        await channel.send(f"⚠️ **Lỗi khi tổng hợp tin:**\n```{str(e)[:200]}```")

@auto_news.before_loop
async def before_auto_news():
    """Đợi bot ready trước khi chạy task"""
    await bot.wait_until_ready()
    logger.info(f"Bot ready, starting auto news in 10 seconds...")
    await asyncio.sleep(10)

# =========================
# DISCORD COMMANDS
# =========================
@bot.command(name="news", aliases=["tin"])
async def get_news(ctx):
    """Lệnh thủ công để lấy tin tức ngay lập tức"""
    async with ctx.typing():
        msg = await ctx.send("🔍 **Đang quét internet...**")
        digest = await build_digest()
        
        if len(digest) > 1900:
            # Gửi thành nhiều phần nếu quá dài
            parts = [digest[i:i+1900] for i in range(0, len(digest), 1900)]
            await msg.delete()
            for i, part in enumerate(parts):
                await ctx.send(f"```markdown\n{part}\n```" if i == 0 else part)
        else:
            await msg.edit(content=f"```markdown\n{digest}\n```")

@bot.command(name="ping")
async def ping(ctx):
    """Kiểm tra độ trễ của bot"""
    latency = round(bot.latency * 1000)
    await ctx.send(f"🏓 Pong! `{latency}ms`")

@bot.command(name="status")
async def bot_status(ctx):
    """Xem trạng thái bot"""
    embed = discord.Embed(
        title="🤖 Bot Status",
        color=0x00ff00,
        timestamp=datetime.now()
    )
    embed.add_field(name="Model", value=f"`{MODEL_NAME}`", inline=True)
    embed.add_field(name="Interval", value=f"`{Config.SEARCH_INTERVAL_HOURS}h`", inline=True)
    embed.add_field(name="Latency", value=f"`{round(bot.latency * 1000)}ms`", inline=True)
    embed.add_field(name="Next update", value=f"<t:{int((datetime.now().timestamp() + Config.SEARCH_INTERVAL_HOURS * 3600))}:R>", inline=False)
    
    await ctx.send(embed=embed)

# =========================
# EVENTS
# =========================
@bot.event
async def on_ready():
    """Khi bot online"""
    logger.info(f"✅ Bot đã online: {bot.user} (ID: {bot.user.id})")
    logger.info(f"Model: {MODEL_NAME}")
    logger.info(f"Channel ID: {CHANNEL_ID}")
    
    # Thay đổi status
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name=f"Tech news | mỗi {Config.SEARCH_INTERVAL_HOURS}h"
        )
    )
    
    # Khởi động task nếu chưa chạy
    if not auto_news.is_running():
        auto_news.start()
        logger.info("Auto news task started")

@bot.event
async def on_command_error(ctx, error):
    """Xử lý lỗi command"""
    if isinstance(error, commands.CommandNotFound):
        await ctx.send("❓ Không tìm thấy lệnh. Gõ `!help` để xem danh sách.")
    else:
        logger.error(f"Command error: {error}")
        await ctx.send(f"⚠️ Lỗi: `{str(error)[:100]}`")

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    if not DISCORD_TOKEN:
        logger.error("DISCORD_TOKEN not found in environment variables!")
        exit(1)
    
    if not API_KEY:
        logger.error("API_KEY not found in environment variables!")
        exit(1)
    
    logger.info("Starting bot...")
    bot.run(DISCORD_TOKEN)
