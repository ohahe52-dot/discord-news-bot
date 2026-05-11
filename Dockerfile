FROM python:3.11-slim

WORKDIR /app

# Cài đặt dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY bot.py .
COPY .env .

# Chạy bot
CMD ["python", "bot.py"]
