FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Chỉ copy file code, KHÔNG copy .env
COPY thongtinvacapnhat.py .

# Lệnh chạy bot
CMD ["python", "thongtinvacapnhat.py"]
