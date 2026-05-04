FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY scraper.py .
# Mặc định sinh dữ liệu mẫu 2000 dòng (đổi --sample thành bỏ để scrape thật)
CMD ["python", "scraper.py", "--sample", "--sample-n", "2000"]
