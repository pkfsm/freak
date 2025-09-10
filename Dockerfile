FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy uploader + rclone config
COPY . .

# Run uploader
CMD ["python", "uploader.py"]
