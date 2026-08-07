FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ ./scripts/

ENV PYTHONUNBUFFERED=1

# Jobs may override via `command:` (see k8s/job-generate-*.yaml).
CMD ["python", "/app/scripts/generate_daily_product_sales_data.py"]
