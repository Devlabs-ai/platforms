FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY lib/ ./lib/
COPY challenges/ ./challenges/

ENV PYTHONUNBUFFERED=1

# Jobs override via `command:` (see challenges/<id>/job.yaml).
CMD ["python", "/app/challenges/daily-product-sales-pipeline-l1/generate.py"]
