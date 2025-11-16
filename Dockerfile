FROM python:3.11-slim AS base
ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["streamlit", "run", "app/duckdb_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
