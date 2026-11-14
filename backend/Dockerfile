FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DATABASE_PATH=/tmp/pocket-signal/signals.db

WORKDIR /app/backend

COPY backend/pyproject.toml ./pyproject.toml
COPY backend/app ./app

RUN pip install --no-cache-dir . \
    && mkdir -p /tmp/pocket-signal \
    && useradd --create-home --uid 10001 app \
    && chown -R app:app /app /tmp/pocket-signal

USER app

EXPOSE 8000

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
