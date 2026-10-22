FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    UV_NO_CACHE=1 \
    UV_PYTHON_DOWNLOADS=never \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

RUN pip install --no-cache-dir uv==0.12.5

# uv.lock's runtime set only: no dev or research group, and not the project, which runs from /app.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-default-groups --no-install-project

COPY common ./common
COPY ingest ./ingest
COPY analytics ./analytics
COPY materializer ./materializer
COPY exporter ./exporter

CMD ["python", "-m", "ingest.main"]
