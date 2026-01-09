FROM python:3.12-slim

ARG PORT=8080
ARG ENTRYPOINT_CMD="uvicorn main:app --host 0.0.0.0 --port 8080"
ARG HEALTHCHECK_PATH=/health

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app
ENV ENTRYPOINT_CMD=${ENTRYPOINT_CMD}

RUN apt-get update && \
    apt-get install -y curl libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:0.9.17 /uv /uvx /bin/

WORKDIR /app

COPY uv.lock pyproject.toml ./
RUN uv sync --frozen

COPY . .

RUN useradd --create-home --uid 1000 appuser && \
    chown -R appuser:appuser /app
USER appuser

EXPOSE ${PORT}

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT}${HEALTHCHECK_PATH} || exit 1

CMD ["sh", "-c", "uv run $ENTRYPOINT_CMD"]
