FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir uv==0.11.6

COPY pyproject.toml uv.lock ./
COPY src/ src/

RUN uv sync --locked --no-dev

ENTRYPOINT ["uv", "run", "gdrive-ownership-transfer"]
