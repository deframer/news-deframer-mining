FROM python:3.12-slim-bookworm

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Copy project files
COPY pyproject.toml README.md uv.lock* ./
COPY miner/ miner/

# Install dependencies and the application
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system .

# Run the application
CMD ["python", "-m", "miner.main"]