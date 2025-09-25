# Use the official Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Set the working directory
WORKDIR /app

# Set environment variables
ENV UV_SYSTEM_PYTHON=1
ENV PATH="/root/.local/bin:$PATH"

# Copy only the requirements file first to leverage Docker cache
COPY pyproject.toml .
RUN uv pip install -r pyproject.toml

# Copy the rest of the application code
COPY . .
# Then copy custom modules (which might change more often)
RUN uv pip install -e .

# Set the entrypoint
# ENTRYPOINT ["python", "src/prefect/extract__gcs__unsplash_napi__photos.py"]