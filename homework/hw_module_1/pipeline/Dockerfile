# 1. Base Image: A lightweight version of Python 3.13
FROM python:3.13-slim

# 2. Install 'uv': This makes our container build much faster than using standard pip
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# 3. Work Directory: All commands will now run inside this folder in the container
WORKDIR /app

# 4. Copy Dependencies: We copy these first so Docker can "cache" the installation step
# If you change your code but NOT your libraries, Docker skips the install next time!
COPY pyproject.toml uv.lock ./

# 5. Build Environment: This installs everything from your lockfile
RUN uv sync --frozen

# 6. Copy Code: Finally, we bring your script into the container
COPY ingest_data.py .

# 7. Entrypoint: This is what runs automatically when the container starts
# It allows us to pass arguments (like --url) directly to the 'docker run' command
ENTRYPOINT ["uv", "run", "python", "ingest_data.py"]