# Use multi-stage build for optimization
FROM anasty17/mltb:latest as base

# Set working directory
WORKDIR /usr/src/app

# Create non-root user for security
RUN groupadd -r mltb && useradd -r -g mltb mltb

# Set proper permissions
RUN chmod 755 /usr/src/app

# Create virtual environment in a separate layer for better caching
RUN python3 -m venv mltbenv && \
    mltbenv/bin/pip install --upgrade pip setuptools wheel

# Copy requirements first for better Docker layer caching
COPY requirements.txt requirements-cli.txt ./

# Install Python dependencies with optimization flags
RUN mltbenv/bin/pip install --no-cache-dir --compile --optimize=2 \
    -r requirements.txt && \
    # Clean up pip cache and unnecessary files
    mltbenv/bin/pip cache purge && \
    find mltbenv -name "*.pyc" -delete && \
    find mltbenv -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true

# Copy application code
COPY . .

# Set ownership to non-root user
RUN chown -R mltb:mltb /usr/src/app

# Switch to non-root user
USER mltb

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python3 -c "import requests; requests.get('http://localhost:8080/ping', timeout=5)" || exit 1

# Use exec form for better signal handling
CMD ["bash", "start.sh"]
