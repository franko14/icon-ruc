# Multi-stage production Docker build for ICON-RUC weather data processing
FROM python:3.11-slim as base

# Install system dependencies for GRIB processing and NetCDF
RUN apt-get update && apt-get install -y \
    libeccodes-dev \
    libhdf5-dev \
    libnetcdf-dev \
    pkg-config \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash --uid 1000 iconuser

WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt requirements-prod.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements-prod.txt

# Development stage
FROM base as development
COPY requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt
USER iconuser
CMD ["python", "enhanced_api_server.py"]

# Production stage
FROM base as production

# Copy application code
COPY --chown=iconuser:iconuser . .

# Create necessary directories
RUN mkdir -p /app/data/{bratislava,grid,processed,raw,runs} && \
    mkdir -p /app/logs && \
    chown -R iconuser:iconuser /app

# Switch to non-root user
USER iconuser

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:5000/api/health || exit 1

# Expose application port
EXPOSE 5000

# Production command with gunicorn
CMD ["gunicorn", "--config", "gunicorn.conf.py", "enhanced_api_server:app"]