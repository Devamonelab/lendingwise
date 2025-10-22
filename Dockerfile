FROM python:3.11-slim

# Metadata
LABEL maintainer="LendingWise AI Team"
LABEL description="Document Processing Workers - SQS Worker & Cross-Validation Watcher"
LABEL version="1.0.0"

# Environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install system dependencies including OCR and PDF processing tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    tesseract-ocr \
    tesseract-ocr-eng \
    poppler-utils \
    libmagic1 \
    libgl1 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories for outputs and reports
RUN mkdir -p outputs \
    Nodes/outputs \
    Nodes/outputs/temp_tamper_check \
    cross_validation/reports \
    result

# Create non-root user for security (commented out for EC2 compatibility)
# If you want to run as non-root, uncomment these lines:
# RUN useradd -m -u 1000 appuser && \
#     chown -R appuser:appuser /app
# USER appuser

# Expose port 8000 for FastAPI service
EXPOSE 8000

# Default CMD - can be overridden in docker-compose.yml
CMD ["python", "sqs_worker.py"]

