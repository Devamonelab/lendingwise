"""
Configuration settings for the Lendingwise AI pipeline.
"""

import os
from typing import Optional

# AWS Configuration
AWS_REGION = os.getenv("AWS_REGION")
# Do not provide hardcoded defaults for credentials; rely on environment/instance/profile
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")

# OpenAI Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")

# Output Configuration
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "outputs")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Document Type Labels
ROUTE_LABELS = ["bank_statement", "identity", "property", "entity", "loan", "unknown"]

# OCR Configuration
OCR_MODE = os.getenv("OCR_MODE", "ocr+llm")
DOC_CATEGORY = os.getenv("DOC_CATEGORY", "")

# Baseline Configuration
BASELINE_FILE = os.getenv("BASELINE_FILE", "")

# Textract Configuration
TEXTRACT_MAX_WAIT_SECONDS = int(os.getenv("TEXTRACT_MAX_WAIT_SECONDS", "600"))
TEXTRACT_FEATURE_TYPES = ["TABLES", "FORMS"]

# S3 Configuration
S3_PRESIGNED_URL_EXPIRY = int(os.getenv("S3_PRESIGNED_URL_EXPIRY", "900"))

# Logging Configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
