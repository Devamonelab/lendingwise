"""
AWS service utilities for S3 and Textract operations.
"""

import boto3
from botocore.exceptions import ClientError, BotoCoreError
from botocore.config import Config
from typing import Dict, Any, List, Optional
import time

from ..config.settings import (
    AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
    TEXTRACT_MAX_WAIT_SECONDS, TEXTRACT_FEATURE_TYPES,
    S3_PRESIGNED_URL_EXPIRY
)


# Global clients
_TEXTRACT_CLIENT = None
_S3_CLIENT = None

_BOTO_CONFIG = Config(
    retries={"max_attempts": 5, "mode": "standard"},
    connect_timeout=10,
    read_timeout=60,
)


def get_textract_client():
    """Get or create Textract client."""
    global _TEXTRACT_CLIENT
    if _TEXTRACT_CLIENT is None:
        kwargs = {"region_name": AWS_REGION, "config": _BOTO_CONFIG}
        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            kwargs["aws_access_key_id"] = AWS_ACCESS_KEY_ID
            kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
        try:
            _TEXTRACT_CLIENT = boto3.client("textract", **kwargs)
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(f"Failed to initialize Textract client: {e}")
    return _TEXTRACT_CLIENT


def get_s3_client():
    """Get or create S3 client."""
    global _S3_CLIENT
    if _S3_CLIENT is None:
        kwargs = {"region_name": AWS_REGION, "config": _BOTO_CONFIG}
        if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
            kwargs["aws_access_key_id"] = AWS_ACCESS_KEY_ID
            kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY
        try:
            _S3_CLIENT = boto3.client("s3", **kwargs)
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(f"Failed to initialize S3 client: {e}")
    return _S3_CLIENT


def download_file_from_s3(bucket: str, key: str, local_path: str) -> str:
    """
    Download file from S3 to local path.
    Returns the local file path.
    """
    try:
        s3_client = get_s3_client()
        s3_client.download_file(bucket, key, local_path)
        return local_path
    except Exception as e:
        raise RuntimeError(f"Failed to download file from S3: {e}")


def generate_presigned_url(bucket: str, key: str) -> Optional[str]:
    """Generate presigned URL for S3 object."""
    try:
        s3_client = get_s3_client()
        return s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=S3_PRESIGNED_URL_EXPIRY,
        )
    except Exception as e:
        print(f"Warning: Could not create presigned URL: {e}")
        return None


def run_textract_async_s3(bucket: str, key: str, max_wait_seconds: int = TEXTRACT_MAX_WAIT_SECONDS) -> Dict[str, Any]:
    """Run Textract document analysis on S3 file."""
    client = get_textract_client()
    try:
        response = client.start_document_analysis(
            DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
            FeatureTypes=TEXTRACT_FEATURE_TYPES,
        )
        job_id = response["JobId"]
        print(f"[Textract] Started job {job_id}")
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"Textract start failed: {e}")

    start_time = time.time()
    while True:
        try:
            status = client.get_document_analysis(JobId=job_id)
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(f"Textract polling failed: {e}")
        job_status = status.get("JobStatus")
        if job_status in ["SUCCEEDED", "FAILED"]:
            break
        if time.time() - start_time > max_wait_seconds:
            raise TimeoutError(f"Textract job {job_id} timed out after {max_wait_seconds}s")
        print("[Textract] Job running...")
        time.sleep(5)

    if job_status == "FAILED":
        raise RuntimeError("Textract job failed.")

    blocks: List[Dict[str, Any]] = []
    next_token = None
    pages_total = status["DocumentMetadata"].get("Pages", None)
    while True:
        try:
            if next_token:
                status = client.get_document_analysis(JobId=job_id, NextToken=next_token)
            else:
                status = client.get_document_analysis(JobId=job_id)
        except (BotoCoreError, ClientError) as e:
            raise RuntimeError(f"Textract pagination failed: {e}")
        blocks.extend(status.get("Blocks", []))
        next_token = status.get("NextToken")
        if not next_token:
            break

    return {
        "engine_meta": {
            "mode": "textract:start_document_analysis",
            "pages": pages_total,
            "job_id": job_id,
        },
        "blocks": blocks,
    }


def run_analyze_id_s3(bucket: str, key: str) -> Dict[str, Any]:
    """
    Use Textract AnalyzeID (best for driver licenses, passports).
    Returns dict. We will convert it into KVs later for merging.
    """
    client = get_textract_client()
    try:
        resp = client.analyze_id(DocumentPages=[{"S3Object": {"Bucket": bucket, "Name": key}}])
        return resp
    except (BotoCoreError, ClientError) as e:
        raise RuntimeError(f"AnalyzeID failed: {e}")
