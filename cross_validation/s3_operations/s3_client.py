"""
S3 client operations for cross validation system.
"""

import json
import os
import sys
from typing import Optional, Tuple

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except Exception:
    boto3 = None  # type: ignore[assignment]
    ClientError = Exception  # type: ignore[assignment]
    NoCredentialsError = Exception  # type: ignore[assignment]


def make_s3_client():
    """Create S3 client."""
    if boto3 is None:
        raise RuntimeError("boto3 is not installed.")
    region = os.getenv("AWS_REGION", "us-east-2")
    return boto3.client("s3", region_name=region)


def parse_s3_url(url: str) -> Tuple[str, str]:
    """Parse s3://bucket/key into (bucket, key)."""
    if not url:
        raise ValueError("empty S3 URL")
    u = url.strip()
    if u.startswith("s3://"):
        u = u[len("s3://"):]
    if "/" not in u:
        raise ValueError(f"invalid S3 URL (no key): {url}")
    bucket, key = u.split("/", 1)
    if not bucket or not key:
        raise ValueError(f"invalid S3 URL: {url}")
    return bucket, key


def get_json_from_s3(s3, bucket: str, key: str) -> Optional[dict]:
    """Download and parse JSON from S3."""
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read()
        return json.loads(body.decode("utf-8"))
    except ClientError as e:  # type: ignore[misc]
        code = getattr(e, "response", {}).get("Error", {}).get("Code")
        print(f"[S3] Error for s3://{bucket}/{key}: {code or e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"[S3] Failed to parse JSON from s3://{bucket}/{key}: {e}", file=sys.stderr)
        return None


def upload_json_to_s3(s3, bucket: str, key: str, data: dict) -> bool:
    """Upload JSON data to S3."""
    try:
        json_body = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_body,
            ContentType="application/json; charset=utf-8"
        )
        print(f"[S3] Successfully uploaded to s3://{bucket}/{key}")
        return True
    except ClientError as e:  # type: ignore[misc]
        code = getattr(e, "response", {}).get("Error", {}).get("Code")
        print(f"[S3] Error uploading to s3://{bucket}/{key}: {code or e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"[S3] Failed to upload to s3://{bucket}/{key}: {e}", file=sys.stderr)
        return False


def extract_date_from_s3_path(s3_path: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract year, month, day from S3 path like LMRFileDocNew/FPCID/YYYY/MM/DD/LMRId/..."""
    try:
        if not s3_path:
            return None, None, None
        
        # Remove s3:// prefix if present
        path = s3_path.replace("s3://", "")
        if "/" in path:
            # Remove bucket name
            path = "/".join(path.split("/")[1:])
        
        # Expected format: LMRFileDocNew/FPCID/YYYY/MM/DD/LMRId/...
        parts = path.split("/")
        if len(parts) >= 5 and parts[0] == "LMRFileDocNew":
            year = parts[2]
            month = parts[3] 
            day = parts[4]
            return year, month, day
        
        return None, None, None
    except Exception as e:
        print(f"[WARN] Failed to extract date from S3 path {s3_path}: {e}")
        return None, None, None