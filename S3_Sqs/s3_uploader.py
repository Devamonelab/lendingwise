#!/usr/bin/env python3
import os, json, datetime, re, argparse
import boto3
from botocore.exceptions import ClientError
import requests
from dotenv import load_dotenv

# Load .env
load_dotenv()

# ---------- Config ----------
BUCKET = "lendingwise-aiagent"
ROOT_PREFIX = "LMRFileDocNew"

# ---------- AWS session ----------
def make_boto3_clients():
    region = "us-east-2"
    session = boto3.Session(region_name=region, profile_name="default")
    s3 = session.client("s3")
    sqs = session.client("sqs")
    return s3, sqs, region

S3, SQS, AWS_REGION = make_boto3_clients()

# ---------- Utilities ----------
def today_parts():
    now = datetime.datetime.now(datetime.timezone.utc).astimezone()
    return str(now.year), f"{now.month:02d}", f"{now.day:02d}"

def sanitize_name(name: str) -> str:
    name = name.strip().replace("\\", "/").split("/")[-1]
    return re.sub(r"[^A-Za-z0-9._ ()\\-]", "_", name) or "document"

def split_base_ext(filename: str):
    m = re.match(r"^(.*?)(\\.[^.]+)?$", filename)
    return (m.group(1) or filename), (m.group(2) or "")

def key_exists(bucket: str, key: str) -> bool:
    try:
        S3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise

def dedup_key(bucket: str, key: str) -> str:
    if not key_exists(bucket, key):
        return key
    folder, name = (key.rsplit("/", 1) + [""])[:2]
    base, ext = split_base_ext(name)
    n = 1
    while True:
        cand = f"{folder}/{base}({n}){ext}" if folder else f"{base}({n}){ext}"
        if not key_exists(bucket, cand):
            return cand
        n += 1

def build_prefix(FPCID: str, year: str, month: str, day: str, LMRId: str) -> str:
    return f"{ROOT_PREFIX}/{FPCID}/{year}/{month}/{day}/{LMRId}/upload"

def upload_bytes(bucket: str, key: str, body: bytes, content_type="application/octet-stream"):
    return S3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)

def read_local_file(path: str) -> tuple[bytes, str, int]:
    with open(path, "rb") as f:
        b = f.read()
    ext = os.path.splitext(path)[1].lower()
    ctype = {
        ".pdf": "application/pdf",
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif"
    }.get(ext, "application/octet-stream")
    return b, ctype, len(b)

def send_sqs_message(
    FPCID: str,
    LMRId: str,
    file_path: str,
    document_name: str,
    year: str,
    month: str,
    day: str,
    checklistId: str = None,
    entity_type: str = "LLC",
    queue_url: str = "https://sqs.us-east-2.amazonaws.com/685551735768/lendingwise"
) -> dict:
    """Send SQS message in new direct format."""
    message = {
        "FPCID": str(FPCID),
        "LMRId": str(LMRId),
        "file": file_path,
        "document-name": document_name,
        "entity_type": entity_type,
        "year": int(year),
        "month": int(month),
        "day": int(day)
    }
    
    # Add checklistId if provided
    if checklistId:
        message["checklistId"] = str(checklistId)
    
    try:
        response = SQS.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message, indent=2)
        )
        print(f"[✓] SQS message sent: {response['MessageId']}")
        return response
    except Exception as e:
        print(f"[WARN] Failed to send SQS message: {e}")
        return {}

# ---------- Core ----------
def upload_document(
    *,
    FPCID: str,
    LMRId: str,
    document_file: str,
    year: str | None = None,
    month: str | None = None,
    day: str | None = None,
    bucket: str = BUCKET,
    document_name: str | None = None,
    checklistId: str | None = None,
    send_sqs: bool = False,
    entity_type: str = "LLC",
):
    # Auto-fill date if missing
    if not (year and month and day):
        y, m, d = today_parts()
        year = year or y
        month = (month or m).zfill(2)
        day = (day or d).zfill(2)

    prefix = build_prefix(str(FPCID), str(year), str(month), str(day), str(LMRId))

    filename = sanitize_name(os.path.basename(document_file))
    doc_key = dedup_key(bucket, f"{prefix}/document/{filename}")

    content, content_type, size_bytes = read_local_file(document_file)
    put_resp = upload_bytes(bucket, doc_key, content, content_type)

    meta_dir = f"{prefix}/metadata/"
    meta_name = sanitize_name(os.path.basename(doc_key)) + ".json"
    meta_key = f"{meta_dir}{meta_name}"

    metadata = {
        "FPCID": str(FPCID),
        "LMRId": str(LMRId),
        "file_name": filename,
        "s3_bucket": bucket,
        "s3_key": doc_key,
        "uploaded_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "content_type": content_type,
        "size_bytes": size_bytes,
        "etag": put_resp.get("ETag", "").strip('"'),
        "prefix_parts": {"year": year, "month": month, "day": day}
    }
    # If provided, embed the logical document_name so ingestion can match DB rows accurately
    if document_name:
        metadata["document_name"] = str(document_name)
    # If provided, embed checklistId for DB context matching
    if checklistId:
        metadata["checklistId"] = str(checklistId)

    upload_bytes(bucket, meta_key, json.dumps(metadata, indent=2).encode("utf-8"), "application/json")

    result = {"document_key": doc_key, "metadata_key": meta_key, "prefix": prefix}
    
    # Optionally send SQS message in new direct format
    if send_sqs:
        sqs_response = send_sqs_message(
            FPCID=FPCID,
            LMRId=LMRId,
            file_path=doc_key,
            document_name=document_name or filename,
            year=year,
            month=month,
            day=day,
            checklistId=checklistId,
            entity_type=entity_type
        )
        result["sqs_message_id"] = sqs_response.get("MessageId")
    
    return result

# ---------- CLI ----------
def parse_args():
    p = argparse.ArgumentParser(description="Upload a document + metadata into S3 in LendingWise layout.")
    p.add_argument("--FPCID", required=True)
    p.add_argument("--LMRId", required=True)
    p.add_argument("--file", dest="document_file", required=True, help="Local file path to upload")
    p.add_argument("--year", help="YYYY (default: today)")
    p.add_argument("--month", help="MM (default: today)")
    p.add_argument("--day", help="DD (default: today)")
    p.add_argument("--bucket", default=BUCKET)
    p.add_argument("--document-name", dest="document_name", help="Logical document name (e.g., 'Adhar card', 'Driving License')")
    p.add_argument("--checklistId", dest="checklistId", help="Checklist ID for document tracking")
    p.add_argument("--send-sqs", action="store_true", help="Send SQS message in new direct format")
    p.add_argument("--entity-type", dest="entity_type", default="LLC", help="Entity type (default: LLC)")
    return p.parse_args()

if __name__ == "__main__":
    a = parse_args()
    out = upload_document(
        FPCID=a.FPCID,
        LMRId=a.LMRId,
        document_file=a.document_file,
        year=a.year, month=a.month, day=a.day,
        bucket=a.bucket,
        document_name=a.document_name,
        checklistId=a.checklistId,
        send_sqs=a.send_sqs,
        entity_type=a.entity_type,
    )
    print(json.dumps(out, indent=2))
