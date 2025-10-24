"""
Ingestion node for processing SQS messages and extracting document metadata.
"""

import os
import json
import time
import re
import datetime
from typing import Optional
from urllib.parse import unquote_plus

from ..config.state_models import PipelineState, IngestionState
from ..utils.helpers import log_agent_event
from botocore.exceptions import ClientError
import boto3
from ..tools.aws_services import get_s3_client
from ..tools.db import fetch_agent_context


# ---------- S3 Upload Utilities (from s3_uploader.py) ----------
BUCKET = "lendingwise-aiagent"
ROOT_PREFIX = "LMRFileDocNew"


def sanitize_name(name: str) -> str:
    """Sanitize filename by removing special characters."""
    name = name.strip().replace("\\", "/").split("/")[-1]
    return re.sub(r"[^A-Za-z0-9._ ()\-]", "_", name) or "document"


def split_base_ext(filename: str):
    """Split filename into base and extension."""
    m = re.match(r"^(.*?)(\.[^.]+)?$", filename)
    return (m.group(1) or filename), (m.group(2) or "")


def key_exists(s3_client, bucket: str, key: str) -> bool:
    """Check if a key exists in S3."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def dedup_key(s3_client, bucket: str, key: str) -> str:
    """Generate a unique key by appending (n) if key exists."""
    if not key_exists(s3_client, bucket, key):
        return key
    folder, name = (key.rsplit("/", 1) + [""])[:2]
    base, ext = split_base_ext(name)
    n = 1
    while True:
        cand = f"{folder}/{base}({n}){ext}" if folder else f"{base}({n}){ext}"
        if not key_exists(s3_client, bucket, cand):
            return cand
        n += 1


def build_prefix(FPCID: str, year: str, month: str, day: str, LMRId: str) -> str:
    """Build S3 prefix path."""
    return f"{ROOT_PREFIX}/{FPCID}/{year}/{month}/{day}/{LMRId}/upload"


def upload_bytes(s3_client, bucket: str, key: str, body: bytes, content_type="application/octet-stream"):
    """Upload bytes to S3."""
    return s3_client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def read_efs_file(path: str) -> tuple:
    """Read file from EFS and return (bytes, content_type, size_bytes)."""
    # Check if file exists first
    if not os.path.exists(path):
        # Try to provide helpful debugging info
        dir_path = os.path.dirname(path)
        filename = os.path.basename(path)
        print(f"[DEBUG] File not found at: {path}")
        print(f"[DEBUG] Directory: {dir_path}")
        print(f"[DEBUG] Filename: {filename}")
        
        # Check if directory exists
        if os.path.exists(dir_path):
            print(f"[DEBUG] Directory exists. Contents:")
            try:
                files = os.listdir(dir_path)
                for f in files[:10]:  # Show first 10 files
                    print(f"[DEBUG]   - {f}")
                if len(files) > 10:
                    print(f"[DEBUG]   ... and {len(files) - 10} more files")
            except Exception as e:
                print(f"[DEBUG] Could not list directory: {e}")
        else:
            print(f"[DEBUG] Directory does not exist!")
        
        raise FileNotFoundError(f"EFS file not found: {path}")
    
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


def upload_from_efs_to_s3(
    s3_client,
    efs_path: str,
    FPCID: str,
    LMRId: str,
    year: str,
    month: str,
    day: str,
    document_name: str = None,
    bucket: str = BUCKET
) -> dict:
    """
    Upload file from EFS to S3 with metadata.
    Returns dict with document_key, metadata_key, s3_key, metadata.
    """
    # Ensure year, month, day are zero-padded strings
    year = str(year)
    month = str(month).zfill(2)
    day = str(day).zfill(2)
    
    # Build S3 prefix
    prefix = build_prefix(str(FPCID), year, month, day, str(LMRId))
    
    # Sanitize filename
    filename = sanitize_name(os.path.basename(efs_path))
    doc_key = dedup_key(s3_client, bucket, f"{prefix}/document/{filename}")
    
    # Read file from EFS
    print(f"[INFO] Reading file from EFS: {efs_path}")
    content, content_type, size_bytes = read_efs_file(efs_path)
    
    # Upload document to S3
    print(f"[INFO] Uploading to S3: s3://{bucket}/{doc_key}")
    put_resp = upload_bytes(s3_client, bucket, doc_key, content, content_type)
    
    # Create metadata
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
        "prefix_parts": {"year": year, "month": month, "day": day},
        "_source": "efs_upload"
    }
    
    # Add document_name if provided
    if document_name:
        metadata["document_name"] = str(document_name)
    
    # Upload metadata to S3
    print(f"[INFO] Uploading metadata to S3: s3://{bucket}/{meta_key}")
    upload_bytes(s3_client, bucket, meta_key, json.dumps(metadata, indent=2).encode("utf-8"), "application/json")
    
    print(f"[✓] Successfully uploaded file and metadata to S3")
    
    return {
        "document_key": doc_key,
        "metadata_key": meta_key,
        "s3_key": doc_key,
        "metadata": metadata,
        "bucket": bucket
    }
# ---------- End S3 Upload Utilities ----------


def Ingestion(state: PipelineState) -> PipelineState:
    """
    Poll SQS until a message arrives and populate ingestion state.
    """
    log_agent_event(state, "Ingestion", "start")
    # Setup clients
    region = os.getenv("AWS_REGION")
    queue_url = "https://sqs.us-east-2.amazonaws.com/685551735768/lendingwise"
    if not queue_url:
        raise ValueError("SQS_QUEUE_URL env var is required for ingestion.")

    sqs = boto3.client("sqs", region_name=region)
    s3 = get_s3_client()

    # Helper: fetch sidecar metadata JSON alongside the uploaded object
    def fetch_metadata(bucket: str, key: str) -> dict:
        base_dir = os.path.dirname(key)
        filename = os.path.basename(key)
        # If the object is under /document/, normalize to metadata path parallel to upload/
        # Example: .../upload/document/<file> -> .../upload/metadata/<file>.json
        if base_dir.endswith("/document"):
            meta_base = base_dir.rsplit("/", 1)[0]
            meta_key = f"{meta_base}/metadata/{filename}.json"
        else:
            meta_key = f"{base_dir}/metadata/{filename}.json"
        print(f"[DEBUG] Metadata key being used: {meta_key}")
        for i in range(1, 4):
            try:
                print(f"[INFO] Attempt {i} to fetch metadata: {meta_key}")
                obj = s3.get_object(Bucket=bucket, Key=meta_key)
                content = obj["Body"].read().decode("utf-8")
                data = json.loads(content)
                # Also return the s3 path we used to fetch the metadata
                data.setdefault("_metadata_s3_path", f"s3://{bucket}/{meta_key}")
                return data
            except ClientError as e:
                print(f"[WARN] Failed to fetch metadata {meta_key}: {e}")
                if i < 3:
                    print("[INFO] Retrying in 2 seconds...")
                    time.sleep(2)
        print("[ERROR] Metadata not found after retries.")
        return {}

    # Long-poll for messages and ignore backlog older than startup time
    startup_ms = int(time.time() * 1000)

    while True:
        try:
            resp = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20,
                AttributeNames=["SentTimestamp"],
            )
            messages = resp.get("Messages", [])
            if not messages:
                print("[INFO] No messages in queue. Waiting...")
                time.sleep(5)
                continue

            msg = messages[0]
            receipt = msg.get("ReceiptHandle")
            attrs = msg.get("Attributes", {})
            sent_ts_str = attrs.get("SentTimestamp")
            if sent_ts_str:
                try:
                    sent_ms = int(sent_ts_str)
                    if sent_ms < startup_ms:
                        print(f"[INFO] Skipping old message (SentTimestamp={sent_ms}) older than cutoff {startup_ms}.")
                        if receipt:
                            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
                            print(f"[✓] Old message deleted: {msg.get('MessageId')}")
                        continue
                except Exception:
                    pass

            body = json.loads(msg.get("Body", "{}"))
            
            # Check if this is the new direct message format
            if "FPCID" in body and "LMRId" in body and "file" in body:
                # New direct message format
                FPCID = body.get("FPCID")
                LMRId = body.get("LMRId")
                checklistId = body.get("checklistId")  # Extract checklistId from SQS
                file_path = body.get("file")
                document_name = body.get("document-name")
                year = body.get("year")
                month = body.get("month")
                day = body.get("day")
                # entity_type = body.get("entity_type")  # Ignore for now as requested
                
                print(f"[INFO] Processing direct SQS message format")
                print(f"[INFO] FPCID: {FPCID}, LMRId: {LMRId}, checklistId: {checklistId}, File: {file_path}")
                
                # Check if file_path is an EFS path (starts with /mnt/efs)
                if file_path.startswith("/mnt/efs"):
                    print(f"[INFO] Detected EFS path, uploading to S3...")
                    
                    # Upload from EFS to S3
                    try:
                        upload_result = upload_from_efs_to_s3(
                            s3_client=s3,
                            efs_path=file_path,
                            FPCID=FPCID,
                            LMRId=LMRId,
                            year=year,
                            month=month,
                            day=day,
                            document_name=document_name,
                            bucket=BUCKET
                        )
                        
                        # Update variables with S3 locations
                        bucket = upload_result["bucket"]
                        key = upload_result["s3_key"]
                        
                        print(f"[✓] File uploaded from EFS to S3: s3://{bucket}/{key}")
                    except FileNotFoundError:
                        print(f"[ERROR] EFS file not found: {file_path}")
                        if receipt:
                            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
                        continue
                    except Exception as e:
                        print(f"[ERROR] Failed to upload from EFS to S3: {e}")
                        if receipt:
                            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
                        continue
                else:
                    # File path is already an S3 key
                    bucket = "lendingwise-aiagent"  # Default bucket
                    key = file_path
                
            else:
                # Legacy S3 event format
                records = body.get("Records") or []
                if not records:
                    print(f"[WARN] Skipping unrecognized message format: {body}")
                    if receipt:
                        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
                    continue

                # Use only the first S3 record
                record = records[0]
                bucket = record.get("s3", {}).get("bucket", {}).get("name")
                key = unquote_plus(record.get("s3", {}).get("object", {}).get("key", ""))
                
                # For legacy format, we'll extract FPCID/LMRId/checklistId from metadata later
                FPCID = None
                LMRId = None
                checklistId = None
                document_name = None
                year = None
                month = None
                day = None

            # Skip metadata files themselves
            if key.endswith(".json"):
                print(f"[INFO] Skipping metadata file: {key}")
                if receipt:
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
                continue

            # Handle metadata based on message format
            if "FPCID" in body and "LMRId" in body and "file" in body:
                # For new direct format, check if we uploaded from EFS
                if file_path.startswith("/mnt/efs") and 'upload_result' in locals():
                    # Use metadata from the S3 upload
                    meta = upload_result["metadata"]
                    # Add metadata S3 path
                    meta["_metadata_s3_path"] = f"s3://{bucket}/{upload_result['metadata_key']}"
                    print("\n====================== 📄 EFS UPLOAD METADATA ======================")
                    try:
                        print(json.dumps(meta, indent=2))
                    except Exception:
                        print(str(meta))
                    print("====================================================================\n")
                else:
                    # For direct S3 path, create metadata from SQS message
                    meta = {
                        "FPCID": str(FPCID),
                        "LMRId": str(LMRId),
                        "checklistId": str(checklistId) if checklistId else None,
                        "document_name": document_name,
                        "file_name": os.path.basename(key),
                        "s3_bucket": bucket,
                        "s3_key": key,
                        "year": year,
                        "month": month,
                        "day": day,
                        "_source": "direct_sqs_message"
                    }
                    print("\n====================== 📄 DIRECT SQS MESSAGE CONTENT ======================")
                    try:
                        print(json.dumps(meta, indent=2))
                    except Exception:
                        print(str(meta))
                    print("=====================================================================\n")
            else:
                # Legacy format - fetch metadata from S3
                meta = fetch_metadata(bucket, key)
                print("\n====================== 📄 METADATA FILE CONTENT ======================")
                try:
                    print(json.dumps(meta, indent=2))
                except Exception:
                    print(str(meta))
                print("=====================================================================\n")
                
                # Extract FPCID/LMRId/checklistId from metadata for legacy format
                FPCID = meta.get("FPCID")
                LMRId = meta.get("LMRId")
                checklistId = meta.get("checklistId")
            
            # Try to determine document name from various sources
            potential_doc_name = (
                document_name or  # From direct SQS message
                meta.get("document_name") or 
                meta.get("file_name") or 
                os.path.splitext(os.path.basename(key))[0]  # Extract filename without extension
            )
            
            db_ctx = {}
            if FPCID and checklistId:
                try:
                    # Pass the potential document name to get more specific context
                    db_ctx = fetch_agent_context(str(FPCID), str(checklistId), potential_doc_name) or {}
                    print("\n====================== 🗄️ DB CONTEXT (BY FPCID + checklistId + document_name) ======================")
                    print(f"Keys -> FPCID={FPCID}, checklistId={checklistId}, document_name={potential_doc_name}")
                    try:
                        print(json.dumps(db_ctx, indent=2))
                    except Exception:
                        print(str(db_ctx))
                    if not db_ctx:
                        print("[INFO] No DB row found for provided keys; will rely on metadata fallbacks.")
                    print("==============================================================================\n")
                except Exception as e:
                    print(f"[INGESTION] DB context fetch error: {e}")

            # Build ingestion item
            item = {
                "s3_bucket": bucket,
                "s3_key": key,
                "metadata_s3_path": meta.get("_metadata_s3_path"),
                "FPCID": FPCID,
                "LMRId": LMRId,
                "checklistId": checklistId,
                # Prefer DB values when available, fallback to metadata
                "document_name": (
                    db_ctx.get("document_name")
                    or meta.get("document_name")
                    or meta.get("file_name")
                ),
                "document_type": meta.get("document_type"),
                "agent_name": (db_ctx.get("agent_name") or meta.get("agent_name")),
                "agent_type": meta.get("agent_type"),
                "tool": (db_ctx.get("tool") or meta.get("tool")),
                "source_url": meta.get("source_url"),
                "uploaded_at": meta.get("uploaded_at"),
                "content_type": meta.get("content_type"),
                "size_bytes": meta.get("size_bytes"),
                "etag": meta.get("etag"),
                "prefix_parts": meta.get("prefix_parts"),
                "_raw_metadata": meta,
            }

            # Print merged summary for quick debugging
            try:
                merged_preview = {
                    "FPCID": item.get("FPCID"),
                    "LMRId": item.get("LMRId"),
                    "checklistId": item.get("checklistId"),
                    "document_name": item.get("document_name"),
                    "agent_name": item.get("agent_name"),
                    "tool": item.get("tool"),
                }
                print("\n====================== 🔗 MERGED CONTEXT (DB + Metadata) ======================")
                print(json.dumps(merged_preview, indent=2))
                print("==============================================================================\n")
            except Exception:
                pass

            # Delete the message now that we have the payload
            if receipt:
                sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
                print(f"[✓] Message deleted from queue: {msg.get('MessageId')}")

            # Populate ingestion state and return
            state.ingestion = IngestionState(
                s3_bucket=item.get("s3_bucket"),
                s3_key=item.get("s3_key"),
                metadata_s3_path=item.get("metadata_s3_path"),
                FPCID=item.get("FPCID"),
                LMRId=item.get("LMRId"),
                checklistId=item.get("checklistId"),
                document_name=item.get("document_name"),
                document_type=item.get("document_type"),
                agent_name=item.get("agent_name"),
                agent_type=item.get("agent_type"),
                tool=item.get("tool"),
                source_url=item.get("source_url"),
                content_type=item.get("content_type"),
                uploaded_at=item.get("uploaded_at"),
                size_bytes=item.get("size_bytes"),
                etag=item.get("etag"),
                prefix_parts=item.get("prefix_parts"),
                raw_metadata=item.get("_raw_metadata"),
            )
            log_agent_event(state, "Ingestion", "completed", {"messageId": msg.get("MessageId")})
            return state

        except ClientError as e:
            print(f"[SQS ERROR] {e}")
            log_agent_event(state, "Ingestion", "error", {"error": str(e)})
            time.sleep(5)
        except Exception as e:
            print(f"[INGESTION ERROR] {e}")
            log_agent_event(state, "Ingestion", "error", {"error": str(e)})
            time.sleep(5)
