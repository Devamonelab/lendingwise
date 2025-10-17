"""
Ingestion node for processing SQS messages and extracting document metadata.
"""

import os
import json
import time
from typing import Optional
from urllib.parse import unquote_plus

from ..config.state_models import PipelineState, IngestionState
from ..utils.helpers import log_agent_event
from botocore.exceptions import ClientError
import boto3
from ..tools.aws_services import get_s3_client
from ..tools.db import fetch_agent_context


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
                file_path = body.get("file")
                document_name = body.get("document-name")
                year = body.get("year")
                month = body.get("month")
                day = body.get("day")
                # entity_type = body.get("entity_type")  # Ignore for now as requested
                
                # Extract bucket and key from file path
                # Assuming file path is like "LMRFileDoc/3580/2025/10/13/1/upload/filename.ext"
                bucket = "lendingwise-aiagent"  # Default bucket
                key = file_path
                
                print(f"[INFO] Processing direct SQS message format")
                print(f"[INFO] FPCID: {FPCID}, LMRId: {LMRId}, File: {file_path}")
                
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
                
                # For legacy format, we'll extract FPCID/LMRId from metadata later
                FPCID = None
                LMRId = None
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
                # For new direct format, create metadata from SQS message
                meta = {
                    "FPCID": str(FPCID),
                    "LMRId": str(LMRId),
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
                
                # Extract FPCID/LMRId from metadata for legacy format
                FPCID = meta.get("FPCID")
                LMRId = meta.get("LMRId")
            
            # Try to determine document name from various sources
            potential_doc_name = (
                document_name or  # From direct SQS message
                meta.get("document_name") or 
                meta.get("file_name") or 
                os.path.splitext(os.path.basename(key))[0]  # Extract filename without extension
            )
            
            db_ctx = {}
            if FPCID and LMRId:
                try:
                    # Pass the potential document name to get more specific context
                    db_ctx = fetch_agent_context(str(FPCID), str(LMRId), potential_doc_name) or {}
                    print("\n====================== 🗄️ DB CONTEXT (BY FPCID + LMRId + document_name) ======================")
                    print(f"Keys -> FPCID={FPCID}, LMRId={LMRId}, document_name={potential_doc_name}")
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
