"""
Long-running worker that waits for SQS messages and processes them through the pipeline.

Starts the existing nodes in sequence: Ingestion -> OCR -> Classification -> Extraction -> Validation Check.
The Ingestion node already internally polls SQS until a message is available.

Usage (PowerShell):
  python sqs_worker.py

Environment (required):
  AWS credentials + region; OPENAI_API_KEY as needed for LLM steps.
Optional env:
  OCR_MODE, DOC_CATEGORY, BASELINE_FILE
"""

import os
import json
import time
from urllib.parse import unquote_plus
import boto3
from botocore.exceptions import ClientError
from Nodes.config.state_models import PipelineState, IngestionState
from Nodes.nodes.ingestion_node import Ingestion
from Nodes.nodes.ocr_node import OCR
from Nodes.nodes.classification_node import Classification
from Nodes.nodes.extraction_node import Extract
from Nodes.nodes.validation_check_node import ValidationCheck
from Nodes.tools.aws_services import get_s3_client
from Nodes.tools.db import fetch_agent_context


def cleanup_failed_document(state: PipelineState) -> None:
    """Clean up failed document and its metadata from S3."""
    try:
        s3 = get_s3_client()
        bucket = state.ingestion.s3_bucket if state.ingestion else None
        key = state.ingestion.s3_key if state.ingestion else None
        if bucket and key:
            # Delete the document
            try:
                s3.delete_object(Bucket=bucket, Key=key)
                print(f"[Cleanup] Deleted S3 object: s3://{bucket}/{key}")
            except Exception as e:
                print(f"[Cleanup] Warn: could not delete object {key}: {e}")

            # Compute and delete metadata key (handles '/document' -> '/metadata')
            import os as _os
            base_dir = _os.path.dirname(key)
            filename = _os.path.basename(key)
            if base_dir.endswith("/document"):
                meta_base = base_dir.rsplit("/", 1)[0]
                meta_key = f"{meta_base}/metadata/{filename}.json"
            else:
                meta_key = f"{base_dir}/metadata/{filename}.json"
            try:
                s3.delete_object(Bucket=bucket, Key=meta_key)
                print(f"[Cleanup] Deleted S3 metadata: s3://{bucket}/{meta_key}")
            except Exception as e:
                print(f"[Cleanup] Warn: could not delete metadata {meta_key}: {e}")
    except Exception as _e:
        print(f"[Cleanup] Skipped cleanup due to error: {_e}")


def process_one_document() -> None:
    state = PipelineState()
    # Ingestion will block/poll until an SQS message is available
    state = Ingestion(state)

    # Log document processing info
    if state.ingestion:
        bucket = state.ingestion.s3_bucket
        key = state.ingestion.s3_key
        doc_name = state.ingestion.document_name
        fpcid = state.ingestion.FPCID
        lmrid = state.ingestion.LMRId
        print(f"[Worker] Processing document: {doc_name} (FPCID: {fpcid}, LMRId: {lmrid})")
        print(f"[Worker] S3 Location: s3://{bucket}/{key}")

    # Run the rest of the pipeline for this document
    state = OCR(state)
    state = Classification(state)
    if not state.classification or not state.classification.passed:
        try:
            if state.classification and state.classification.message:
                print(f"[Classifier] {state.classification.message}")
        except Exception:
            pass
        
        # On failure: remove the uploaded document and its sidecar metadata from S3
        cleanup_failed_document(state)
        
        # Get folder info for logging
        bucket = state.ingestion.s3_bucket if state.ingestion else None
        key = state.ingestion.s3_key if state.ingestion else ""
        try:
            base_prefix = key.split("/upload/")[0] + "/upload"
        except Exception:
            base_prefix = os.path.dirname(key)
        
        print(f"[Worker] Classification failed; document cleaned up. Please re-upload correct document to folder: {base_prefix}/")
        print(f"[Worker] Continuing to process other messages while waiting for re-upload...")
        
        # Return immediately to allow processing of other messages
        # The normal SQS polling will handle re-uploads when they arrive
        return
    
    # Classification passed - proceed with extraction and validation
    print(f"[Worker] ✓ Classification passed for {state.ingestion.document_name if state.ingestion else 'document'}")
    state = Extract(state)
    print(f"[Worker] ✓ Extraction completed")
    
    # Run validation check
    state = ValidationCheck(state)
    print(f"[Worker] ✓ Validation check completed")
    
    print(f"[Worker] ✓ Document processing completed successfully")


def main() -> int:
    print("Worker started. Waiting for SQS messages... Press Ctrl+C to stop.")
    print("[INFO] Non-blocking mode: Failed documents will be cleaned up and worker will continue processing other messages.")
    print("[INFO] Re-uploads will be automatically detected and processed when they arrive in the queue.")
    
    while True:
        try:
            process_one_document()
            # Small pause between jobs to avoid hot-looping
            time.sleep(0.5)
        except KeyboardInterrupt:
            print("Stopping worker...")
            break
        except Exception as e:
            print(f"[Worker] Error: {e}")
            # Backoff briefly before trying the next message
            time.sleep(5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


