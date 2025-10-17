"""
TamperCheck node for document security analysis.
"""

import os
import json
from typing import Optional

from ..config.state_models import PipelineState, TamperCheckState
from ..config.settings import OUTPUT_DIR, BASELINE_FILE
from ..utils.tamper_detection import tamper_check
from ..tools.aws_services import download_file_from_s3
from ..tools.db import update_tblaigents_by_keys
from ..utils.helpers import load_baseline_file, get_filename_without_extension, log_agent_event


def tamper_check_s3_file(bucket: str, key: str, baseline_texts: Optional[dict] = None) -> dict:
    """
    Perform tamper check on S3 file by downloading it temporarily.
    Returns tamper check results.
    """
    # Create temporary local file path
    temp_dir = os.path.join(OUTPUT_DIR, "temp_tamper_check")
    os.makedirs(temp_dir, exist_ok=True)
    
    filename = os.path.basename(key)
    temp_path = os.path.join(temp_dir, f"temp_{filename}")
    
    try:
        # Download file from S3
        local_path = download_file_from_s3(bucket, key, temp_path)
        
        # Perform tamper check
        results = tamper_check(local_path, baseline_texts)
        
        # Add S3 metadata to results
        results["s3_bucket"] = bucket
        results["s3_key"] = key
        
        return results
        
    finally:
        # Clean up temporary file
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass  # Ignore cleanup errors


def TamperCheck(state: PipelineState) -> PipelineState:
    """
    Perform tamper detection on the ingested file.
    """
    if state.ingestion is None:
        raise ValueError("Ingestion state missing; run Ingestion node first.")

    bucket = state.ingestion.s3_bucket
    key = state.ingestion.s3_key
    
    if not bucket or not key:
        raise ValueError("Missing S3 bucket/key from ingestion.")

    print(f"\n=== TAMPER DETECTION CHECK ===")
    log_agent_event(state, "Tamper Check", "start")
    print(f"Checking file: {key}")
    
    # Load baseline texts if available
    baseline_texts = load_baseline_file(BASELINE_FILE)
    if baseline_texts:
        print(f"Loaded baseline data from: {BASELINE_FILE}")
    
    try:
        # Perform tamper check using the integrated function
        tamper_results = tamper_check_s3_file(bucket, key, baseline_texts)
        
        print(f"📄 File: {os.path.basename(key)}")
        print(f"Status: {tamper_results['status']}")
        print(f"Created: {tamper_results['meta']['created']}")
        print(f"Modified: {tamper_results['meta']['modified']}")
        
        if tamper_results["reasons"]:
            print("\n🔍 Tamper Detection Reasons:")
            for reason in tamper_results["reasons"]:
                print(f" • {reason}")
        else:
            print("No tampering anomalies detected.")
        
        # Determine if human verification is required
        human_verification_required = tamper_results["status"] in ["SUSPICIOUS", "TAMPERED"]
        
        if human_verification_required:
            print(f"\n⚠️  WARNING: File status is {tamper_results['status']}")
            print("🚨 HUMAN VERIFICATION REQUIRED")
            print("This file has been flagged for potential tampering or fraud.")
            print("Please review the reasons above before proceeding with document processing.")

        elif tamper_results["status"] == "ERROR":
            print(f"\n❌ ERROR: Tamper check failed - {tamper_results['reasons']}")
            raise RuntimeError(f"Tamper check failed: {tamper_results['reasons']}")
        else:
            print("✅ File passed tamper check - proceeding with normal processing.")
        
        # Populate tamper check state
        state.tamper_check = TamperCheckState(
            status=tamper_results["status"],
            reasons=tamper_results["reasons"],
            human_verification_required=human_verification_required,
            meta=tamper_results["meta"],
            s3_bucket=bucket,
            s3_key=key,
        )
        log_agent_event(state, "Tamper Check", "completed", {"status": tamper_results["status"]})

        # If human verification is required, mark cross_validation TRUE immediately for this row.
        try:
            if human_verification_required and state.ingestion:
                update_tblaigents_by_keys(
                    FPCID=state.ingestion.FPCID,
                    LMRId=state.ingestion.LMRId,
                    updates={"cross_validation": True},
                    document_name=state.ingestion.document_name,
                )
        except Exception as _e:
            # Non-fatal; continue pipeline
            pass
        
    except Exception as e:
        print(f"⚠️  Warning: Tamper check failed: {e}")
        log_agent_event(state, "Tamper Check", "error", {"error": str(e)})
        print("Proceeding with document processing despite tamper check failure...")
        
        # Create error state but continue processing
        state.tamper_check = TamperCheckState(
            status="ERROR",
            reasons=[f"Tamper check failed: {str(e)}"],
            human_verification_required=True,
            s3_bucket=bucket,
            s3_key=key,
        )
    
    return state
