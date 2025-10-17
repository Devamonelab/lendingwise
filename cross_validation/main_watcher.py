"""
Main cross-validation watcher script.

Watch for FPCID/LMRId pairs to become cross-validated and, once ready,
fetch all document details from each row's `verified_result_s3_path` in S3
and cross-validate against the tblfile database table.

Rules:
- A pair (FPCID, LMRId) is considered READY only when ALL its rows
  have `cross_validation = TRUE`.
- Once a pair transitions to READY, this script:
  1. Queries all rows for that pair where verified_result_s3_path is NOT NULL
  2. Downloads the JSON at verified_result_s3_path for each document
  3. Fetches corresponding borrower data from tblfile (if available)
  4. Cross-validates all fields across S3 documents and DB
  5. Generates a detailed verification report

Usage:
  python cross_validation/main_watcher.py [--interval 5] [--output-dir OUTDIR] [--no-require-file-s3]
"""

import argparse
import sys
import time
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set

from mysql.connector import Error

# Handle both direct execution and module import
try:
    from .database import connect_db, fetch_all_statuses_grouped, fetch_docs_for_pair, fetch_borrower_data_from_tblfile, update_is_verified
    from .s3_operations import make_s3_client, parse_s3_url, get_json_from_s3
    from .models import DocumentDetails
    from .validation import EnhancedValidator
    from .reports import write_comprehensive_json_report, write_markdown_report, write_enhanced_cross_validation_report_to_s3
except ImportError:
    # Direct execution fallback
    from database import connect_db, fetch_all_statuses_grouped, fetch_docs_for_pair, fetch_borrower_data_from_tblfile, update_is_verified
    from s3_operations import make_s3_client, parse_s3_url, get_json_from_s3
    from models import DocumentDetails
    from validation import EnhancedValidator
    from reports import write_comprehensive_json_report, write_markdown_report, write_enhanced_cross_validation_report_to_s3


def handle_ready_pair(
    pair: Tuple[str, str],
    require_file_s3: bool,
    output_dir: Optional[str],
    s3,
    processed_docs: Set[str]
) -> Set[str]:
    """
    Process a ready FPCID/LMRId pair with enhanced GPT-4o validation.
    Returns set of processed document S3 paths.
    """
    FPCID, LMRId = pair
    print(f"\n{'='*80}")
    print(f"[PROCESS] Starting enhanced cross-validation for FPCID={FPCID}, LMRId={LMRId}")
    print(f"{'='*80}\n")
    
    # Step 1: Fetch document rows from tblaiagents (Identity Verification Agent only)
    try:
        with connect_db() as conn:
            rows = fetch_docs_for_pair(conn, FPCID, LMRId, require_file_s3=require_file_s3)
    except Error as e:
        print(f"[ERROR] DB error while fetching rows for {FPCID}/{LMRId}: {e}", file=sys.stderr)
        return processed_docs
    
    # Process all documents (removed agent_name filter)
    if not rows:
        print(f"[INFO] No verified documents found for {FPCID}/{LMRId}")
        return processed_docs
    
    # Filter out already processed documents
    current_s3_paths = {row.get("verified_result_s3_path") for row in rows if row.get("verified_result_s3_path")}
    new_s3_paths = current_s3_paths - processed_docs
    
    if not new_s3_paths:
        print(f"[INFO] All {len(current_s3_paths)} document(s) already processed for {FPCID}/{LMRId}")
        return processed_docs
    
    print(f"[INFO] Found {len(rows)} verified document(s) ({len(new_s3_paths)} new)")
    
    # Step 2: Download S3 documents
    documents: List[DocumentDetails] = []
    first_s3_path = None
    
    for i, row in enumerate(rows, 1):
        vpath = row.get("verified_result_s3_path") or ""
        if not vpath:
            continue
        
        if not first_s3_path:
            first_s3_path = vpath
        
        doc_name = row.get("document_name") or f"document_{i}"
        print(f"[S3] Downloading {doc_name}: {vpath}")
        
        payload: Optional[dict] = None
        if s3:
            try:
                bucket, key = parse_s3_url(vpath)
                payload = get_json_from_s3(s3, bucket, key)
            except Exception as e:
                print(f"[ERROR] Failed to fetch {vpath}: {e}", file=sys.stderr)
        
        documents.append(DocumentDetails(
            document_name=doc_name,
            agent_name=row.get("agent_name"),
            tool=row.get("tool"),
            file_s3_location=row.get("file_s3_location"),
            metadata_s3_path=row.get("metadata_s3_path"),
            verified_result_s3_path=vpath,
            verified_details=payload
        ))
    
    print(f"[INFO] Successfully downloaded {len([d for d in documents if d.verified_details])} document(s)")
    
    if not documents:
        print(f"[ERROR] No documents available for validation")
        return processed_docs
    
    # Step 3: Fetch borrower data from tblfile
    borrower_data = None
    try:
        with connect_db() as conn:
            borrower_data = fetch_borrower_data_from_tblfile(conn, FPCID, LMRId)
    except Error as e:
        print(f"[WARN] DB error while fetching from tblfile: {e}", file=sys.stderr)
    
    if borrower_data:
        print(f"[INFO] Found borrower data in tblfile (reference-based validation)")
    else:
        print(f"[INFO] No borrower data found in tblfile (cross-document validation only)")
    
    # Step 4: Run enhanced validation with GPT-4o
    print(f"\n[VALIDATE] Starting GPT-4o enhanced validation...")
    try:
        validator = EnhancedValidator()
        report = validator.validate(documents, borrower_data, FPCID, LMRId)
    except Exception as e:
        print(f"[ERROR] Validation failed: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return processed_docs
    
    # Step 5: Print summary to console
    print(f"\n{'='*80}")
    print(f"[RESULT] Status: {report.validation_summary.status}")
    print(f"[RESULT] Score: {report.validation_summary.score}/100")
    print(f"[RESULT] Message: {report.validation_summary.message}")
    print(f"[RESULT] Recommendation: {report.recommendation.action} ({report.recommendation.confidence})")
    print(f"{'='*80}")
    print(f"[SUMMARY] Total Fields: {report.summary['total_fields']}")
    print(f"[SUMMARY] Matched: {report.summary['matched']}")
    print(f"[SUMMARY] Partial: {report.summary['partial']}")
    print(f"[SUMMARY] Failed: {report.summary['failed']}")
    if report.summary.get('issues'):
        print(f"[ISSUES]")
        for issue in report.summary['issues']:
            print(f"  - {issue}")
    print(f"{'='*80}\n")
    
    # Step 6: Write enhanced cross-validation report to S3
    s3_key = None
    s3_url = None
    if s3:
        try:
            s3_key = write_enhanced_cross_validation_report_to_s3(
                report, 
                s3,
                first_document_s3_path=first_s3_path
            )
            if s3_key:
                s3_url = f"s3://lendingwise-aiagent/{s3_key}"
                print(f"[✓] Cross-validation report saved to S3: {s3_url}")
            else:
                print(f"[WARN] Failed to save report to S3")
        except Exception as e:
            print(f"[ERROR] Failed to write cross-validation report to S3: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
    
    # Step 7: Update DB Is_varified flag and report path
    verification_passed = (report.validation_summary.status == "PASS")
    try:
        with connect_db() as conn:
            update_is_verified(conn, FPCID, LMRId, verification_passed, s3_url)
        print(f"[DB] Updated Is_varified = {verification_passed} for {FPCID}/{LMRId}")
        if s3_url:
            print(f"[DB] Updated cross_validation_report_path = {s3_url}")
    except Error as e:
        print(f"[ERROR] Failed to update database: {e}", file=sys.stderr)
    
    # Step 8: Write local reports only if output_dir is specified (for backward compatibility)
    if output_dir:
        try:
            # Write simplified JSON report
            import json
            output_path = os.path.join(output_dir, f"{FPCID}_{LMRId}_validation.json")
            os.makedirs(output_dir, exist_ok=True)
            
            # Convert report to dict
            from dataclasses import asdict
            report_dict = asdict(report)
            
            with open(output_path, 'w') as f:
                json.dump(report_dict, f, indent=2)
            
            print(f"[INFO] Local JSON report written to {output_path}")
        except Exception as e:
            print(f"[ERROR] Failed to write local JSON report: {e}", file=sys.stderr)
    
    # Return updated set of processed documents
    return current_s3_paths | processed_docs


def main():
    """Main entry point."""
    ap = argparse.ArgumentParser(
        description="Watch cross_validation and fetch verified document details from S3 "
                    "once FPCID/LMRId pairs are ready, then cross-validate against tblfile"
    )
    ap.add_argument("--interval", type=float, default=5.0, help="Polling interval in seconds (default: 5)")
    ap.add_argument(
        "--output-dir",
        default=None,
        help="Directory to write local JSON reports (default: None - only save to S3)"
    )
    ap.add_argument(
        "--no-require-file-s3",
        action="store_true",
        help="Do not require file_s3_location to be present (default: require)"
    )
    args = ap.parse_args()
    
    require_file_s3 = not args.no_require_file_s3
    
    print(f"[INIT] Starting cross-validation watcher...")
    print(f"[INIT] Polling interval: {args.interval:.0f}s")
    print(f"[INIT] Require file_s3_location: {require_file_s3}")
    print(f"[INIT] Local output directory: {args.output_dir or 'None (S3 only)'}")
    print(f"[INIT] Enhanced reports will be saved to S3 in cross-validation-result/ folders")
    
    last_status: Dict[Tuple[str, str], bool] = {}
    processed_docs: Dict[Tuple[str, str], Set[str]] = {}  # Track processed S3 paths per pair
    
    try:
        s3 = make_s3_client()
        print("[INIT] S3 client created successfully")
    except Exception as e:
        print(f"[WARN] Could not create S3 client: {e}", file=sys.stderr)
        s3 = None
    
    try:
        while True:
            try:
                with connect_db() as conn:
                    statuses = fetch_all_statuses_grouped(conn)
            except Error as e:
                print(f"[ERROR] DB error: {e}", file=sys.stderr)
                statuses = {}
            
            processed_now = 0
            for pair, is_ready in statuses.items():
                if is_ready:
                    # Get or create processed docs set for this pair
                    if pair not in processed_docs:
                        processed_docs[pair] = set()
                    
                    # Process the pair and update processed docs
                    updated_docs = handle_ready_pair(
                        pair,
                        require_file_s3=require_file_s3,
                        output_dir=args.output_dir,
                        s3=s3,
                        processed_docs=processed_docs[pair]
                    )
                    
                    # Track if we actually processed new documents
                    if updated_docs != processed_docs[pair]:
                        processed_now += 1
                        processed_docs[pair] = updated_docs
                
                last_status[pair] = is_ready
            
            total_ready = sum(1 for v in statuses.items() if v[1])
            print(f"[TICK] {processed_now} processed; total ready {total_ready}/{len(statuses)} - {datetime.now().strftime('%H:%M:%S')}")
            time.sleep(max(0.5, args.interval))
    
    except KeyboardInterrupt:
        print("\n[EXIT] Stopped by user.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
