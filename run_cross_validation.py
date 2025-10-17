#!/usr/bin/env python3
"""
Cross-validation runner script.

This script provides different modes for running cross-validation:
1. Legacy mode: Run existing validation logic (compatible with extract_verified_details.py)
2. Watcher mode: Continuous monitoring for cross-validation ready pairs
3. One-time mode: Process all ready pairs once and exit

Usage:
    # Run legacy validation (existing behavior)
    python run_cross_validation.py --mode legacy
    
    # Run continuous watcher (new behavior)
    python run_cross_validation.py --mode watcher --interval 10
    
    # Run one-time processing
    python run_cross_validation.py --mode once
"""

import argparse
import sys
import os

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cross_validation.legacy_validator import main as legacy_main
from cross_validation.main_watcher import main as watcher_main


def main():
    """Main entry point for cross-validation runner."""
    parser = argparse.ArgumentParser(
        description="Cross-validation system with multiple modes",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run legacy validation (existing behavior)
  python run_cross_validation.py --mode legacy
  
  # Run continuous watcher
  python run_cross_validation.py --mode watcher --interval 10 --output-dir result
  
  # Run one-time processing
  python run_cross_validation.py --mode once --output-dir result
        """
    )
    
    parser.add_argument(
        "--mode",
        choices=["legacy", "watcher", "once"],
        default="legacy",
        help="Validation mode: legacy (existing behavior), watcher (continuous), or once (one-time)"
    )
    
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Polling interval in seconds for watcher mode (default: 5)"
    )
    
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory for local reports (default: None - only save to S3)"
    )
    
    parser.add_argument(
        "--no-require-file-s3",
        action="store_true",
        help="Do not require file_s3_location to be present"
    )
    
    args = parser.parse_args()
    
    print(f"🚀 Starting cross-validation in {args.mode} mode...")
    
    if args.mode == "legacy":
        # Run legacy validation
        print("📋 Running legacy validation (existing extract_verified_details.py behavior)")
        return legacy_main()
    
    elif args.mode == "watcher":
        # Run continuous watcher
        print(f"👁️ Running continuous watcher (interval: {args.interval}s)")
        # Modify sys.argv to pass arguments to watcher
        sys.argv = [
            "main_watcher.py",
            "--interval", str(args.interval)
        ]
        if args.output_dir:
            sys.argv.extend(["--output-dir", args.output_dir])
        if args.no_require_file_s3:
            sys.argv.append("--no-require-file-s3")
        
        return watcher_main()
    
    elif args.mode == "once":
        # Run one-time processing
        print("🔄 Running one-time cross-validation")
        
        # Import required modules
        from cross_validation.database import connect_db, fetch_all_statuses_grouped
        from cross_validation.main_watcher import handle_ready_pair
        from cross_validation.s3_operations import make_s3_client
        
        try:
            # Get S3 client
            try:
                s3 = make_s3_client()
                print("[INIT] S3 client created successfully")
            except Exception as e:
                print(f"[WARN] Could not create S3 client: {e}")
                s3 = None
            
            # Get all ready pairs
            with connect_db() as conn:
                statuses = fetch_all_statuses_grouped(conn)
            
            ready_pairs = [(fpcid, lmrid) for (fpcid, lmrid), is_ready in statuses.items() if is_ready]
            
            if not ready_pairs:
                print("[INFO] No ready pairs found for cross-validation")
                return 0
            
            print(f"[INFO] Found {len(ready_pairs)} ready pair(s) for processing")
            
            # Process each ready pair
            for pair in ready_pairs:
                handle_ready_pair(
                    pair,
                    require_file_s3=not args.no_require_file_s3,
                    output_dir=args.output_dir,
                    s3=s3
                )
            
            print(f"\n🎉 One-time processing complete! Processed {len(ready_pairs)} pair(s)")
            return 0
            
        except Exception as e:
            print(f"[ERROR] One-time processing failed: {e}")
            return 1


if __name__ == "__main__":
    sys.exit(main())
