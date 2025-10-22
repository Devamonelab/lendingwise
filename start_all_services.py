#!/usr/bin/env python3
"""
Start all LendingWise services in parallel:
1. FastAPI server (port 8000)
2. SQS Worker (document processing)
3. Cross-Validation Watcher (validation monitoring)

All services run in background threads and the main process monitors them.
"""

import sys
import time
import threading
import subprocess
import signal
import os

# Global flag for graceful shutdown
shutdown_flag = threading.Event()


def run_fastapi():
    """Run FastAPI server"""
    print("[FastAPI] Starting API server on port 8000...")
    try:
        # Use subprocess to run uvicorn
        process = subprocess.Popen(
            ["python", "-m", "uvicorn", "S3_Sqs.fe_push_simple_api:app", "--host", "0.0.0.0", "--port", "8000"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
        
        # Stream output
        for line in process.stdout:
            if not shutdown_flag.is_set():
                print(f"[FastAPI] {line.strip()}")
        
        process.wait()
        
    except Exception as e:
        print(f"[FastAPI] Error: {e}")


def run_sqs_worker():
    """Run SQS Worker"""
    print("[SQS Worker] Starting document processing worker...")
    # Give FastAPI a moment to start
    time.sleep(3)
    
    try:
        # Import and run the worker
        from sqs_worker import process_one_document
        
        while not shutdown_flag.is_set():
            try:
                process_one_document()
                time.sleep(0.5)  # Small pause between jobs
            except KeyboardInterrupt:
                break
            except Exception as e:
                if not shutdown_flag.is_set():
                    print(f"[SQS Worker] Error: {e}")
                    time.sleep(5)  # Backoff on error
                    
    except Exception as e:
        print(f"[SQS Worker] Fatal error: {e}")


def run_cross_validation():
    """Run Cross-Validation Watcher"""
    print("[Cross-Validation] Starting validation watcher...")
    # Give other services a moment to start
    time.sleep(5)
    
    try:
        # Import and run the watcher
        from cross_validation.main_watcher import main as watcher_main
        
        # Set up arguments for the watcher
        sys.argv = ["run_cross_validation_watcher.py", "--interval", "5"]
        
        watcher_main()
        
    except Exception as e:
        if not shutdown_flag.is_set():
            print(f"[Cross-Validation] Error: {e}")


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    print("\n[Main] Shutdown signal received. Stopping all services...")
    shutdown_flag.set()


def main():
    """Main function to start all services"""
    print("=" * 80)
    print("🚀 LendingWise - Starting All Services")
    print("=" * 80)
    print()
    print("Services:")
    print("  1. FastAPI Server (port 8000)")
    print("  2. SQS Worker (document processing)")
    print("  3. Cross-Validation Watcher (validation monitoring)")
    print()
    print("Press Ctrl+C to stop all services")
    print("=" * 80)
    print()
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create threads for each service
    threads = []
    
    # Start FastAPI in a thread
    api_thread = threading.Thread(target=run_fastapi, name="FastAPI", daemon=True)
    api_thread.start()
    threads.append(api_thread)
    print("[Main] ✓ FastAPI thread started")
    
    # Start SQS Worker in a thread
    sqs_thread = threading.Thread(target=run_sqs_worker, name="SQS-Worker", daemon=True)
    sqs_thread.start()
    threads.append(sqs_thread)
    print("[Main] ✓ SQS Worker thread started")
    
    # Start Cross-Validation in a thread
    cv_thread = threading.Thread(target=run_cross_validation, name="Cross-Validation", daemon=True)
    cv_thread.start()
    threads.append(cv_thread)
    print("[Main] ✓ Cross-Validation thread started")
    
    print()
    print("=" * 80)
    print("✅ All services started successfully!")
    print("=" * 80)
    print()
    
    # Monitor threads
    try:
        while not shutdown_flag.is_set():
            # Check if any thread died
            alive_threads = [t for t in threads if t.is_alive()]
            
            if len(alive_threads) < len(threads):
                dead_threads = [t.name for t in threads if not t.is_alive()]
                print(f"[Main] Warning: Some threads died: {dead_threads}")
                
                # Could implement restart logic here if needed
                # For now, just continue monitoring
            
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n[Main] Keyboard interrupt received")
        shutdown_flag.set()
    
    # Wait for threads to finish
    print("[Main] Waiting for all services to stop...")
    for thread in threads:
        thread.join(timeout=10)
    
    print("[Main] All services stopped. Goodbye!")
    return 0


if __name__ == "__main__":
    sys.exit(main())

