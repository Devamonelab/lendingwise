"""
Standalone runner for cross-validation watcher.
This script can be run directly from the project root.

Usage:
    python run_cross_validation_watcher.py --interval 5
    python run_cross_validation_watcher.py --interval 10 --output-dir result
"""

import sys
import argparse

# Import from cross_validation package
from cross_validation.main_watcher import main

if __name__ == "__main__":
    sys.exit(main())

