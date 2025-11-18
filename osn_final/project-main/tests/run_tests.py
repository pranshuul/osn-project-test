#!/usr/bin/env python3
"""
Test runner for the NFS system tests.

Usage:
    ./run_tests.py [unit|integration|performance|all] [options]

Options:
    -v, --verbose    Enable verbose output
    -s, --no-capture Show output from tests
    -x, --exitfirst  Exit instantly on first error or failed test
    -k EXPRESSION    Only run tests which match the given substring expression
    --log-level LEVEL Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
"""
import os
import sys
import logging
import subprocess
from pathlib import Path
from typing import List, Optional

# Project root directory
PROJECT_ROOT = Path(__file__).parent.parent

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(PROJECT_ROOT / 'logs' / 'test_runner.log')
    ]
)
logger = logging.getLogger(__name__)

def run_tests(test_type: str, args: List[str]) -> int:
    """Run tests of the specified type."""
    test_dir = f"tests/{test_type}"
    
    if not (PROJECT_ROOT / test_dir).exists():
        logger.error("Test directory not found: %s", test_dir)
        return 1
    
    # Build the test command
    cmd = [
        sys.executable, "-m", "pytest",
        "-v",  # Verbose output
        "--log-level=INFO",
        "--cov=src",
        "--cov-report=term-missing",
        "--cov-report=html:htmlcov",
        test_dir
    ]
    
    # Add additional arguments
    cmd.extend(args)
    
    logger.info("Running %s tests...", test_type.capitalize())
    logger.debug("Command: %s", " ".join(cmd))
    
    # Run the tests
    try:
        return subprocess.call(cmd, cwd=str(PROJECT_ROOT))
    except KeyboardInterrupt:
        logger.info("Test run interrupted by user")
        return 1

def main():
    """Main entry point for the test runner."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Run NFS system tests")
    parser.add_argument(
        'test_type',
        nargs='?',
        default='all',
        choices=['unit', 'integration', 'performance', 'all'],
        help='Type of tests to run (default: all)'
    )
    parser.add_argument(
        'pytest_args',
        nargs=argparse.REMAINDER,
        help='Additional arguments to pass to pytest'
    )
    
    args = parser.parse_args()
    
    # Process pytest arguments
    pytest_args = []
    for arg in args.pytest_args:
        if arg.startswith('--log-level'):
            # Handle log level
            if '=' in arg:
                level = arg.split('=', 1)[1]
            else:
                level = args.pytest_args.pop(0)
            logging.getLogger().setLevel(level.upper())
        else:
            pytest_args.append(arg)
    
    # Create necessary directories
    (PROJECT_ROOT / 'logs').mkdir(exist_ok=True)
    (PROJECT_ROOT / 'results').mkdir(exist_ok=True)
    
    # Run the appropriate tests
    if args.test_type == 'all':
        test_types = ['unit', 'integration', 'performance']
    else:
        test_types = [args.test_type]
    
    exit_code = 0
    for test_type in test_types:
        logger.info("\n" + "=" * 80)
        logger.info("RUNNING %s TESTS", test_type.upper())
        logger.info("=" * 80)
        
        result = run_tests(test_type, pytest_args)
        if result != 0:
            exit_code = result
            if '--exitfirst' in pytest_args or '-x' in pytest_args:
                break
    
    return exit_code

if __name__ == "__main__":
    sys.exit(main())
