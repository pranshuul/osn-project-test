"""Test utilities for NFS system tests."""
import os
import time
import socket
import subprocess
import signal
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any
from dataclasses import dataclass
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants from environment
NAMING_SERVER_HOST = os.getenv('NAMING_SERVER_HOST', '127.0.0.1')
NAMING_SERVER_PORT = int(os.getenv('NAMING_SERVER_PORT', '5000'))
STORAGE_SERVER_HOST = os.getenv('STORAGE_SERVER_HOST', '127.0.0.1')
STORAGE_SERVER_PORT = int(os.getenv('STORAGE_SERVER_PORT', '7000'))
TEST_USER = os.getenv('TEST_USER', 'testuser')
TEST_PASSWORD = os.getenv('TEST_PASSWORD', 'testpass123')
TEST_FILE = os.getenv('TEST_FILE', 'test_file.txt')
TEST_CONTENT = os.getenv('TEST_CONTENT', 'This is a test file content.')
TEST_DIR = os.getenv('TEST_DIR', 'test_dir')
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '5'))
RETRY_DELAY = float(os.getenv('RETRY_DELAY', '1.0'))

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
BIN_DIR = PROJECT_ROOT / 'bin'
DATA_DIR = PROJECT_ROOT / 'data'

@dataclass
class TestProcess:
    """Helper class to manage test processes."""
    process: subprocess.Popen
    name: str
    log_file: Optional[Path] = None

    def terminate(self):
        """Terminate the process and clean up."""
        try:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            
            if self.log_file and self.log_file.exists():
                with open(self.log_file, 'a', encoding='utf-8') as f:
                    f.write(f"\n\n=== Process {self.name} terminated at {time.ctime()} ===\n")
        except Exception as e:
            logger.warning("Error terminating process %s: %s", self.name, e)

def run_command(command: List[str], cwd: Optional[Path] = None, env: Optional[Dict[str, str]] = None) -> Tuple[int, str, str]:
    """Run a shell command and return (returncode, stdout, stderr)."""
    if env is None:
        env = os.environ.copy()
    
    logger.debug("Running command: %s", ' '.join(command))
    result = subprocess.run(
        command,
        cwd=str(cwd) if cwd else None,
        env=env,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        logger.warning(
            "Command failed with code %d: %s\nSTDERR: %s",
            result.returncode, ' '.join(command), result.stderr
        )
    
    return result.returncode, result.stdout, result.stderr

def is_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check if a TCP port is open and listening."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError):
        return False
    except Exception as e:
        logger.warning("Error checking port %s:%d: %s", host, port, e)
        return False

def wait_for_port(host: str, port: int, timeout: float = 10.0, check_interval: float = 0.1) -> bool:
    """Wait until a port is open or timeout is reached."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if is_port_open(host, port):
            return True
        time.sleep(check_interval)
    return False

def start_test_servers() -> Tuple[TestProcess, TestProcess]:
    """Start the naming server and storage server for testing."""
    # Ensure binary directory exists
    if not BIN_DIR.exists():
        raise FileNotFoundError(f"Binary directory not found: {BIN_DIR}")
    
    # Create log directory
    log_dir = PROJECT_ROOT / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    # Start Naming Server
    ns_log = log_dir / 'naming_server_test.log'
    ns_cmd = [str(BIN_DIR / 'name_server')]
    ns_process = subprocess.Popen(
        ns_cmd,
        stdout=open(ns_log, 'w'),
        stderr=subprocess.STDOUT,
        cwd=str(PROJECT_ROOT)
    )
    naming_server = TestProcess(ns_process, "Naming Server", ns_log)
    
    # Start Storage Server
    ss_log = log_dir / 'storage_server_test.log'
    ss_cmd = [str(BIN_DIR / 'storage_server')]
    ss_process = subprocess.Popen(
        ss_cmd,
        stdout=open(ss_log, 'w'),
        stderr=subprocess.STDOUT,
        cwd=str(PROJECT_ROOT)
    )
    storage_server = TestProcess(ss_process, "Storage Server", ss_log)
    
    # Wait for servers to start
    if not wait_for_port(NAMING_SERVER_HOST, NAMING_SERVER_PORT, timeout=10):
        naming_server.terminate()
        storage_server.terminate()
        raise RuntimeError("Naming Server failed to start")
    
    if not wait_for_port(STORAGE_SERVER_HOST, STORAGE_SERVER_PORT, timeout=10):
        naming_server.terminate()
        storage_server.terminate()
        raise RuntimeError("Storage Server failed to start")
    
    return naming_server, storage_server

def stop_test_servers(naming_server: TestProcess, storage_server: TestProcess):
    """Stop the test servers."""
    storage_server.terminate()
    naming_server.terminate()

def run_client_command(command: str, username: str = TEST_USER, expect_success: bool = True) -> Tuple[bool, str]:
    """Run a client command and return (success, output)."""
    client_cmd = [str(BIN_DIR / 'client'), username] + command.split()
    
    for attempt in range(MAX_RETRIES):
        returncode, stdout, stderr = run_command(
            client_cmd,
            cwd=PROJECT_ROOT
        )
        
        if returncode == 0 or not expect_success:
            return returncode == 0, stdout + (stderr if stderr else "")
        
        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY * (attempt + 1))
    
    return False, f"Command failed after {MAX_RETRIES} attempts: {stdout} {stderr}"

def cleanup_test_files():
    """Clean up test files and directories."""
    test_files = [
        DATA_DIR / 'files' / TEST_FILE,
        DATA_DIR / 'files' / f"{TEST_FILE}.copy",
        DATA_DIR / 'files' / TEST_DIR,
    ]
    
    for path in test_files:
        try:
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                import shutil
                shutil.rmtree(path, ignore_errors=True)
        except Exception as e:
            logger.warning("Error cleaning up %s: %s", path, e)
