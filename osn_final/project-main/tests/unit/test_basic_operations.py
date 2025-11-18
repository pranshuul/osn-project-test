"""Unit tests for basic file operations."""
import os
import time
import pytest
from pathlib import Path
from typing import Tuple

from test_utils import (
    run_client_command, cleanup_test_files, 
    TEST_FILE, TEST_CONTENT, TEST_DIR,
    run_command, PROJECT_ROOT, BIN_DIR
)

class TestBasicOperations:
    """Test basic file operations."""
    
    @pytest.fixture(autouse=True)
    def setup_and_teardown(self):
        """Setup and teardown for each test."""
        # Setup
        cleanup_test_files()
        yield
        # Teardown
        cleanup_test_files()
    
    def test_create_and_read_file(self):
        ""Test creating and reading a file.""
        # Create a file
        success, output = run_client_command(f"CREATE {TEST_FILE} \"{TEST_CONTENT}\"")
        assert success, f"Failed to create file: {output}"
        assert "created successfully" in output.lower()
        
        # Read the file
        success, output = run_client_command(f"READ {TEST_FILE}")
        assert success, f"Failed to read file: {output}"
        assert TEST_CONTENT in output
    
    def test_write_to_file(self):
        ""Test writing to an existing file.""
        # Create a file
        success, _ = run_client_command(f"CREATE {TEST_FILE} \"Initial content\"")
        assert success, "Failed to create test file"
        
        # Write to the file
        new_content = "Updated content"
        success, output = run_client_command(f"WRITE {TEST_FILE} 0 \"{new_content}\"")
        assert success, f"Failed to write to file: {output}"
        
        # Verify the content
        success, output = run_client_command(f"READ {TEST_FILE}")
        assert success
        assert new_content in output
    
    def test_delete_file(self):
        ""Test deleting a file.""
        # Create a file
        success, _ = run_client_command(f"CREATE {TEST_FILE} \"{TEST_CONTENT}\"")
        assert success, "Failed to create test file"
        
        # Delete the file
        success, output = run_client_command(f"DELETE {TEST_FILE}")
        assert success, f"Failed to delete file: {output}"
        
        # Verify the file is deleted
        success, output = run_client_command(f"READ {TEST_FILE}", expect_success=False)
        assert not success
        assert "not found" in output.lower() or "no such file" in output.lower()
    
    def test_file_info(self):
        ""Test getting file information.""
        # Create a file
        success, _ = run_client_command(f"CREATE {TEST_FILE} \"{TEST_CONTENT}\"")
        assert success, "Failed to create test file"
        
        # Get file info
        success, output = run_client_command(f"INFO {TEST_FILE}")
        assert success, f"Failed to get file info: {output}"
        
        # Verify the output contains expected fields
        info_fields = ["filename", "owner", "size", "created", "modified"]
        for field in info_fields:
            assert f"{field}:" in output.lower(), f"Missing field in file info: {field}"
    
    def test_nonexistent_file(self):
        ""Test operations on non-existent file.""
        # Try to read non-existent file
        success, output = run_client_command("READ nonexistent.txt", expect_success=False)
        assert not success
        assert "not found" in output.lower() or "no such file" in output.lower()
        
        # Try to delete non-existent file
        success, output = run_client_command("DELETE nonexistent.txt", expect_success=False)
        assert not success
        assert "not found" in output.lower() or "no such file" in output.lower()
    
    def test_create_directory(self):
        ""Test creating a directory.""
        # Create a directory
        success, output = run_client_command(f"CREATEFOLDER {TEST_DIR}")
        assert success, f"Failed to create directory: {output}"
        
        # Create a file in the directory
        nested_file = f"{TEST_DIR}/nested_file.txt"
        success, output = run_client_command(f"CREATE {nested_file} \"{TEST_CONTENT}\"")
        assert success, f"Failed to create file in directory: {output}"
        
        # Read the file
        success, output = run_client_command(f"READ {nested_file}")
        assert success, f"Failed to read file in directory: {output}"
        assert TEST_CONTENT in output
    
    def test_copy_file(self):
        ""Test copying a file.""
        # Create a file
        success, _ = run_client_command(f"CREATE {TEST_FILE} \"{TEST_CONTENT}\"")
        assert success, "Failed to create test file"
        
        # Copy the file
        copy_file = f"{TEST_FILE}.copy"
        success, output = run_client_command(f"COPY {TEST_FILE} {copy_file}")
        assert success, f"Failed to copy file: {output}"
        
        # Verify the copy
        success, output = run_client_command(f"READ {copy_file}")
        assert success, f"Failed to read copied file: {output}"
        assert TEST_CONTENT in output
    
    def test_concurrent_access(self):
        ""Test concurrent access to the same file.""
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        # Create a test file
        success, _ = run_client_command(f"CREATE {TEST_FILE} \"Initial content\\n\"")
        assert success, "Failed to create test file"
        
        # Function to append to file
        def append_to_file(thread_id):
            line = f"Line from thread {thread_id}"
            return run_client_command(f"WRITE {TEST_FILE} -1 \"{line}\\n\"")
        
        # Run multiple threads appending to the file
        num_threads = 5
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(append_to_file, i) for i in range(num_threads)]
            results = [future.result() for future in as_completed(futures)]
        
        # Verify all writes were successful
        for success, output in results:
            assert success, f"Write operation failed: {output}"
        
        # Verify the file contains all lines
        success, output = run_client_command(f"READ {TEST_FILE}")
        assert success
        
        # Count the number of lines (should be 1 initial + num_threads)
        lines = [line for line in output.split('\n') if line.strip()]
        assert len(lines) == num_threads + 1, "Not all lines were written"
    
    def test_large_file_operations(self):
        ""Test operations with large files.""
        # Generate a large content (1MB)
        large_content = "X" * (1024 * 1024)  # 1MB
        
        # Create a large file
        success, output = run_client_command(f"CREATE large_file.txt \"{large_content}\"")
        assert success, "Failed to create large file"
        
        # Read the large file
        success, output = run_client_command("READ large_file.txt")
        assert success, "Failed to read large file"
        assert len(output.strip()) == len(large_content), "File content length mismatch"
        
        # Cleanup
        run_client_command("DELETE large_file.txt")
