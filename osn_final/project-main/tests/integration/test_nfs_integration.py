"""Integration tests for the NFS system."""
import os
import time
import pytest
from pathlib import Path
from typing import Tuple, List, Dict, Any

from test_utils import (
    run_client_command, cleanup_test_files, start_test_servers, stop_test_servers,
    TestProcess, TEST_FILE, TEST_CONTENT, TEST_DIR, PROJECT_ROOT, BIN_DIR,
    NAMING_SERVER_HOST, NAMING_SERVER_PORT, STORAGE_SERVER_HOST, STORAGE_SERVER_PORT
)

class TestNFSIntegration:
    """Integration tests for the NFS system."""
    
    @classmethod
    def setup_class(cls):
        """Setup test class."""
        # Start test servers
        cls.naming_server, cls.storage_server = start_test_servers()
        
        # Register a test user
        run_client_command("REGISTER testuser testpass123")
    
    @classmethod
    def teardown_class(cls):
        """Teardown test class."""
        # Stop test servers
        stop_test_servers(cls.naming_server, cls.storage_server)
    
    def setup_method(self):
        """Setup before each test method."""
        cleanup_test_files()
    
    def teardown_method(self):
        """Teardown after each test method."""
        cleanup_test_files()
    
    def test_file_operations_workflow(self):
        ""Test complete workflow of file operations."""
        # 1. Create a file
        success, output = run_client_command(f"CREATE {TEST_FILE} \"{TEST_CONTENT}\"")
        assert success, f"Failed to create file: {output}"
        
        # 2. Verify file exists
        success, output = run_client_command(f"INFO {TEST_FILE}")
        assert success, f"File info failed: {output}"
        assert "size:" in output.lower()
        
        # 3. Read file content
        success, output = run_client_command(f"READ {TEST_FILE}")
        assert success, f"Failed to read file: {output}"
        assert TEST_CONTENT in output
        
        # 4. Update file content
        new_content = "Updated content"
        success, output = run_client_command(f"WRITE {TEST_FILE} 0 \"{new_content}\"")
        assert success, f"Failed to update file: {output}"
        
        # 5. Verify update
        success, output = run_client_command(f"READ {TEST_FILE}")
        assert success
        assert new_content in output
        
        # 6. Copy file
        copy_file = f"{TEST_FILE}.copy"
        success, output = run_client_command(f"COPY {TEST_FILE} {copy_file}")
        assert success, f"Failed to copy file: {output}"
        
        # 7. Verify copy
        success, output = run_client_command(f"READ {copy_file}")
        assert success
        assert new_content in output
        
        # 8. Delete files
        for filename in [TEST_FILE, copy_file]:
            success, output = run_client_command(f"DELETE {filename}")
            assert success, f"Failed to delete {filename}: {output}"
            
            # Verify deletion
            success, output = run_client_command(f"READ {filename}", expect_success=False)
            assert not success
    
    def test_directory_operations(self):
        ""Test directory operations."""
        # 1. Create a directory
        success, output = run_client_command(f"CREATEFOLDER {TEST_DIR}")
        assert success, f"Failed to create directory: {output}"
        
        # 2. Create a file in the directory
        nested_file = f"{TEST_DIR}/nested_file.txt"
        success, output = run_client_command(f"CREATE {nested_file} \"{TEST_CONTENT}\"")
        assert success, f"Failed to create file in directory: {output}"
        
        # 3. List directory contents
        success, output = run_client_command(f"VIEWFOLDER {TEST_DIR}")
        assert success, f"Failed to list directory: {output}"
        assert "nested_file.txt" in output
        
        # 4. Move file to another location
        new_location = f"{TEST_DIR}/moved_file.txt"
        success, output = run_client_command(f"MOVE {nested_file} {new_location}")
        assert success, f"Failed to move file: {output}"
        
        # 5. Verify move
        success, output = run_client_command(f"VIEWFOLDER {TEST_DIR}")
        assert success
        assert "nested_file.txt" not in output
        assert "moved_file.txt" in output
    
    def test_permissions(self):
        ""Test file permissions and access control."""
        # Create a file with testuser
        success, _ = run_client_command(f"CREATE {TEST_FILE} \"{TEST_CONTENT}\"")
        assert success, "Failed to create test file"
        
        # Try to access with a different user (should fail)
        other_user = "otheruser"
        run_client_command(f"REGISTER {other_user} password123")
        
        success, output = run_client_command(
            f"READ {TEST_FILE}", 
            username=other_user,
            expect_success=False
        )
        assert not success
        assert "permission" in output.lower() or "unauthorized" in output.lower()
        
        # Grant read permission
        success, output = run_client_command(
            f"ADDACCESS {TEST_FILE} {other_user} READ"
        )
        assert success, f"Failed to grant read permission: {output}"
        
        # Now should be able to read
        success, output = run_client_command(
            f"READ {TEST_FILE}",
            username=other_user
        )
        assert success, f"Should be able to read with permission: {output}"
        
        # But not write
        success, output = run_client_command(
            f"WRITE {TEST_FILE} 0 \"New content\"",
            username=other_user,
            expect_success=False
        )
        assert not success
        assert "permission" in output.lower() or "unauthorized" in output.lower()
        
        # Grant write permission
        success, output = run_client_command(
            f"ADDACCESS {TEST_FILE} {other_user} WRITE"
        )
        assert success, f"Failed to grant write permission: {output}"
        
        # Now should be able to write
        success, output = run_client_command(
            f"WRITE {TEST_FILE} 0 \"New content\"",
            username=other_user
        )
        assert success, f"Should be able to write with permission: {output}"
    
    def test_concurrent_access(self):
        ""Test concurrent access from multiple clients."""
        import threading
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        # Create a test file
        success, _ = run_client_command(f"CREATE {TEST_FILE} \"Initial content\\n\"")
        assert success, "Failed to create test file"
        
        # Function to append to file
        def append_to_file(thread_id):
            line = f"Line from thread {thread_id}"
            return run_client_command(
                f"WRITE {TEST_FILE} -1 \"{line}\\n\"",
                username=f"user{thread_id}"
            )
        
        # Register test users
        num_threads = 5
        for i in range(num_threads):
            run_client_command(f"REGISTER user{i} pass{i}")
            run_client_command(f"ADDACCESS {TEST_FILE} user{i} WRITE")
        
        # Run multiple threads appending to the file
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
