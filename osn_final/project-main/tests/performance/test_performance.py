"""Performance tests for the NFS system."""
import os
import time
import pytest
import statistics
from pathlib import Path
from typing import List, Tuple, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from test_utils import (
    run_client_command, cleanup_test_files, start_test_servers, stop_test_servers,
    TestProcess, TEST_FILE, TEST_CONTENT, TEST_DIR, PROJECT_ROOT, BIN_DIR
)

# Performance test configurations
PERF_TEST_ITERATIONS = 10
PERF_FILE_SIZES = [
    ("1KB", 1024),          # 1KB
    ("10KB", 10 * 1024),     # 10KB
    ("100KB", 100 * 1024),   # 100KB
    ("1MB", 1024 * 1024),    # 1MB
]
PERF_CONCURRENT_CLIENTS = [1, 5, 10, 25, 50]  # Number of concurrent clients

class TestNFSPerformance:
    """Performance tests for the NFS system."""
    
    @classmethod
    def setup_class(cls):
        """Setup test class."""
        # Start test servers
        cls.naming_server, cls.storage_server = start_test_servers()
        
        # Register a test user
        run_client_command("REGISTER testuser testpass123")
        
        # Create a test directory
        run_client_command(f"CREATEFOLDER {TEST_DIR}")
    
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
    
    def generate_test_content(self, size: int) -> str:
        """Generate test content of specified size."""
        # Use a repeating pattern to make compression less effective
        pattern = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        repeat = (size // len(pattern)) + 1
        return (pattern * repeat)[:size]
    
    def test_file_write_performance(self):
        ""Test write performance with different file sizes.""
        results = {}
        
        for size_name, size in PERF_FILE_SIZES:
            test_content = self.generate_test_content(size)
            test_file = f"perf_write_{size_name}.txt"
            
            times = []
            for i in range(PERF_TEST_ITERATIONS):
                start_time = time.time()
                
                success, output = run_client_command(
                    f"CREATE {test_file} \"{test_content}\""
                )
                assert success, f"Failed to create file: {output}"
                
                end_time = time.time()
                times.append(end_time - start_time)
                
                # Cleanup for next iteration
                run_client_command(f"DELETE {test_file}")
            
            # Calculate statistics
            avg_time = statistics.mean(times)
            throughput = size / (avg_time * 1024)  # KB/s
            
            results[size_name] = {
                'size_bytes': size,
                'iterations': PERF_TEST_ITERATIONS,
                'avg_time_seconds': avg_time,
                'throughput_kb_per_sec': throughput,
                'all_times': times
            }
            
            print(f"\nWrite Performance - {size_name}:")
            print(f"  Average time: {avg_time:.4f} seconds")
            print(f"  Throughput: {throughput:.2f} KB/s")
        
        # Save results to file
        self.save_performance_results("write_performance.json", results)
    
    def test_file_read_performance(self):
        ""Test read performance with different file sizes.""
        results = {}
        
        for size_name, size in PERF_FILE_SIZES:
            test_content = self.generate_test_content(size)
            test_file = f"perf_read_{size_name}.txt"
            
            # Create the test file first
            success, _ = run_client_command(f"CREATE {test_file} \"{test_content}\"")
            assert success, f"Failed to create test file {test_file}"
            
            times = []
            for _ in range(PERF_TEST_ITERATIONS):
                start_time = time.time()
                
                success, output = run_client_command(f"READ {test_file}")
                assert success, f"Failed to read file: {output}"
                assert len(output.strip()) >= size, "Incomplete content read"
                
                end_time = time.time()
                times.append(end_time - start_time)
            
            # Calculate statistics
            avg_time = statistics.mean(times)
            throughput = size / (avg_time * 1024)  # KB/s
            
            results[size_name] = {
                'size_bytes': size,
                'iterations': PERF_TEST_ITERATIONS,
                'avg_time_seconds': avg_time,
                'throughput_kb_per_sec': throughput,
                'all_times': times
            }
            
            print(f"\nRead Performance - {size_name}:")
            print(f"  Average time: {avg_time:.6f} seconds")
            print(f"  Throughput: {throughput:.2f} KB/s")
            
            # Cleanup
            run_client_command(f"DELETE {test_file}")
        
        # Save results to file
        self.save_performance_results("read_performance.json", results)
    
    def test_concurrent_reads(self):
        ""Test performance with multiple concurrent read operations."""
        test_file = "concurrent_read_test.txt"
        test_content = self.generate_test_content(10 * 1024)  # 10KB file
        
        # Create the test file
        success, _ = run_client_command(f"CREATE {test_file} \"{test_content}\"")
        assert success, "Failed to create test file"
        
        results = {}
        
        for num_clients in PERF_CONCURRENT_CLIENTS:
            print(f"\nTesting with {num_clients} concurrent readers...")
            
            def read_worker() -> float:
                start_time = time.time()
                success, output = run_client_command(f"READ {test_file}")
                end_time = time.time()
                
                assert success, f"Read failed: {output}"
                assert len(output.strip()) >= len(test_content), "Incomplete content read"
                
                return end_time - start_time
            
            # Run concurrent readers
            with ThreadPoolExecutor(max_workers=num_clients) as executor:
                futures = [executor.submit(read_worker) for _ in range(num_clients)]
                times = [future.result() for future in as_completed(futures)]
            
            # Calculate statistics
            avg_time = statistics.mean(times)
            throughput = (len(test_content) * num_clients) / (avg_time * 1024)  # KB/s
            
            results[num_clients] = {
                'num_clients': num_clients,
                'file_size': len(test_content),
                'avg_time_seconds': avg_time,
                'throughput_kb_per_sec': throughput,
                'all_times': times
            }
            
            print(f"  Average time per read: {avg_time:.6f} seconds")
            print(f"  Total throughput: {throughput:.2f} KB/s")
        
        # Save results to file
        self.save_performance_results("concurrent_reads.json", results)
        
        # Cleanup
        run_client_command(f"DELETE {test_file}")
    
    def test_concurrent_writes(self):
        ""Test performance with multiple concurrent write operations."""
        results = {}
        
        for num_clients in PERF_CONCURRENT_CLIENTS:
            print(f"\nTesting with {num_clients} concurrent writers...")
            
            test_files = [f"concurrent_write_{i}.txt" for i in range(num_clients)]
            test_content = self.generate_test_content(1024)  # 1KB per file
            
            def write_worker(file_idx: int) -> Tuple[bool, float]:
                filename = test_files[file_idx]
                start_time = time.time()
                
                success, output = run_client_command(
                    f"CREATE {filename} \"{test_content}\""
                )
                
                end_time = time.time()
                return success, (end_time - start_time)
            
            # Run concurrent writers
            with ThreadPoolExecutor(max_workers=num_clients) as executor:
                futures = [executor.submit(write_worker, i) for i in range(num_clients)]
                results_list = [future.result() for future in as_completed(futures)]
            
            # Process results
            success_count = sum(1 for success, _ in results_list if success)
            times = [time for _, time in results_list]
            
            if times:
                avg_time = statistics.mean(times)
                throughput = (len(test_content) * num_clients) / (avg_time * 1024)  # KB/s
                
                results[num_clients] = {
                    'num_clients': num_clients,
                    'file_size': len(test_content),
                    'success_count': success_count,
                    'avg_time_seconds': avg_time,
                    'throughput_kb_per_sec': throughput,
                    'all_times': times
                }
                
                print(f"  Success rate: {success_count}/{num_clients}")
                print(f"  Average time per write: {avg_time:.6f} seconds")
                print(f"  Total throughput: {throughput:.2f} KB/s")
            
            # Cleanup
            for filename in test_files:
                run_client_command(f"DELETE {filename}", expect_success=False)
        
        # Save results to file
        self.save_performance_results("concurrent_writes.json", results)
    
    def save_performance_results(self, filename: str, data: Dict[str, Any]):
        """Save performance results to a JSON file."""
        import json
        from datetime import datetime
        
        # Create results directory if it doesn't exist
        results_dir = PROJECT_ROOT / "results"
        results_dir.mkdir(exist_ok=True)
        
        # Add timestamp to filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = Path(filename).stem
        ext = Path(filename).suffix
        output_file = results_dir / f"{base_name}_{timestamp}{ext}"
        
        # Save the data
        with open(output_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'test_name': self.id(),
                'data': data
            }, f, indent=2)
        
        print(f"\nResults saved to: {output_file}")

# This test requires manual execution as it's destructive
# @pytest.mark.skip(reason="Manual execution only - this test is destructive")
# def test_system_limits():
#     """Test system limits by creating a large number of files."""
#     num_files = 1000
#     print(f"\nCreating {num_files} files...")
#     
#     start_time = time.time()
#     
#     for i in range(num_files):
#         filename = f"limit_test_{i:04d}.txt"
#         success, output = run_client_command(f"CREATE {filename} \"Test content {i}\"")
#         
#         if not success:
#             print(f"Failed after {i} files: {output}")
#             break
#             
#         if (i + 1) % 100 == 0:
#             print(f"  Created {i + 1} files...")
#     
#     end_time = time.time()
#     elapsed = end_time - start_time
#     
#     print(f"\nCreated {num_files} files in {elapsed:.2f} seconds")
#     print(f"Average time per file: {(elapsed * 1000 / num_files):.2f} ms")
#     
#     # Cleanup
#     print("\nCleaning up...")
#     for i in range(num_files):
#         filename = f"limit_test_{i:04d}.txt"
#         run_client_command(f"DELETE {filename}", expect_success=False)
