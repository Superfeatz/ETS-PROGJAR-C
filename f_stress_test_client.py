import socket
import json
import base64
import logging
import os
import sys
import time
import random
import threading
import multiprocessing
import concurrent.futures
import argparse
from collections import defaultdict
import statistics
import csv
from typing import Dict, List, Tuple, Optional, Any, Union
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class TestResult:
    """Data class for test operation results"""
    worker_id: int
    operation: str
    file_size: int = 0
    duration: float = 0.0
    throughput: float = 0.0
    status: str = 'OK'
    error: Optional[str] = None


@dataclass
class TestStatistics:
    """Data class for test statistics"""
    operation: str
    file_size_mb: int
    client_pool_size: int
    executor_type: str
    server_pool_size: int = 0
    avg_duration: float = 0.0
    median_duration: float = 0.0
    min_duration: float = 0.0
    max_duration: float = 0.0
    avg_throughput: float = 0.0
    median_throughput: float = 0.0
    min_throughput: float = 0.0
    max_throughput: float = 0.0
    success_count: int = 0
    fail_count: int = 0


class FileManager:
    """Handles file operations for testing"""
    
    def __init__(self, test_files_dir: str = 'test_files', downloads_dir: str = 'downloads'):
        self.test_files_dir = Path(test_files_dir)
        self.downloads_dir = Path(downloads_dir)
        self._ensure_directories()
    
    def _ensure_directories(self) -> None:
        """Create necessary directories if they don't exist"""
        self.test_files_dir.mkdir(exist_ok=True)
        self.downloads_dir.mkdir(exist_ok=True)
    
    def generate_test_file(self, size_mb: int) -> str:
        """Generate a test file of specified size"""
        filename = f"test_file_{size_mb}MB.bin"
        filepath = self.test_files_dir / filename
        
        if self._file_exists_with_correct_size(filepath, size_mb):
            logging.info(f"Test file {filename} already exists with correct size")
            return str(filepath)
        
        return self._create_test_file(filepath, size_mb)
    
    def _file_exists_with_correct_size(self, filepath: Path, size_mb: int) -> bool:
        """Check if file exists with correct size"""
        expected_size = size_mb * 1024 * 1024
        return filepath.exists() and filepath.stat().st_size == expected_size
    
    def _create_test_file(self, filepath: Path, size_mb: int) -> str:
        """Create a new test file"""
        logging.info(f"Generating test file: {filepath.name} ({size_mb} MB)")
        
        chunk_size = 1024 * 1024  # 1MB chunks
        with open(filepath, 'wb') as f:
            for _ in range(size_mb):
                f.write(os.urandom(chunk_size))
        
        logging.info(f"Test file generated: {filepath}")
        return str(filepath)
    
    def get_download_path(self, filename: str, worker_id: int) -> str:
        """Get download path with worker ID prefix"""
        return str(self.downloads_dir / f"worker{worker_id}_{filename}")


class NetworkClient:
    """Handles network communication with the server"""
    
    def __init__(self, server_address: Tuple[str, int], timeout: int = 600):
        self.server_address = server_address
        self.timeout = timeout
        self.chunk_size = 65536
        self.recv_buffer_size = 8192
        self.message_delimiter = "\r\n\r\n"
    
    def send_command(self, command_str: str = "") -> Dict[str, Any]:
        """Send command to server and receive response"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.timeout)
        
        try:
            return self._execute_command(sock, command_str)
        except socket.timeout as e:
            logging.error(f"Socket timeout: {str(e)}")
            return {'status': 'ERROR', 'data': f'Socket timeout: {str(e)}'}
        except ConnectionRefusedError:
            logging.error("Connection refused. Is the server running?")
            return {'status': 'ERROR', 'data': 'Connection refused. Is the server running?'}
        except Exception as e:
            logging.error(f"Error in send_command: {str(e)}")
            return {'status': 'ERROR', 'data': str(e)}
        finally:
            sock.close()
    
    def _execute_command(self, sock: socket.socket, command_str: str) -> Dict[str, Any]:
        """Execute command through socket connection"""
        start_connect = time.time()
        sock.connect(self.server_address)
        connect_time = time.time() - start_connect
        logging.debug(f"Connection established in {connect_time:.2f}s")
        
        self._send_command_data(sock, command_str)
        response_data = self._receive_response(sock)
        
        return self._parse_response(response_data)
    
    def _send_command_data(self, sock: socket.socket, command_str: str) -> None:
        """Send command data in chunks"""
        chunks = [command_str[i:i+self.chunk_size] 
                 for i in range(0, len(command_str), self.chunk_size)]
        
        for chunk in chunks:
            sock.sendall(chunk.encode())
        
        sock.sendall(self.message_delimiter.encode())
    
    def _receive_response(self, sock: socket.socket) -> str:
        """Receive response data from socket"""
        data_received = ""
        
        while True:
            try:
                data = sock.recv(self.recv_buffer_size)
                if data:
                    data_received += data.decode()
                    if self.message_delimiter in data_received:
                        break
                else:
                    break
            except socket.timeout:
                logging.error("Socket timeout while receiving data")
                raise
        
        return data_received
    
    def _parse_response(self, data_received: str) -> Dict[str, Any]:
        """Parse JSON response from received data"""
        json_response = data_received.split(self.message_delimiter)[0]
        return json.loads(json_response)


class PerformanceTracker:
    """Tracks performance metrics and statistics"""
    
    def __init__(self):
        self.results: Dict[str, List[TestResult]] = {
            'upload': [],
            'download': [],
            'list': []
        }
        self.success_count: Dict[str, int] = {
            'upload': 0,
            'download': 0,
            'list': 0
        }
        self.fail_count: Dict[str, int] = {
            'upload': 0,
            'download': 0,
            'list': 0
        }
    
    def reset_counters(self) -> None:
        """Reset all performance counters"""
        for operation in ['upload', 'download', 'list']:
            self.success_count[operation] = 0
            self.fail_count[operation] = 0
            self.results[operation] = []
    
    def record_success(self, operation: str) -> None:
        """Record successful operation"""
        self.success_count[operation] += 1
    
    def record_failure(self, operation: str) -> None:
        """Record failed operation"""
        self.fail_count[operation] += 1
    
    def add_result(self, operation: str, result: TestResult) -> None:
        """Add result to tracking"""
        self.results[operation].append(result)
    
    def calculate_statistics(self, operation: str, all_results: List[TestResult], 
                           file_size_mb: int, client_pool_size: int, 
                           executor_type: str) -> Optional[TestStatistics]:
        """Calculate performance statistics from results"""
        durations = [r.duration for r in all_results if r.status == 'OK']
        throughputs = [r.throughput for r in all_results if r.throughput > 0]
        
        if not durations:
            logging.warning("No successful operations to calculate statistics")
            return TestStatistics(
                operation=operation,
                file_size_mb=file_size_mb,
                client_pool_size=client_pool_size,
                executor_type=executor_type,
                success_count=self.success_count[operation],
                fail_count=self.fail_count[operation]
            )
        
        return TestStatistics(
            operation=operation,
            file_size_mb=file_size_mb,
            client_pool_size=client_pool_size,
            executor_type=executor_type,
            avg_duration=statistics.mean(durations),
            median_duration=statistics.median(durations),
            min_duration=min(durations),
            max_duration=max(durations),
            avg_throughput=statistics.mean(throughputs) if throughputs else 0,
            median_throughput=statistics.median(throughputs) if throughputs else 0,
            min_throughput=min(throughputs) if throughputs else 0,
            max_throughput=max(throughputs) if throughputs else 0,
            success_count=self.success_count[operation],
            fail_count=self.fail_count[operation]
        )


class TestOperations:
    """Handles different test operations (upload, download, list)"""
    
    def __init__(self, network_client: NetworkClient, file_manager: FileManager, 
                 performance_tracker: PerformanceTracker):
        self.network_client = network_client
        self.file_manager = file_manager
        self.performance_tracker = performance_tracker
    
    def perform_upload(self, file_path: str, worker_id: int) -> TestResult:
        """Upload a file and measure performance"""
        start_time = time.time()
        filename = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        try:
            logging.info(f"Worker {worker_id}: Starting upload of {filename} ({file_size/1024/1024:.2f} MB)")
            
            file_content = self._read_file_as_base64(file_path)
            command_str = f"UPLOAD {filename} {file_content}"
            result = self.network_client.send_command(command_str)
            
            return self._process_upload_result(result, worker_id, filename, file_size, start_time)
            
        except Exception as e:
            return self._handle_operation_exception(e, worker_id, 'upload', filename, file_size, start_time)
    
    def perform_download(self, filename: str, worker_id: int) -> TestResult:
        """Download a file and measure performance"""
        start_time = time.time()
        
        try:
            logging.info(f"Worker {worker_id}: Starting download of {filename}")
            
            command_str = f"GET {filename}"
            result = self.network_client.send_command(command_str)
            
            return self._process_download_result(result, worker_id, filename, start_time)
            
        except Exception as e:
            return self._handle_operation_exception(e, worker_id, 'download', filename, 0, start_time)
    
    def perform_list(self, worker_id: int) -> TestResult:
        """Perform list operation and measure performance"""
        start_time = time.time()
        
        try:
            command_str = "LIST"
            result = self.network_client.send_command(command_str)
            
            return self._process_list_result(result, worker_id, start_time)
            
        except Exception as e:
            return self._handle_operation_exception(e, worker_id, 'list', '', 0, start_time)
    
    def _read_file_as_base64(self, file_path: str) -> str:
        """Read file and encode as base64"""
        with open(file_path, 'rb') as fp:
            return base64.b64encode(fp.read()).decode()
    
    def _process_upload_result(self, result: Dict[str, Any], worker_id: int, 
                             filename: str, file_size: int, start_time: float) -> TestResult:
        """Process upload operation result"""
        end_time = time.time()
        duration = end_time - start_time
        throughput = file_size / duration if duration > 0 else 0
        
        if result['status'] == 'OK':
            logging.info(f"Worker {worker_id}: Upload successful - {filename} "
                        f"({file_size/1024/1024:.2f} MB) in {duration:.2f}s - "
                        f"{throughput/1024/1024:.2f} MB/s")
            self.performance_tracker.record_success('upload')
        else:
            logging.error(f"Worker {worker_id}: Upload failed - {filename}: {result['data']}")
            self.performance_tracker.record_failure('upload')
        
        return TestResult(
            worker_id=worker_id,
            operation='upload',
            file_size=file_size,
            duration=duration,
            throughput=throughput,
            status=result['status']
        )
    
    def _process_download_result(self, result: Dict[str, Any], worker_id: int, 
                               filename: str, start_time: float) -> TestResult:
        """Process download operation result"""
        end_time = time.time()
        duration = end_time - start_time
        
        if result['status'] == 'OK':
            file_content = base64.b64decode(result['data_file'])
            file_size = len(file_content)
            throughput = file_size / duration if duration > 0 else 0
            
            download_path = self.file_manager.get_download_path(filename, worker_id)
            with open(download_path, 'wb') as f:
                f.write(file_content)
            
            logging.info(f"Worker {worker_id}: Download successful - {filename} "
                        f"({file_size/1024/1024:.2f} MB) in {duration:.2f}s - "
                        f"{throughput/1024/1024:.2f} MB/s")
            self.performance_tracker.record_success('download')
            
            return TestResult(
                worker_id=worker_id,
                operation='download',
                file_size=file_size,
                duration=duration,
                throughput=throughput,
                status='OK'
            )
        else:
            logging.error(f"Worker {worker_id}: Download failed - {filename}: {result['data']}")
            self.performance_tracker.record_failure('download')
            
            return TestResult(
                worker_id=worker_id,
                operation='download',
                file_size=0,
                duration=duration,
                throughput=0,
                status='ERROR',
                error=result['data']
            )
    
    def _process_list_result(self, result: Dict[str, Any], worker_id: int, 
                           start_time: float) -> TestResult:
        """Process list operation result"""
        end_time = time.time()
        duration = end_time - start_time
        
        if result['status'] == 'OK':
            file_count = len(result['data'])
            logging.info(f"Worker {worker_id}: List successful - {file_count} files in {duration:.2f}s")
            self.performance_tracker.record_success('list')
        else:
            logging.error(f"Worker {worker_id}: List failed: {result['data']}")
            self.performance_tracker.record_failure('list')
        
        return TestResult(
            worker_id=worker_id,
            operation='list',
            duration=duration,
            status=result['status']
        )
    
    def _handle_operation_exception(self, exception: Exception, worker_id: int, 
                                  operation: str, filename: str, file_size: int, 
                                  start_time: float) -> TestResult:
        """Handle exceptions during operations"""
        end_time = time.time()
        duration = end_time - start_time
        logging.error(f"Worker {worker_id}: {operation.capitalize()} exception - {filename}: {str(exception)}")
        self.performance_tracker.record_failure(operation)
        
        return TestResult(
            worker_id=worker_id,
            operation=operation,
            file_size=file_size,
            duration=duration,
            throughput=0,
            status='ERROR',
            error=str(exception)
        )


class ResultsExporter:
    """Handles exporting test results to CSV"""
    
    @staticmethod
    def save_results_to_csv(all_stats: List[TestStatistics]) -> str:
        """Save test results to CSV file"""
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        csv_filename = f"stress_test_results_{timestamp}.csv"
        
        fieldnames = [
            'operation', 'file_size_mb', 'client_pool_size', 'server_pool_size', 'executor_type',
            'avg_duration', 'median_duration', 'min_duration', 'max_duration',
            'avg_throughput', 'median_throughput', 'min_throughput', 'max_throughput',
            'success_count', 'fail_count'
        ]
        
        with open(csv_filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for stats in all_stats:
                writer.writerow({
                    'operation': stats.operation,
                    'file_size_mb': stats.file_size_mb,
                    'client_pool_size': stats.client_pool_size,
                    'server_pool_size': stats.server_pool_size,
                    'executor_type': stats.executor_type,
                    'avg_duration': stats.avg_duration,
                    'median_duration': stats.median_duration,
                    'min_duration': stats.min_duration,
                    'max_duration': stats.max_duration,
                    'avg_throughput': stats.avg_throughput,
                    'median_throughput': stats.median_throughput,
                    'min_throughput': stats.min_throughput,
                    'max_throughput': stats.max_throughput,
                    'success_count': stats.success_count,
                    'fail_count': stats.fail_count
                })
        
        logging.info(f"Results saved to {csv_filename}")
        return csv_filename


class StressTestClient:
    """Main stress test client class - maintains backward compatibility"""
    
    def __init__(self, server_address: Tuple[str, int] = ('localhost', 6667)):
        self.server_address = server_address
        
        # Initialize components
        self.file_manager = FileManager()
        self.network_client = NetworkClient(server_address)
        self.performance_tracker = PerformanceTracker()
        self.test_operations = TestOperations(
            self.network_client, 
            self.file_manager, 
            self.performance_tracker
        )
        self.results_exporter = ResultsExporter()
        
        # Backward compatibility properties
        self.results = self.performance_tracker.results
        self.success_count = self.performance_tracker.success_count
        self.fail_count = self.performance_tracker.fail_count
    
    def generate_test_file(self, size_mb: int) -> str:
        """Generate a test file of specified size - backward compatibility"""
        return self.file_manager.generate_test_file(size_mb)
    
    def send_command(self, command_str: str = "") -> Dict[str, Any]:
        """Send command to server - backward compatibility"""
        return self.network_client.send_command(command_str)
    
    def perform_upload(self, file_path: str, worker_id: int) -> Dict[str, Any]:
        """Upload a file and measure performance - backward compatibility"""
        result = self.test_operations.perform_upload(file_path, worker_id)
        return {
            'worker_id': result.worker_id,
            'operation': result.operation,
            'file_size': result.file_size,
            'duration': result.duration,
            'throughput': result.throughput,
            'status': result.status,
            'error': result.error
        }
    
    def perform_download(self, filename: str, worker_id: int) -> Dict[str, Any]:
        """Download a file and measure performance - backward compatibility"""
        result = self.test_operations.perform_download(filename, worker_id)
        return {
            'worker_id': result.worker_id,
            'operation': result.operation,
            'file_size': result.file_size,
            'duration': result.duration,
            'throughput': result.throughput,
            'status': result.status,
            'error': result.error
        }
    
    def perform_list(self, worker_id: int) -> Dict[str, Any]:
        """Perform list operation and measure performance - backward compatibility"""
        result = self.test_operations.perform_list(worker_id)
        return {
            'worker_id': result.worker_id,
            'operation': result.operation,
            'duration': result.duration,
            'status': result.status,
            'error': result.error
        }
    
    def reset_counters(self) -> None:
        """Reset success and fail counters - backward compatibility"""
        self.performance_tracker.reset_counters()
    
    def run_stress_test(self, operation: str, file_size_mb: int, 
                       client_pool_size: int, executor_type: str = 'thread') -> Optional[Dict[str, Any]]:
        """Run a stress test with specified parameters"""
        self.reset_counters()
        
        if operation not in ['upload', 'download', 'list']:
            logging.error(f"Invalid operation: {operation}")
            return None
        
        logging.info(f"Starting {operation} stress test with {file_size_mb}MB files, "
                    f"{client_pool_size} {executor_type} workers")
        
        # Setup test file
        test_file = self._setup_test_file(operation, file_size_mb)
        if test_file is None and operation in ['upload', 'download']:
            return None
        
        # Run the test
        all_results = self._execute_stress_test(operation, test_file, client_pool_size, executor_type)
        
        # Calculate and return statistics
        stats = self.performance_tracker.calculate_statistics(
            operation, all_results, file_size_mb, client_pool_size, executor_type
        )
        
        if stats:
            self._log_test_completion(stats)
            return {
                'operation': stats.operation,
                'file_size_mb': stats.file_size_mb,
                'client_pool_size': stats.client_pool_size,
                'executor_type': stats.executor_type,
                'avg_duration': stats.avg_duration,
                'median_duration': stats.median_duration,
                'min_duration': stats.min_duration,
                'max_duration': stats.max_duration,
                'avg_throughput': stats.avg_throughput,
                'median_throughput': stats.median_throughput,
                'min_throughput': stats.min_throughput,
                'max_throughput': stats.max_throughput,
                'success_count': stats.success_count,
                'fail_count': stats.fail_count
            }
        
        return None
    
    def _setup_test_file(self, operation: str, file_size_mb: int) -> Optional[str]:
        """Setup test file for operations that need it"""
        if operation not in ['upload', 'download']:
            return None
        
        test_file = self.generate_test_file(file_size_mb)
        
        if operation == 'download':
            logging.info("Ensuring test file exists on server for download test")
            upload_result = self.test_operations.perform_upload(test_file, 0)
            if upload_result.status != 'OK':
                logging.error(f"Failed to upload test file to server: {upload_result.error}")
                return None
        
        return test_file
    
    def _execute_stress_test(self, operation: str, test_file: Optional[str], 
                           client_pool_size: int, executor_type: str) -> List[TestResult]:
        """Execute the stress test with specified parameters"""
        executor_class = (concurrent.futures.ThreadPoolExecutor 
                         if executor_type == 'thread' 
                         else concurrent.futures.ProcessPoolExecutor)
        
        all_results = []
        
        with executor_class(max_workers=client_pool_size) as executor:
            futures = self._submit_test_tasks(executor, operation, test_file, client_pool_size)
            all_results = self._collect_results(futures, operation)
        
        return all_results
    
    def _submit_test_tasks(self, executor, operation: str, test_file: Optional[str], 
                          client_pool_size: int) -> List:
        """Submit test tasks to executor"""
        futures = []
        
        for i in range(client_pool_size):
            if operation == 'upload':
                futures.append(executor.submit(self.test_operations.perform_upload, test_file, i))
            elif operation == 'download':
                filename = os.path.basename(test_file)
                futures.append(executor.submit(self.test_operations.perform_download, filename, i))
            else:  # list
                futures.append(executor.submit(self.test_operations.perform_list, i))
        
        return futures
    
    def _collect_results(self, futures: List, operation: str) -> List[TestResult]:
        """Collect results from completed futures"""
        all_results = []
        
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                all_results.append(result)
                self.performance_tracker.add_result(operation, result)
            except Exception as e:
                logging.error(f"Worker failed with exception: {str(e)}")
        
        return all_results
    
    def _log_test_completion(self, stats: TestStatistics) -> None:
        """Log test completion statistics"""
        logging.info(f"Test complete: {stats.success_count} succeeded, {stats.fail_count} failed")
        logging.info(f"Average duration: {stats.avg_duration:.2f}s, "
                    f"Average throughput: {stats.avg_throughput/1024/1024:.2f} MB/s")
    
    def run_all_tests(self, file_sizes: List[int], client_pool_sizes: List[int], 
                     server_pool_sizes: List[int], executor_types: List[str], 
                     operations: List[str]) -> None:
        """Run all test combinations and save results to CSV"""
        all_stats = []
        
        for server_pool_size in server_pool_sizes:
            logging.info(f"Tests for server pool size: {server_pool_size}")
            logging.info("Please restart the server with the appropriate pool size!")
            input("Press Enter when the server is ready...")
            
            for executor_type in executor_types:
                for operation in operations:
                    for file_size in file_sizes:
                        for client_pool_size in client_pool_sizes:
                            stats_dict = self.run_stress_test(operation, file_size, client_pool_size, executor_type)
                            if stats_dict:
                                stats = TestStatistics(**stats_dict)
                                stats.server_pool_size = server_pool_size
                                all_stats.append(stats)
        
        self.save_results_to_csv(all_stats)
    
    def save_results_to_csv(self, all_stats: Union[List[TestStatistics], List[Dict[str, Any]]]) -> str:
        """Save test results to CSV file - backward compatibility"""
        # Convert dict format to TestStatistics if needed
        if all_stats and isinstance(all_stats[0], dict):
            stats_objects = []
            for stats_dict in all_stats:
                stats = TestStatistics(**stats_dict)
                stats_objects.append(stats)
            all_stats = stats_objects
        
        return self.results_exporter.save_results_to_csv(all_stats)


class ConfigurationManager:
    """Handles configuration and logging setup"""
    
    @staticmethod
    def setup_logging() -> None:
        """Configure logging settings"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("stress_test.log"),
                logging.StreamHandler()
            ]
        )
    
    @staticmethod
    def parse_arguments() -> argparse.Namespace:
        """Parse command line arguments"""
        parser = argparse.ArgumentParser(description='File Server Stress Test Client')
        parser.add_argument('--host', default='localhost', help='Server host (default: localhost)')
        parser.add_argument('--port', type=int, default=6667, help='Server port (default: 6667)')
        parser.add_argument('--operation', choices=['upload', 'download', 'list', 'all'], default='all', 
                           help='Operation to test (default: all)')
        parser.add_argument('--file-sizes', type=int, nargs='+', default=[10, 50, 100], 
                           help='File sizes in MB (default: 10 50 100)')
        parser.add_argument('--client-pools', type=int, nargs='+', default=[1, 5, 10], 
                           help='Client worker pool sizes (default: 1 5 10)')
        parser.add_argument('--server-pools', type=int, nargs='+', default=[1, 5, 10], 
                           help='Server worker pool sizes to test against (default: 1 5 10)')
        parser.add_argument('--executor', choices=['thread', 'process', 'both'], default='thread', 
                           help='Executor type (default: thread)')
        parser.add_argument('--debug', action='store_true', help='Enable debug logging')
        
        return parser.parse_args()


def main() -> None:
    """Main function to run stress tests"""
    config_manager = ConfigurationManager()
    config_manager.setup_logging()
    
    args = config_manager.parse_arguments()
    
    # Set debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logging.debug("Debug logging enabled")
    
    # Initialize stress test client
    server_address = (args.host, args.port)
    client = StressTestClient(server_address)
    
    logging.info(f"Starting stress test client for server at {args.host}:{args.port}")
    
    # Determine operations to run
    operations = ['upload', 'download', 'list'] if args.operation == 'all' else [args.operation]
    
    # Determine executor types to test
    executor_types = ['thread', 'process'] if args.executor == 'both' else [args.executor]
    
    # Test server connectivity before starting
    logging.info("Testing server connectivity...")
    connectivity_result = client.send_command("LIST")
    if connectivity_result['status'] != 'OK':
        logging.error(f"Failed to connect to server: {connectivity_result['data']}")
        logging.error("Please ensure the server is running and accessible")
        sys.exit(1)
    
    logging.info("Server connectivity confirmed. Starting stress tests...")
    
    try:
        if len(args.server_pools) > 1:
            # Run comprehensive tests across multiple server pool sizes
            logging.info("Running comprehensive test suite across multiple server configurations")
            client.run_all_tests(
                file_sizes=args.file_sizes,
                client_pool_sizes=args.client_pools,
                server_pool_sizes=args.server_pools,
                executor_types=executor_types,
                operations=operations
            )
        else:
            # Run tests for single server configuration
            logging.info(f"Running tests for server pool size: {args.server_pools[0]}")
            all_stats = []
            
            for executor_type in executor_types:
                logging.info(f"Testing with {executor_type} executor")
                
                for operation in operations:
                    logging.info(f"Testing {operation} operation")
                    
                    for file_size in args.file_sizes:
                        logging.info(f"Testing with {file_size} MB files")
                        
                        for client_pool_size in args.client_pools:
                            logging.info(f"Testing with {client_pool_size} client workers")
                            
                            # Run individual stress test
                            stats_dict = client.run_stress_test(
                                operation=operation,
                                file_size_mb=file_size,
                                client_pool_size=client_pool_size,
                                executor_type=executor_type
                            )
                            
                            if stats_dict:
                                stats = TestStatistics(**stats_dict)
                                stats.server_pool_size = args.server_pools[0]
                                all_stats.append(stats)
                                
                                # Log summary for this test
                                logging.info(f"Test Summary - Operation: {operation}, "
                                           f"File Size: {file_size}MB, "
                                           f"Client Workers: {client_pool_size}, "
                                           f"Executor: {executor_type}")
                                logging.info(f"Success: {stats.success_count}, "
                                           f"Failed: {stats.fail_count}, "
                                           f"Avg Duration: {stats.avg_duration:.2f}s, "
                                           f"Avg Throughput: {stats.avg_throughput/1024/1024:.2f} MB/s")
                            else:
                                logging.error(f"Failed to complete test for {operation} operation")
                            
                            # Add small delay between tests to prevent overwhelming the server
                            time.sleep(1)
            
            # Save results to CSV
            if all_stats:
                csv_file = client.save_results_to_csv(all_stats)
                logging.info(f"All test results saved to: {csv_file}")
                
                # Print summary statistics
                print_test_summary(all_stats)
            else:
                logging.warning("No test results to save")
    
    except KeyboardInterrupt:
        logging.info("Test interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error during testing: {str(e)}")
        sys.exit(1)
    
    logging.info("Stress testing completed successfully")


def print_test_summary(all_stats: List[TestStatistics]) -> None:
    """Print a summary of all test results"""
    print("\n" + "="*80)
    print("STRESS TEST SUMMARY")
    print("="*80)
    
    # Group results by operation
    operations_summary = defaultdict(list)
    for stats in all_stats:
        operations_summary[stats.operation].append(stats)
    
    for operation, stats_list in operations_summary.items():
        print(f"\n{operation.upper()} OPERATION RESULTS:")
        print("-" * 40)
        
        total_success = sum(s.success_count for s in stats_list)
        total_failed = sum(s.fail_count for s in stats_list)
        
        print(f"Total Operations: {total_success + total_failed}")
        print(f"Successful: {total_success}")
        print(f"Failed: {total_failed}")
        print(f"Success Rate: {(total_success/(total_success + total_failed))*100:.1f}%" 
              if (total_success + total_failed) > 0 else "N/A")
        
        if stats_list:
            # Find best and worst performing configurations
            successful_stats = [s for s in stats_list if s.success_count > 0]
            
            if successful_stats:
                best_throughput = max(successful_stats, key=lambda x: x.avg_throughput)
                fastest_duration = min(successful_stats, key=lambda x: x.avg_duration)
                
                print(f"\nBest Throughput Configuration:")
                print(f"  File Size: {best_throughput.file_size_mb}MB, "
                      f"Client Pool: {best_throughput.client_pool_size}, "
                      f"Executor: {best_throughput.executor_type}")
                print(f"  Avg Throughput: {best_throughput.avg_throughput/1024/1024:.2f} MB/s")
                
                print(f"\nFastest Configuration:")
                print(f"  File Size: {fastest_duration.file_size_mb}MB, "
                      f"Client Pool: {fastest_duration.client_pool_size}, "
                      f"Executor: {fastest_duration.executor_type}")
                print(f"  Avg Duration: {fastest_duration.avg_duration:.2f}s")
    
    print("\n" + "="*80)


if __name__ == "__main__":
    main()
