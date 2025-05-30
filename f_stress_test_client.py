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

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("stress_test.log"),
        logging.StreamHandler()
    ]
)

class StressTestClient:
    def __init__(self, server_address=('localhost', 6667)):
        self.server_address = server_address
        self.results = {
            'upload': [],
            'download': [],
            'list': []
        }
        self.success_count = {
            'upload': 0,
            'download': 0,
            'list': 0
        }
        self.fail_count = {
            'upload': 0,
            'download': 0,
            'list': 0
        }
        
        # Create test files directory if it doesn't exist
        if not os.path.exists('test_files'):
            os.makedirs('test_files')
        
        # Create downloads directory if it doesn't exist
        if not os.path.exists('downloads'):
            os.makedirs('downloads')

    def generate_test_file(self, size_mb):
        """Generate a test file of specified size"""
        filename = f"test_file_{size_mb}MB.bin"
        filepath = os.path.join('test_files', filename)
        
        # Check if the file already exists with the correct size
        if os.path.exists(filepath) and os.path.getsize(filepath) == size_mb * 1024 * 1024:
            logging.info(f"Test file {filename} already exists with correct size")
            return filepath
        
        logging.info(f"Generating test file: {filename} ({size_mb} MB)")
        with open(filepath, 'wb') as f:
            # Generate chunks of 1MB to avoid memory issues
            chunk_size = 1024 * 1024  # 1MB
            for _ in range(size_mb):
                f.write(os.urandom(chunk_size))
        
        logging.info(f"Test file generated: {filepath}")
        return filepath

    def send_command(self, command_str=""):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(600)  # Increased to 5 minutes for large files
        try:
            start_connect = time.time()
            sock.connect(self.server_address)
            connect_time = time.time() - start_connect
            logging.debug(f"Connection established in {connect_time:.2f}s")
            
            # Send command in chunks if it's large
            chunks = [command_str[i:i+65536] for i in range(0, len(command_str), 65536)]
            for chunk in chunks:
                sock.sendall((chunk).encode())
            
            # Send terminator
            sock.sendall("\r\n\r\n".encode())
            
            # Receive response in chunks
            data_received = "" 
            while True:
                try:
                    data = sock.recv(8192)  # Increased buffer size
                    if data:
                        data_received += data.decode()
                        if "\r\n\r\n" in data_received:
                            break
                    else:
                        break
                except socket.timeout:
                    logging.error("Socket timeout while receiving data")
                    return {'status': 'ERROR', 'data': 'Socket timeout while receiving data'}
            
            json_response = data_received.split("\r\n\r\n")[0]
            hasil = json.loads(json_response)
            return hasil
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

    def perform_upload(self, file_path, worker_id):
        """Upload a file and measure performance"""
        start_time = time.time()
        filename = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        
        try:
            logging.info(f"Worker {worker_id}: Starting upload of {filename} ({file_size/1024/1024:.2f} MB)")
            
            # Read file in chunks to avoid memory issues with large files
            with open(file_path, 'rb') as fp:
                file_content = base64.b64encode(fp.read()).decode()
            
            # Prepare command
            command_str = f"UPLOAD {filename} {file_content}"
            
            # Send command
            result = self.send_command(command_str)
            
            end_time = time.time()
            duration = end_time - start_time
            throughput = file_size / duration if duration > 0 else 0
            
            if result['status'] == 'OK':
                logging.info(f"Worker {worker_id}: Upload successful - {filename} ({file_size/1024/1024:.2f} MB) in {duration:.2f}s - {throughput/1024/1024:.2f} MB/s")
                self.success_count['upload'] += 1
            else:
                logging.error(f"Worker {worker_id}: Upload failed - {filename}: {result['data']}")
                self.fail_count['upload'] += 1
                
            return {
                'worker_id': worker_id,
                'operation': 'upload',
                'file_size': file_size,
                'duration': duration,
                'throughput': throughput,
                'status': result['status']
            }
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            logging.error(f"Worker {worker_id}: Upload exception - {filename}: {str(e)}")
            self.fail_count['upload'] += 1
            return {
                'worker_id': worker_id,
                'operation': 'upload',
                'file_size': file_size,
                'duration': duration,
                'throughput': 0,
                'status': 'ERROR',
                'error': str(e)
            }

    def perform_download(self, filename, worker_id):
        """Download a file and measure performance"""
        start_time = time.time()
        
        try:
            logging.info(f"Worker {worker_id}: Starting download of {filename}")
            
            command_str = f"GET {filename}"
            result = self.send_command(command_str)
            
            if result['status'] == 'OK':
                file_content = base64.b64decode(result['data_file'])
                file_size = len(file_content)
                
                # Save to downloads folder with worker ID prefix to avoid conflicts
                download_path = os.path.join('downloads', f"worker{worker_id}_{filename}")
                with open(download_path, 'wb') as f:
                    f.write(file_content)
                
                end_time = time.time()
                duration = end_time - start_time
                throughput = file_size / duration if duration > 0 else 0
                
                logging.info(f"Worker {worker_id}: Download successful - {filename} ({file_size/1024/1024:.2f} MB) in {duration:.2f}s - {throughput/1024/1024:.2f} MB/s")
                self.success_count['download'] += 1
                
                return {
                    'worker_id': worker_id,
                    'operation': 'download',
                    'file_size': file_size,
                    'duration': duration,
                    'throughput': throughput,
                    'status': 'OK'
                }
            else:
                end_time = time.time()
                duration = end_time - start_time
                logging.error(f"Worker {worker_id}: Download failed - {filename}: {result['data']}")
                self.fail_count['download'] += 1
                
                return {
                    'worker_id': worker_id,
                    'operation': 'download',
                    'file_size': 0,
                    'duration': duration,
                    'throughput': 0,
                    'status': 'ERROR',
                    'error': result['data']
                }
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            logging.error(f"Worker {worker_id}: Download exception - {filename}: {str(e)}")
            self.fail_count['download'] += 1
            
            return {
                'worker_id': worker_id,
                'operation': 'download',
                'file_size': 0,
                'duration': duration,
                'throughput': 0,
                'status': 'ERROR',
                'error': str(e)
            }

    def perform_list(self, worker_id):
        """Perform list operation and measure performance"""
        start_time = time.time()
        
        try:
            command_str = "LIST"
            result = self.send_command(command_str)
            
            end_time = time.time()
            duration = end_time - start_time
            
            if result['status'] == 'OK':
                file_count = len(result['data'])
                logging.info(f"Worker {worker_id}: List successful - {file_count} files in {duration:.2f}s")
                self.success_count['list'] += 1
            else:
                logging.error(f"Worker {worker_id}: List failed: {result['data']}")
                self.fail_count['list'] += 1
                
            return {
                'worker_id': worker_id,
                'operation': 'list',
                'duration': duration,
                'status': result['status']
            }
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            logging.error(f"Worker {worker_id}: List exception: {str(e)}")
            self.fail_count['list'] += 1
            
            return {
                'worker_id': worker_id,
                'operation': 'list',
                'duration': duration,
                'status': 'ERROR',
                'error': str(e)
            }

    def reset_counters(self):
        """Reset success and fail counters"""
        self.success_count = {
            'upload': 0,
            'download': 0,
            'list': 0
        }
        self.fail_count = {
            'upload': 0,
            'download': 0,
            'list': 0
        }
        self.results = {
            'upload': [],
            'download': [],
            'list': []
        }

    def run_stress_test(self, operation, file_size_mb, client_pool_size, executor_type='thread'):
        """Run a stress test with specified parameters"""
        self.reset_counters()
        
        if operation not in ['upload', 'download', 'list']:
            logging.error(f"Invalid operation: {operation}")
            return
            
        logging.info(f"Starting {operation} stress test with {file_size_mb}MB files, {client_pool_size} {executor_type} workers")
        
        # Generate test file if needed for upload tests
        test_file = None
        if operation == 'upload' or operation == 'download':
            test_file = self.generate_test_file(file_size_mb)
        
        # First, ensure file exists on server for download tests
        if operation == 'download':
            logging.info(f"Ensuring test file exists on server for download test")
            upload_result = self.perform_upload(test_file, 0)  # Upload with worker ID 0 (setup)
            if upload_result['status'] != 'OK':
                logging.error(f"Failed to upload test file to server: {upload_result.get('error', 'Unknown error')}")
                return None
        
        # Choose the executor based on type
        if executor_type == 'thread':
            executor_class = concurrent.futures.ThreadPoolExecutor
        else:  # process
            executor_class = concurrent.futures.ProcessPoolExecutor
        
        # Run the stress test
        all_results = []
        
        with executor_class(max_workers=client_pool_size) as executor:
            futures = []
            
            for i in range(client_pool_size):
                if operation == 'upload':
                    futures.append(executor.submit(self.perform_upload, test_file, i))
                elif operation == 'download':
                    file_name = os.path.basename(test_file)
                    futures.append(executor.submit(self.perform_download, file_name, i))
                else:  # list
                    futures.append(executor.submit(self.perform_list, i))
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    all_results.append(result)
                    self.results[operation].append(result)
                except Exception as e:
                    logging.error(f"Worker failed with exception: {str(e)}")
        
        # Calculate statistics
        durations = [r['duration'] for r in all_results if r['status'] == 'OK']
        throughputs = [r['throughput'] for r in all_results if r.get('throughput', 0) > 0]
        
        if not durations:
            logging.warning("No successful operations to calculate statistics")
            return {
                'operation': operation,
                'file_size_mb': file_size_mb,
                'client_pool_size': client_pool_size,
                'executor_type': executor_type,
                'success_count': self.success_count[operation],
                'fail_count': self.fail_count[operation]
            }
        
        stats = {
            'operation': operation,
            'file_size_mb': file_size_mb,
            'client_pool_size': client_pool_size,
            'executor_type': executor_type,
            'avg_duration': statistics.mean(durations) if durations else 0,
            'median_duration': statistics.median(durations) if durations else 0,
            'min_duration': min(durations) if durations else 0,
            'max_duration': max(durations) if durations else 0,
            'avg_throughput': statistics.mean(throughputs) if throughputs else 0,
            'median_throughput': statistics.median(throughputs) if throughputs else 0,
            'min_throughput': min(throughputs) if throughputs else 0,
            'max_throughput': max(throughputs) if throughputs else 0,
            'success_count': self.success_count[operation],
            'fail_count': self.fail_count[operation]
        }
        
        logging.info(f"Test complete: {stats['success_count']} succeeded, {stats['fail_count']} failed")
        logging.info(f"Average duration: {stats['avg_duration']:.2f}s, Average throughput: {stats['avg_throughput']/1024/1024:.2f} MB/s")
        
        return stats

    def run_all_tests(self, file_sizes, client_pool_sizes, server_pool_sizes, executor_types, operations):
        """Run all test combinations and save results to CSV"""
        all_stats = []
        
        # For each server configuration, we'd need to manually restart the server
        for server_pool_size in server_pool_sizes:
            logging.info(f"Tests for server pool size: {server_pool_size}")
            logging.info("Please restart the server with the appropriate pool size!")
            input("Press Enter when the server is ready...")
            
            for executor_type in executor_types:
                for operation in operations:
                    for file_size in file_sizes:
                        for client_pool_size in client_pool_sizes:
                            stats = self.run_stress_test(operation, file_size, client_pool_size, executor_type)
                            if stats:
                                stats['server_pool_size'] = server_pool_size
                                all_stats.append(stats)
        
        # Save all results to CSV
        self.save_results_to_csv(all_stats)
        
    def save_results_to_csv(self, all_stats):
        """Save test results to CSV file"""
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        csv_filename = f"stress_test_results_{timestamp}.csv"
        
        with open(csv_filename, 'w', newline='') as csvfile:
            fieldnames = [
                'operation', 'file_size_mb', 'client_pool_size', 'server_pool_size', 'executor_type',
                'avg_duration', 'median_duration', 'min_duration', 'max_duration',
                'avg_throughput', 'median_throughput', 'min_throughput', 'max_throughput',
                'success_count', 'fail_count'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for stats in all_stats:
                writer.writerow(stats)
        
        logging.info(f"Results saved to {csv_filename}")
        return csv_filename

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='File Server Stress Test Client')
    parser.add_argument('--host', default='localhost', help='Server host (default: localhost)')
    parser.add_argument('--port', type=int, default=6667, help='Server port (default: 6667)')
    parser.add_argument('--operation', choices=['upload', 'download', 'list', 'all'], default='all', 
                        help='Operation to test (default: all)')
    parser.add_argument('--file-sizes', type=int, nargs='+', default=[10, 50, 100], 
                        help='File sizes in MB (default: 10 50 100)')
    parser.add_argument('--client-pools', type=int, nargs='+', default=[1, 5, 50], 
                        help='Client worker pool sizes (default: 1 5 50)')
    parser.add_argument('--server-pools', type=int, nargs='+', default=[1, 5, 50], 
                        help='Server worker pool sizes to test against (default: 1 5 50)')
    parser.add_argument('--executor', choices=['thread', 'process', 'both'], default='thread', 
                        help='Executor type (default: thread)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Prepare test parameters
    file_sizes = args.file_sizes
    client_pool_sizes = args.client_pools
    server_pool_sizes = args.server_pools
    
    if args.executor == 'both':
        executor_types = ['thread', 'process']
    else:
        executor_types = [args.executor]
        
    if args.operation == 'all':
        operations = ['list', 'download', 'upload']
    else:
        operations = [args.operation]
    
    # Create and run stress test client
    client = StressTestClient((args.host, args.port))
    
    # Run a single test if specific parameters are provided
    if len(operations) == 1 and len(file_sizes) == 1 and len(client_pool_sizes) == 1 and len(server_pool_sizes) == 1:
        logging.info(f"Running a single test with operation={operations[0]}, file_size={file_sizes[0]}MB, client_pool={client_pool_sizes[0]}")
        stats = client.run_stress_test(operations[0], file_sizes[0], client_pool_sizes[0], executor_types[0])
        if stats:
            stats['server_pool_size'] = server_pool_sizes[0]
            client.save_results_to_csv([stats])
    else:
        # Run all test combinations
        client.run_all_tests(file_sizes, client_pool_sizes, server_pool_sizes, executor_types, operations)
