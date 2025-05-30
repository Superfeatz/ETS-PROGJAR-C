import socket
import logging
import signal
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple, Any
from file_protocol import FileProtocol

# logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)-10s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("server_threadpool.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ServerConfig:
    """Server configuration with sensible defaults"""
    host: str = '0.0.0.0'
    port: int = 6667
    pool_size: int = 50
    backlog: int = 50
    buffer_size: int = 131072  # 128KB
    socket_timeout: int = 1800  # 30 minutes
    graceful_shutdown_timeout: int = 30
    enable_keepalive: bool = True

@dataclass
class ServerMetrics:
    """Server performance metrics"""
    total_connections: int = 0
    active_connections: int = 0
    total_requests: int = 0
    total_errors: int = 0
    start_time: float = field(default_factory=time.time)
    connection_times: list = field(default_factory=list)
    
    def add_connection_time(self, duration: float):
        self.connection_times.append(duration)
        # Keep only last 1000 entries to prevent memory growth
        if len(self.connection_times) > 1000:
            self.connection_times = self.connection_times[-1000:]

class ConnectionHandler:
    """Enhanced connection handler with better error handling and metrics"""
    
    def __init__(self, file_protocol: FileProtocol, metrics: ServerMetrics):
        self.fp = file_protocol
        self.metrics = metrics
        
    def handle_client(self, connection: socket.socket, address: Tuple[str, int]) -> None:
        """Enhanced client handler with proper resource management"""
        start_time = time.time()
        thread_name = threading.current_thread().name
        
        logger.info(f"{thread_name}: Handling connection from {address}")
        
        with self._managed_connection(connection, address) as conn:
            try:
                self._process_client_requests(conn, address, thread_name)
            except Exception as e:
                logger.error(f"{thread_name}: Unexpected error handling {address}: {str(e)}")
                self.metrics.total_errors += 1
            finally:
                duration = time.time() - start_time
                self.metrics.add_connection_time(duration)
                logger.info(f"{thread_name}: Connection from {address} completed in {duration:.2f}s")
    
    @contextmanager
    def _managed_connection(self, connection: socket.socket, address: Tuple[str, int]):
        """Context manager for connection lifecycle"""
        self.metrics.active_connections += 1
        self.metrics.total_connections += 1
        
        try:
            # Configure connection
            connection.settimeout(1800)  # 30 minutes
            if hasattr(socket, 'SO_KEEPALIVE'):
                connection.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            
            yield connection
        finally:
            self.metrics.active_connections -= 1
            try:
                connection.close()
            except Exception as e:
                logger.warning(f"Error closing connection to {address}: {str(e)}")
    
    def _process_client_requests(self, connection: socket.socket, address: Tuple[str, int], thread_name: str):
        """Process client requests with enhanced buffering and error handling"""
        buffer = ""
        consecutive_errors = 0
        max_consecutive_errors = 5
        
        while consecutive_errors < max_consecutive_errors:
            try:
                data = connection.recv(131072)
                if not data:
                    logger.debug(f"{thread_name}: Client {address} disconnected normally")
                    break
                
                buffer += data.decode('utf-8', errors='replace')
                
                # Process complete commands
                while "\r\n\r\n" in buffer:
                    command, buffer = buffer.split("\r\n\r\n", 1)
                    
                    if command.strip():  # Only process non-empty commands
                        self._handle_command(connection, command, address, thread_name)
                        self.metrics.total_requests += 1
                        consecutive_errors = 0  # Reset error counter on successful command
                
            except socket.timeout:
                logger.warning(f"{thread_name}: Socket timeout for {address}")
                break
            except UnicodeDecodeError as e:
                logger.error(f"{thread_name}: Unicode decode error from {address}: {str(e)}")
                consecutive_errors += 1
                self.metrics.total_errors += 1
            except Exception as e:
                logger.error(f"{thread_name}: Error processing request from {address}: {str(e)}")
                consecutive_errors += 1
                self.metrics.total_errors += 1
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(f"{thread_name}: Too many consecutive errors from {address}, closing connection")
                    break
    
    def _handle_command(self, connection: socket.socket, command: str, address: Tuple[str, int], thread_name: str):
        """Handle individual command with error recovery"""
        try:
            logger.debug(f"{thread_name}: Processing command from {address}: {command[:50]}...")
            
            result = self.fp.proses_string(command)
            response = result + "\r\n\r\n"
            
            # Send response in chunks if it's large
            response_bytes = response.encode('utf-8')
            bytes_sent = 0
            chunk_size = 65536  # 64KB chunks
            
            while bytes_sent < len(response_bytes):
                chunk = response_bytes[bytes_sent:bytes_sent + chunk_size]
                sent = connection.send(chunk)
                if sent == 0:
                    raise socket.error("Socket connection broken")
                bytes_sent += sent
                
            logger.debug(f"{thread_name}: Response sent to {address}, {len(response_bytes)} bytes")
            
        except Exception as e:
            logger.error(f"{thread_name}: Error handling command from {address}: {str(e)}")
            # Try to send error response
            try:
                error_response = '{"status": "ERROR", "data": "Internal server error"}\r\n\r\n'
                connection.sendall(error_response.encode())
            except:
                pass  # If we can't send error response, connection is likely broken
            raise

class EnhancedThreadPoolServer:
    """Enhanced thread pool server with better lifecycle management"""
    
    def __init__(self, config: ServerConfig):
        self.config = config
        self.metrics = ServerMetrics()
        self.file_protocol = FileProtocol()
        self.connection_handler = ConnectionHandler(self.file_protocol, self.metrics)
        
        self.server_socket: Optional[socket.socket] = None
        self.executor: Optional[ThreadPoolExecutor] = None
        self.shutdown_event = threading.Event()
        self.running = False
        
        # Setup signal handlers for graceful shutdown
        self._setup_signal_handlers()
    
    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            self.shutdown()
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
    
    def _create_server_socket(self) -> socket.socket:
        """Create and configure server socket"""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Enable socket reuse
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Set socket timeout for accept operations
        sock.settimeout(1.0)  # 1 second timeout for accept
        
        # Bind and listen
        sock.bind((self.config.host, self.config.port))
        sock.listen(self.config.backlog)
        
        return sock
    
    def start(self):
        """Start the server with proper initialization"""
        if self.running:
            logger.warning("Server is already running")
            return
        
        try:
            logger.info(f"Starting enhanced thread pool server on {self.config.host}:{self.config.port}")
            logger.info(f"Configuration: pool_size={self.config.pool_size}, backlog={self.config.backlog}")
            
            # Create server socket
            self.server_socket = self._create_server_socket()
            
            # Create thread pool executor
            self.executor = ThreadPoolExecutor(
                max_workers=self.config.pool_size,
                thread_name_prefix="Worker"
            )
            
            self.running = True
            self.metrics.start_time = time.time()
            
            logger.info(f"Server started successfully, accepting connections...")
            
            # Start metrics logging thread
            metrics_thread = threading.Thread(target=self._log_metrics_periodically, daemon=True)
            metrics_thread.start()
            
            # Main server loop
            self._run_server_loop()
            
        except Exception as e:
            logger.error(f"Failed to start server: {str(e)}")
            self.shutdown()
            raise
    
    def _run_server_loop(self):
        """Main server loop with graceful shutdown support"""
        while self.running and not self.shutdown_event.is_set():
            try:
                connection, client_address = self.server_socket.accept()
                logger.debug(f"Accepted connection from {client_address}")
                
                # Submit connection handling to thread pool
                future = self.executor.submit(
                    self.connection_handler.handle_client,
                    connection,
                    client_address
                )
                
                # Optional: Add callback for future completion
                future.add_done_callback(self._handle_task_completion)
                
            except socket.timeout:
                # Normal timeout, check if we should shutdown
                continue
            except Exception as e:
                if self.running:
                    logger.error(f"Error accepting connection: {str(e)}")
                    time.sleep(0.1)  # Brief pause to prevent tight error loop
    
    def _handle_task_completion(self, future):
        """Handle completion of connection handling tasks"""
        try:
            future.result()  # This will raise any exception that occurred
        except Exception as e:
            logger.error(f"Connection handling task failed: {str(e)}")
    
    def _log_metrics_periodically(self):
        """Log server metrics periodically"""
        while self.running and not self.shutdown_event.is_set():
            time.sleep(60)  # Log every minute
            if self.running:
                uptime = time.time() - self.metrics.start_time
                avg_conn_time = (sum(self.metrics.connection_times) / len(self.metrics.connection_times) 
                               if self.metrics.connection_times else 0)
                
                logger.info(f"Server Metrics - Uptime: {uptime:.0f}s, "
                          f"Total Connections: {self.metrics.total_connections}, "
                          f"Active: {self.metrics.active_connections}, "
                          f"Requests: {self.metrics.total_requests}, "
                          f"Errors: {self.metrics.total_errors}, "
                          f"Avg Connection Time: {avg_conn_time:.2f}s")
    
    def shutdown(self):
        """Graceful server shutdown"""
        if not self.running:
            return
        
        logger.info("Initiating server shutdown...")
        self.running = False
        self.shutdown_event.set()
        
        # Close server socket
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception as e:
                logger.error(f"Error closing server socket: {str(e)}")
        
        # Shutdown thread pool
        if self.executor:
            logger.info(f"Shutting down thread pool, waiting up to {self.config.graceful_shutdown_timeout}s...")
            self.executor.shutdown(wait=True, timeout=self.config.graceful_shutdown_timeout)
        
        # Log final metrics
        uptime = time.time() - self.metrics.start_time
        logger.info(f"Server shutdown complete. Final stats - "
                   f"Uptime: {uptime:.0f}s, "
                   f"Total Connections: {self.metrics.total_connections}, "
                   f"Total Requests: {self.metrics.total_requests}, "
                   f"Total Errors: {self.metrics.total_errors}")

def main():
    """Main function with argument parsing"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced File Server with Thread Pool')
    parser.add_argument('--host', default='0.0.0.0', help='Server host (default: 0.0.0.0)')
    parser.add_argument('--port', type=int, default=6667, help='Server port (default: 6667)')
    parser.add_argument('--pool-size', type=int, default=50, help='Thread pool size (default: 50)')
    parser.add_argument('--backlog', type=int, default=50, help='Socket backlog (default: 50)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Create server configuration
    config = ServerConfig(
        host=args.host,
        port=args.port,
        pool_size=args.pool_size,
        backlog=args.backlog
    )
    
    # Create and start server
    server = EnhancedThreadPoolServer(config)
    
    try:
        server.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    except Exception as e:
        logger.error(f"Server error: {str(e)}")
    finally:
        server.shutdown()

if __name__ == "__main__":
    main()
