import socket
import logging
import multiprocessing
import concurrent.futures
import argparse
from typing import Tuple, Optional
from file_protocol import FileProtocol


class ClientHandler:
    """Class to handle client connections and requests"""
    
    def __init__(self, buffer_size: int = 131072):
        self.buffer_size = buffer_size
        self.file_protocol = FileProtocol()
        self.message_delimiter = "\r\n\r\n"
    
    def handle_client(self, connection: socket.socket, address: Tuple[str, int]) -> None:
        """Handle individual client connection"""
        logging.warning(f"handling connection from {address}")
        buffer = ""
        
        try:
            while True:
                data = connection.recv(self.buffer_size)
                if not data:
                    break
                
                buffer += data.decode()
                buffer = self._process_buffer(connection, buffer)
                
        except Exception as e:
            logging.warning(f"Error handling client {address}: {str(e)}")
        finally:
            self._cleanup_connection(connection, address)
    
    def _process_buffer(self, connection: socket.socket, buffer: str) -> str:
        """Process commands from buffer and send responses"""
        while self.message_delimiter in buffer:
            command, buffer = buffer.split(self.message_delimiter, 1)
            response = self._generate_response(command)
            connection.sendall(response.encode())
        return buffer
    
    def _generate_response(self, command: str) -> str:
        """Generate response for a command"""
        hasil = self.file_protocol.proses_string(command)
        return hasil + self.message_delimiter
    
    def _cleanup_connection(self, connection: socket.socket, address: Tuple[str, int]) -> None:
        """Clean up connection resources"""
        logging.warning(f"connection from {address} closed")
        connection.close()


class ProcessPoolFileServer:
    """File server using process pool for handling concurrent connections"""
    
    DEFAULT_IP = '0.0.0.0'
    DEFAULT_PORT = 8889
    DEFAULT_POOL_SIZE = 5
    
    def __init__(self, ipaddress: str = DEFAULT_IP, port: int = DEFAULT_PORT, pool_size: int = DEFAULT_POOL_SIZE):
        self.ipinfo = (ipaddress, port)
        self.pool_size = pool_size
        self.socket = None
        self.client_handler = ClientHandler()
        self._setup_socket()
    
    def _setup_socket(self) -> None:
        """Initialize and configure the server socket"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    def run(self) -> None:
        """Start the server and handle connections"""
        self._log_server_start()
        self._bind_and_listen()
        
        with concurrent.futures.ProcessPoolExecutor(max_workers=self.pool_size) as executor:
            self._handle_connections(executor)
    
    def _log_server_start(self) -> None:
        """Log server startup information"""
        logging.warning(f"server running on ip address {self.ipinfo} with process pool size {self.pool_size}")
    
    def _bind_and_listen(self) -> None:
        """Bind socket to address and start listening"""
        self.socket.bind(self.ipinfo)
        self.socket.listen(1)
    
    def _handle_connections(self, executor: concurrent.futures.ProcessPoolExecutor) -> None:
        """Main connection handling loop"""
        try:
            while True:
                connection, client_address = self.socket.accept()
                logging.warning(f"connection from {client_address}")
                
                # Submit client handling task to process pool
                executor.submit(self.client_handler.handle_client, connection, client_address)
                
        except KeyboardInterrupt:
            logging.warning("Server shutting down")
        except Exception as e:
            logging.warning(f"Error in server: {str(e)}")
        finally:
            self._cleanup_server()
    
    def _cleanup_server(self) -> None:
        """Clean up server resources"""
        if self.socket:
            self.socket.close()


class ServerConfiguration:
    """Handle server configuration and argument parsing"""
    
    @staticmethod
    def parse_arguments() -> argparse.Namespace:
        """Parse command line arguments"""
        parser = argparse.ArgumentParser(description='File Server with Process Pool')
        parser.add_argument('--port', type=int, default=6667, 
                          help='Server port (default: 6667)')
        parser.add_argument('--pool-size', type=int, default=5, 
                          help='Process pool size (default: 5)')
        return parser.parse_args()
    
    @staticmethod
    def setup_logging() -> None:
        """Configure logging settings"""
        logging.basicConfig(
            level=logging.WARNING, 
            format='%(asctime)s - %(levelname)s - %(message)s'
        )


# Global function for backward compatibility
def handle_client(connection: socket.socket, address: Tuple[str, int]) -> None:
    """Global function to handle client requests (for backward compatibility)"""
    handler = ClientHandler()
    handler.handle_client(connection, address)


# Backward compatible Server class
class Server(ProcessPoolFileServer):
    """Backward compatible server class"""
    pass


def main() -> None:
    """Main function to start the server"""
    config = ServerConfiguration()
    config.setup_logging()
    
    args = config.parse_arguments()
    
    # Create and run server with parsed arguments
    server = ProcessPoolFileServer(
        ipaddress='0.0.0.0', 
        port=args.port, 
        pool_size=args.pool_size
    )
    server.run()
    
    # Original duplicate server creation (preserved for backward compatibility)
    fallback_server = ProcessPoolFileServer(
        ipaddress='0.0.0.0', 
        port=6667, 
        pool_size=5
    )
    fallback_server.run()


if __name__ == "__main__":
    # Support for multiprocessing on all platforms
    multiprocessing.freeze_support()
    main()
