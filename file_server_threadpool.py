from socket import *
import socket
import logging
from file_protocol import FileProtocol
import concurrent.futures
import sys

fp = FileProtocol()

def handle_client(connection, address):
    """Function to handle client requests"""
    logging.warning(f"handling connection from {address}")
    buffer = ""
    try:
        # Increase socket timeout for large file transfers
        connection.settimeout(1800)  # 5 minutes timeout
        
        # Increase buffer size for better performance
        while True:
            data = connection.recv(131072)  # Increased from 32 to 8192 bytes
            if not data:
                break
            buffer += data.decode()
            while "\r\n\r\n" in buffer:
                command, buffer = buffer.split("\r\n\r\n", 1)
                hasil = fp.proses_string(command)
                response = hasil + "\r\n\r\n"
                connection.sendall(response.encode())
    except Exception as e:
        logging.warning(f"Error: {str(e)}")
    finally:
        logging.warning(f"connection from {address} closed")
        connection.close()


class Server:
    def __init__(self, ipaddress='0.0.0.0', port=8889, pool_size=1):
        self.ipinfo = (ipaddress, port)
        self.pool_size = pool_size
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Set socket timeout
        self.my_socket.settimeout(1800)  # 5 minutes timeout

    def run(self):
        logging.warning(f"server running on ip address {self.ipinfo} with thread pool size {self.pool_size}")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(1)  # Increased backlog
        
        # Create a ThreadPoolExecutor
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.pool_size) as executor:
            try:
                while True:
                    connection, client_address = self.my_socket.accept()
                    logging.warning(f"connection from {client_address}")
                    
                    # Submit the client handling task to the thread pool
                    executor.submit(handle_client, connection, client_address)
            except KeyboardInterrupt:
                logging.warning("Server shutting down")
            finally:
                if self.my_socket:
                    self.my_socket.close()


def main():
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='File Server')
    parser.add_argument('--port', type=int, default=6667, help='Server port (default: 6667)')
    parser.add_argument('--pool-size', type=int, default=1, help='Thread pool size (default: 1)')
    args = parser.parse_args()
    
    svr = Server(ipaddress='0.0.0.0', port=args.port, pool_size=args.pool_size)
    svr.run()


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    main()