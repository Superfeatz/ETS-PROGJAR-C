import socket
import json
import base64
import logging
import os
import sys

class FileClient:
    def __init__(self, server_address=('localhost', 7777)):
        self.server_address = server_address
        logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
        logging.warning("Client initialized")

    def send_command(self, command_str=""):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.server_address)
        logging.warning(f"connecting to {self.server_address}")
        try:
            logging.warning(f"sending message: {command_str}")
            sock.sendall((command_str + "\r\n\r\n").encode())
            data_received = "" 
            while True:
                data = sock.recv(16)
                if data:
                    data_received += data.decode()
                    if "\r\n\r\n" in data_received:
                        break
                else:
                    break
            json_response = data_received.split("\r\n\r\n")[0]
            hasil = json.loads(json_response)
            logging.warning("data received from server")
            return hasil
        except Exception as e:
            logging.warning(f"error during data sending/receiving: {str(e)}")
            return {'status': 'ERROR', 'data': str(e)}
        finally:
            sock.close()

    def list_files(self):
        command_str = "LIST"
        hasil = self.send_command(command_str)
        if hasil['status'] == 'OK':
            print("\nDaftar file di server:")
            print("=====================")
            if not hasil['data']:
                print("(Tidak ada file)")
            else:
                for i, nmfile in enumerate(hasil['data'], 1):
                    print(f"{i}. {nmfile}")
            return True
        else:
            print(f"\nGagal mendapatkan daftar file: {hasil['data']}")
            return False

    def download_file(self, filename=""):
        if not filename:
            print("\nNama file tidak boleh kosong!")
            return False
            
        print(f"\nMengunduh file: {filename}")
        command_str = f"GET {filename}"
        hasil = self.send_command(command_str)
        
        if hasil['status'] == 'OK':
            namafile = hasil['data_namafile']
            isifile = base64.b64decode(hasil['data_file'])
            
            # Make sure the downloads directory exists
            if not os.path.exists('downloads'):
                os.makedirs('downloads')
                
            filepath = os.path.join('downloads', namafile)
            with open(filepath, 'wb+') as fp:
                fp.write(isifile)
                
            print(f"✓ File {namafile} berhasil diunduh ke folder 'downloads'")
            return True
        else:
            print(f"✗ Gagal mengunduh file: {hasil['data']}")
            return False

    def upload_file(self, filepath=""):
        if not filepath:
            print("\nPath file tidak boleh kosong!")
            return False
            
        if not os.path.exists(filepath):
            print(f"\n✗ File {filepath} tidak ditemukan")
            return False
        
        try:
            filename = os.path.basename(filepath)
            print(f"\nMengunggah file: {filename}")
            
            with open(filepath, 'rb') as fp:
                file_content = base64.b64encode(fp.read()).decode()
                
            command_str = f"UPLOAD {filename} {file_content}"
            hasil = self.send_command(command_str)
            
            if hasil['status'] == 'OK':
                print(f"✓ File {filename} berhasil diunggah ke server")
                return True
            else:
                print(f"✗ Gagal mengunggah file: {hasil['data']}")
                return False
        except Exception as e:
            print(f"✗ Error saat mengunggah file: {str(e)}")
            return False

    def delete_file(self, filename=""):
        if not filename:
            print("\nNama file tidak boleh kosong!")
            return False
            
        print(f"\nMenghapus file: {filename}")
        command_str = f"DELETE {filename}"
        hasil = self.send_command(command_str)
        
        if hasil['status'] == 'OK':
            print(f"✓ File {filename} berhasil dihapus dari server")
            return True
        else:
            print(f"✗ Gagal menghapus file: {hasil['data']}")
            return False

    def run_cli(self):
        print("\n===== FILE CLIENT =====")
        print(f"Terhubung ke server: {self.server_address[0]}:{self.server_address[1]}")
        
        while True:
            print("\nPilih perintah:")
            print("1. LIST - Tampilkan daftar file")
            print("2. GET - Unduh file")
            print("3. UPLOAD - Unggah file")
            print("4. DELETE - Hapus file")
            print("5. EXIT - Keluar")
            
            try:
                choice = input("\nMasukkan pilihan (1-5): ").strip()
                
                if choice == "1":
                    self.list_files()
                    
                elif choice == "2":
                    self.list_files()
                    filename = input("\nMasukkan nama file yang akan diunduh: ").strip()
                    self.download_file(filename)
                    
                elif choice == "3":
                    filepath = input("\nMasukkan path file yang akan diunggah: ").strip()
                    self.upload_file(filepath)
                    
                elif choice == "4":
                    self.list_files()
                    filename = input("\nMasukkan nama file yang akan dihapus: ").strip()
                    self.delete_file(filename)
                    
                elif choice == "5":
                    print("\nKeluar dari program...")
                    break
                    
                else:
                    print("\n✗ Pilihan tidak valid. Silakan pilih 1-5.")
                    
            except Exception as e:
                print(f"\n✗ Error: {str(e)}")

if __name__ == '__main__':
    # Parse command-line arguments for server address and port
    server_ip = '172.16.16.102'  # Default
    server_port = 6667     # Default
    
    try:
        if len(sys.argv) > 1:
            server_ip = sys.argv[1]
        if len(sys.argv) > 2:
            server_port = int(sys.argv[2])
    except Exception as e:
        print(f"Error parsing arguments: {str(e)}")
        print("Usage: python file_client.py [server_ip] [server_port]")
        sys.exit(1)
    
    client = FileClient((server_ip, server_port))
    client.run_cli()