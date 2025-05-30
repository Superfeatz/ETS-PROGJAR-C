import socket
import json
import base64
import logging
import os

server_address = ('0.0.0.0', 7777)

def send_command(command_str=""):
    global server_address
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(server_address)
    logging.warning(f"connecting to {server_address}")
    try:
        logging.warning(f"sending message ")
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
        logging.warning("data received from server:")
        return hasil
    except Exception as e:
        logging.warning(f"error during data receiving: {str(e)}")
        return False


def remote_list():
    command_str = f"LIST"
    hasil = send_command(command_str)
    if (hasil['status'] == 'OK'):
        print("daftar file : ")
        for nmfile in hasil['data']:
            print(f"- {nmfile}")
        return True
    else:
        print(f"Gagal: {hasil['data']}")
        return False


def remote_get(filename=""):
    command_str = f"GET {filename}"
    hasil = send_command(command_str)
    if (hasil['status'] == 'OK'):
        namafile = hasil['data_namafile']
        isifile = base64.b64decode(hasil['data_file'])
        fp = open(namafile, 'wb+')
        fp.write(isifile)
        fp.close()
        print(f"File {namafile} berhasil diunduh")
        return True
    else:
        print(f"Gagal: {hasil['data']}")
        return False


def remote_upload(filepath=""):
    if not os.path.exists(filepath):
        print(f"File {filepath} tidak ditemukan")
        return False
    
    try:
        with open(filepath, 'rb') as fp:
            file_content = base64.b64encode(fp.read()).decode()
        command_str = f"UPLOAD {filepath} {file_content}"
        hasil = send_command(command_str)
        if hasil['status'] == 'OK':
            print(f"File {filepath} berhasil diupload")
            return True
        else:
            print(f"Gagal: {hasil['data']}")
            return False
    except FileNotFoundError:
        print("File tidak ditemukan")
        return False
    except Exception as e:
        logging.warning(f"Error: {str(e)}")
        return False


def remote_delete(filename=""):
    command_str = f"DELETE {filename}"
    hasil = send_command(command_str)
    if hasil['status'] == 'OK':
        print(f"File {filename} berhasil dihapus")
        return True
    else:
        print(f"Gagal: {hasil['data']}")
        return False


if __name__ == '__main__':
    server_address = ('172.16.16.102', 6667)
    
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.warning("Client started")
    
    while True:
        command = input("Masukkan perintah (LIST, GET, UPLOAD, DELETE, EXIT): ").strip().upper()
        if command == 'LIST':
            remote_list()
        elif command.startswith('GET'):
            _, filename = command.split(maxsplit=1)
            remote_get(filename)
        elif command.startswith('UPLOAD'):
            _, filepath = command.split(maxsplit=1)
            remote_upload(filepath)
        elif command.startswith('DELETE'):
            _, filename = command.split(maxsplit=1)
            remote_delete(filename)
        elif command == 'EXIT':
            break
        else:
            print("Perintah tidak dikenali")