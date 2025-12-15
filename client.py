import socket
import os

SERVER_IP = '127.0.0.1'
SERVER_PORT = 5001
BUFFER = 1024

filename = input("Enter filename: ")

if not os.path.exists(filename):
    print("File not found!")
    exit()

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((SERVER_IP, SERVER_PORT))


client_socket.send(filename.ljust(100).encode())
filesize = os.path.getsize(filename)
client_socket.send(str(filesize).ljust(20).encode())


with open(filename, "rb") as f:
    while True:
        chunk = f.read(BUFFER)
        if not chunk:
            break
        client_socket.sendall(chunk)
print("File uploaded.")


while True:
    cmd = input("Type 'back' to get file back (or 'exit' to quit): ").strip().lower()
    if cmd == "back":
        client_socket.send(cmd.ljust(50).encode())
        break
    elif cmd == "exit":
        print("Exiting without retrieving file.")
        client_socket.close()
        exit()
    else:
        print("Unknown command. Type 'back' or 'exit'.")


final_size = int(client_socket.recv(20).decode().strip())
received = 0
with open("returned_" + filename, "wb") as f:
    while received < final_size:
        chunk = client_socket.recv(min(BUFFER, final_size - received))
        f.write(chunk)
        received += len(chunk)
print("File returned successfully.")

client_socket.close()
