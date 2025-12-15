import socket
import os

# Programmer defined IP and Port
IP = '127.0.0.1'   
PORT = 6002        
BUFFER = 1024

def recv_exact(conn, size):
    data = b""
    while len(data) < size:
        packet = conn.recv(size - len(data))
        if not packet:
            return data
        data += packet
    return data

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((IP, PORT))
s.listen(1)
print(f"[STORAGE SERVER] running on {IP}:{PORT}")

while True:
    conn, addr = s.accept()
    print(f"Connected from: {addr}")

    command = recv_exact(conn, 50).decode().strip()

    if command == "STORE":
        partname = recv_exact(conn, 100).decode().strip()
        partsize = int(recv_exact(conn, 20).decode().strip())

        received = 0
        with open(partname, "wb") as f:
            while received < partsize:
                chunk = conn.recv(min(BUFFER, partsize - received))
                f.write(chunk)
                received += len(chunk)
        print(f"Stored part: {partname}")

    elif command == "GET":
        partname = recv_exact(conn, 100).decode().strip()
        partsize = os.path.getsize(partname)
        conn.send(str(partsize).ljust(20).encode())

        with open(partname, "rb") as f:
            while True:
                chunk = f.read(BUFFER)
                if not chunk:
                    break
                conn.sendall(chunk)
        print(f"Sent part: {partname}")

    conn.close()
