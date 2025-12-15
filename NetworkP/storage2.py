# storage_node_1.py
import socket
import os

HOST = "127.0.0.1"
PORT = 6002   # ← غيره لكل نود: 6001، 6002، 6003، 6004

FOLDER = "node1_storage"
os.makedirs(FOLDER, exist_ok=True)

def recv_exact(conn, size):
    data = b""
    while len(data) < size:
        chunk = conn.recv(size - len(data))
        if not chunk:
            return None
        data += chunk
    return data

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind((HOST, PORT))
server.listen(5)

print(f"Storage Node running on {PORT}")

while True:
    conn, addr = server.accept()
    cmd = recv_exact(conn, 10).decode().strip()
    fname_len = int(recv_exact(conn, 10).decode())
    fname = recv_exact(conn, fname_len).decode()

    path = os.path.join(FOLDER, fname)

    if cmd == "STORE":
        part_size = int(recv_exact(conn, 20).decode())
        with open(path, "wb") as f:
            remaining = part_size
            while remaining > 0:
                chunk = conn.recv(min(4096, remaining))
                if not chunk:
                    break
                f.write(chunk)
                remaining -= len(chunk)

    elif cmd == "FETCH":
        if not os.path.exists(path):
            conn.send(b"NOFILE".ljust(10))
        else:
            conn.send(b"FOUND".ljust(10))
            size = os.path.getsize(path)
            conn.send(str(size).zfill(20).encode())
            with open(path, "rb") as f:
                while True:
                    data = f.read(4096)
                    if not data:
                        break
                    conn.sendall(data)

    conn.close()
