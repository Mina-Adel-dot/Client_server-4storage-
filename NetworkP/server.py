import socket
import math

HOST = "127.0.0.1"
PORT = 5001

# 4 Storage Nodes
STORAGE_NODES = [
    ("127.0.0.1", 6001),
    ("127.0.0.1", 6002),
    ("127.0.0.1", 6003),
    ("127.0.0.1", 6004),
]

def connect_storage(ip, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    return s

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

print("Main Server running on port 5001...")

while True:
    conn, addr = server.accept()
    print(f"Client connected: {addr}")

    command = recv_exact(conn, 10).decode().strip()

    if command == "UPLOAD":
        # استقبال اسم الملف وحجمه
        filename_len = int(recv_exact(conn, 10).decode())
        filename = recv_exact(conn, filename_len).decode()
        filesize = int(recv_exact(conn, 20).decode())

        # تقسيم الملف إلى 4 أجزاء متساوية تقريبًا
        part_size = math.ceil(filesize / 4)

        for i, (ip, port) in enumerate(STORAGE_NODES):
            st = connect_storage(ip, port)
            st.send(b"STORE".ljust(10))
            st.send(str(len(filename)).zfill(10).encode())
            st.send(filename.encode())
            
            # آخر جزء قد يكون أصغر من part_size
            if i == 3:
                this_part_size = filesize - part_size * 3
            else:
                this_part_size = part_size

            st.send(str(this_part_size).zfill(20).encode())

            # إرسال جزء من البيانات من العميل إلى الـ Storage Node
            remaining = this_part_size
            while remaining > 0:
                chunk = conn.recv(min(4096, remaining))
                if not chunk:
                    break
                st.sendall(chunk)
                remaining -= len(chunk)

            st.close()

        print(f"File '{filename}' uploaded across 4 storage nodes.")

    elif command == "GET":
        filename_len = int(recv_exact(conn, 10).decode())
        filename = recv_exact(conn, filename_len).decode()

        full_data = b""
        found_any = False

        for ip, port in STORAGE_NODES:
            st = connect_storage(ip, port)
            st.send(b"FETCH".ljust(10))
            st.send(str(len(filename)).zfill(10).encode())
            st.send(filename.encode())

            status = st.recv(10)
            if status == b"NOFILE":
                st.close()
                continue

            found_any = True
            part_size = int(st.recv(20).decode())

            remaining = part_size
            while remaining > 0:
                chunk = st.recv(min(4096, remaining))
                if not chunk:
                    break
                full_data += chunk
                remaining -= len(chunk)

            st.close()

        if not found_any:
            conn.send(b"NOFILE")
        else:
            conn.send(b"FOUND")
            conn.send(str(len(full_data)).zfill(20).encode())
            conn.sendall(full_data)

        print(f"File '{filename}' retrieved from storage nodes and sent to client.")

    conn.close()
