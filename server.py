import socket
import os


SERVER_IP = '127.0.0.1'
SERVER_PORT = 5001
BUFFER = 1024


STORAGE_SERVERS = [
    ('127.0.0.1', 6001),
    ('127.0.0.1', 6002),
    ('127.0.0.1', 6003),
    ('127.0.0.1', 6004)
]

NUM_STORAGE = len(STORAGE_SERVERS)

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((SERVER_IP, SERVER_PORT))
server_socket.listen(1)
print(f"[MAIN SERVER] running on {SERVER_IP}:{SERVER_PORT}")

conn, addr = server_socket.accept()
print(f"Client connected: {addr}")


filename = conn.recv(100).decode().strip()
filesize = int(conn.recv(20).decode().strip())


received = 0
with open(filename, "wb") as f:
    while received < filesize:
        chunk = conn.recv(min(BUFFER, filesize - received))
        if not chunk:
            break
        f.write(chunk)
        received += len(chunk)
print(f"File received: {filename}")

part_size = (filesize + NUM_STORAGE - 1) // NUM_STORAGE
parts = []
with open(filename, "rb") as f:
    for i in range(NUM_STORAGE):
        partname = f"{filename}.part{i+1}"
        with open(partname, "wb") as pf:
            if i < NUM_STORAGE - 1:
                pf.write(f.read(part_size))
            else:
                pf.write(f.read())
        parts.append(partname)
print(f"File split into {NUM_STORAGE} parts.")


for i in range(NUM_STORAGE):
    sp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sp.connect(STORAGE_SERVERS[i])
    sp.send("STORE".ljust(50).encode())
    sp.send(parts[i].ljust(100).encode())
    sp.send(str(os.path.getsize(parts[i])).ljust(20).encode())

    with open(parts[i], "rb") as f:
        while True:
            chunk = f.read(BUFFER)
            if not chunk:
                break
            sp.sendall(chunk)
    sp.close()
print("All parts stored on storage servers.")


cmd = conn.recv(50).decode().strip().lower()
if cmd == "back":
    print("Client requested the file back.")


    for i in range(NUM_STORAGE):
        sp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sp.connect(STORAGE_SERVERS[i])
        sp.send("GET".ljust(50).encode())
        sp.send(parts[i].ljust(100).encode())

        partsize = int(sp.recv(20).decode().strip())
        with open(f"restore_{parts[i]}", "wb") as f:
            received = 0
            while received < partsize:
                chunk = sp.recv(min(BUFFER, partsize - received))
                f.write(chunk)
                received += len(chunk)
        sp.close()
    print("All parts received back.")


    final_name = "reconstructed_" + filename
    with open(final_name, "wb") as final:
        for pn in parts:
            with open("restore_" + pn, "rb") as p:
                final.write(p.read())
    print(f"File reconstructed successfully as {final_name}")

  
    final_size = os.path.getsize(final_name)
    conn.send(str(final_size).ljust(20).encode())
    with open(final_name, "rb") as f:
        while True:
            chunk = f.read(BUFFER)
            if not chunk:
                break
            conn.sendall(chunk)

conn.close()
server_socket.close()
