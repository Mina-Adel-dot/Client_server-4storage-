import socket
import os

SERVER_IP = "10.72.16.168"
SERVER_PORT = 6001

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect((SERVER_IP, SERVER_PORT))

while True:
    action = input("Type UPLOAD, GET, or FINISH: ").strip().upper()
    
    if action == "FINISH":
        print("Exiting client.")
        break

    if action not in ["UPLOAD", "GET"]:
        print("Invalid option, try again.")
        continue

    client.send(action.ljust(10).encode())

    if action == "UPLOAD":
        path = input("Enter file path: ").strip()
        if not os.path.exists(path):
            print("File does not exist!")
            continue

        filename = os.path.basename(path)
        filesize = os.path.getsize(path)

        client.send(str(len(filename)).zfill(10).encode())
        client.send(filename.encode())
        client.send(str(filesize).zfill(20).encode())

        with open(path, "rb") as f:
            while True:
                data = f.read(4096)
                if not data:
                    break
                client.sendall(data)

        print("Upload complete.")

    elif action == "GET":
        filename = input("Enter filename: ").strip()
        client.send(str(len(filename)).zfill(10).encode())
        client.send(filename.encode())

        status = client.recv(10)
        if status == b"NOFILE":
            print("File not found.")
        else:
            filesize = int(client.recv(20).decode())
            with open("DL_" + filename, "wb") as f:
                remaining = filesize
                while remaining > 0:
                    chunk = client.recv(min(4096, remaining))
                    if not chunk:
                        break
                    f.write(chunk)
                    remaining -= len(chunk)

            print("Download complete.")

client.close()


