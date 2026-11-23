
import socket
import time

HOST = "127.0.0.1"
PORT = 8080

TOTAL_SIZE = 64 * 1024 * 1024  # 64 MB

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Keep the client's receive buffer small to create backpressure.
client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)

client.connect((HOST, PORT))
print("Connected to server")

payload = b"x" * TOTAL_SIZE

print("Sending 64 MB...")
client.sendall(payload)
print("All data sent!")

print("Not reading server response for 10 seconds...")
time.sleep(10)

print("Now reading echoed data...")

received = 0

while received < TOTAL_SIZE:
    data = client.recv(65536)

    if not data:
        break

    received += len(data)

print(f"Received: {received} bytes")

client.close()
