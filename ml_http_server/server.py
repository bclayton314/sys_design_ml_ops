import socket

HOST = "127.0.0.1"
PORT = 8080


server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(5)

print(f"Server listening on http://{HOST}:{PORT}")

while True:
    client_socket, client_address = server_socket.accept()
    print(f"Accepted connection from {client_address}")

    request_data = client_socket.recv(1024)
    print("---- Raw Request ----")
    print(request_data.decode(errors="ignore"))
    print("---------------------")

    response_body = "Hello from your ML-focused HTTP server!"

    response = (
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: text/plain\r\n"
        f"Content-Length: {len(response_body)}\r\n"
        "\r\n"
        f"{response_body}"
    )

    client_socket.sendall(response.encode())
    client_socket.close()