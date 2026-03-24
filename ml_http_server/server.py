import socket
import json

HOST = "127.0.0.1"
PORT = 8080


def json_response(status_line, body_dict):
    response_body = json.dumps(body_dict)

    response = (
        f"{status_line}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(response_body)}\r\n"
        "\r\n"
        f"{response_body}"
    )
    return response


def handle_route(method, path):
    if method == "GET" and path == "/":
        return "HTTP/1.1 200 OK", {
            "message": "Welcome to your ML-focused HTTP server"
        }

    elif method == "GET" and path == "/health":
        return "HTTP/1.1 200 OK", {
            "status": "ok"
        }

    else:
        return "HTTP/1.1 404 Not Found", {
            "error": "Not Found"
        }


server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(5)

print(f"Server listening on http://{HOST}:{PORT}")


while True:
    client_socket, client_address = server_socket.accept()
    print(f"Accepted connection from {client_address}")

    request_data = client_socket.recv(1024)
    request_text = request_data.decode(errors="ignore")

    print("---- Raw Request ----")
    print(request_text)
    print("---------------------")

    request_lines = request_text.split("\r\n")
    request_line = request_lines[0]
    parts = request_line.split()

    if len(parts) == 3:
        method, path, version = parts
        print(f"Method:  {method}")
        print(f"Path:    {path}")
        print(f"Version: {version}")

        status_line, response_body_dict = handle_route(method, path)
    else:
        status_line = "HTTP/1.1 400 Bad Request"
        response_body_dict = {"error": "Bad Request"}

    response = json_response(status_line, response_body_dict)

    client_socket.sendall(response.encode())
    client_socket.close()