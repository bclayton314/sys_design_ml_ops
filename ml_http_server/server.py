import socket
import json
import threading

HOST = "127.0.0.1"
PORT = 8080


def predict(features):
    """
    Very simple hardcoded linear model:
    prediction = 1.5 + 2.0*x1 + 1.0*x2
    """
    if len(features) != 2:
        raise ValueError("Expected exactly 2 features")

    x1, x2 = features
    return 1.5 + 2.0 * x1 + 1.0 * x2


def json_response(status_line, body_dict):
    response_body = json.dumps(body_dict)
    response_body_bytes = response_body.encode("utf-8")

    response_headers = (
        f"{status_line}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(response_body_bytes)}\r\n"
        "\r\n"
    )

    return response_headers.encode("utf-8") + response_body_bytes


def parse_http_request(request_text):
    parts = request_text.split("\r\n\r\n", 1)

    header_text = parts[0]
    body_text = parts[1] if len(parts) > 1 else ""

    header_lines = header_text.split("\r\n")
    request_line = header_lines[0]
    request_line_parts = request_line.split()

    if len(request_line_parts) != 3:
        raise ValueError("Malformed request line")

    method, path, version = request_line_parts
    return method, path, version, body_text


def handle_route(method, path, body_text):
    if method == "GET" and path == "/":
        return "HTTP/1.1 200 OK", {
            "message": "Welcome to your ML-focused HTTP server"
        }

    elif method == "GET" and path == "/health":
        return "HTTP/1.1 200 OK", {
            "status": "ok"
        }

    elif method == "POST" and path == "/predict":
        try:
            body_data = json.loads(body_text)

            if "features" not in body_data:
                return "HTTP/1.1 400 Bad Request", {
                    "error": "Missing 'features' field"
                }

            features = body_data["features"]

            if not isinstance(features, list):
                return "HTTP/1.1 400 Bad Request", {
                    "error": "'features' must be a list"
                }

            prediction = predict(features)

            return "HTTP/1.1 200 OK", {
                "prediction": prediction
            }

        except json.JSONDecodeError:
            return "HTTP/1.1 400 Bad Request", {
                "error": "Invalid JSON body"
            }
        except ValueError as e:
            return "HTTP/1.1 400 Bad Request", {
                "error": str(e)
            }

    else:
        return "HTTP/1.1 404 Not Found", {
            "error": "Not Found"
        }


def handle_client(client_socket, client_address):
    print(f"Accepted connection from {client_address}")

    try:
        request_data = client_socket.recv(4096)
        request_text = request_data.decode("utf-8", errors="ignore")

        print("---- Raw Request ----")
        print(request_text)
        print("---------------------")

        method, path, version, body_text = parse_http_request(request_text)

        print(f"Method:  {method}")
        print(f"Path:    {path}")
        print(f"Version: {version}")
        print(f"Body:    {body_text}")

        status_line, response_body_dict = handle_route(method, path, body_text)

    except ValueError as e:
        status_line = "HTTP/1.1 400 Bad Request"
        response_body_dict = {"error": str(e)}
    except Exception as e:
        print(f"Unexpected server error: {e}")
        status_line = "HTTP/1.1 500 Internal Server Error"
        response_body_dict = {"error": "Internal Server Error"}

    response = json_response(status_line, response_body_dict)
    client_socket.sendall(response)
    client_socket.close()


server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(5)

print(f"Server listening on http://{HOST}:{PORT}")

while True:
    client_socket, client_address = server_socket.accept()

    client_thread = threading.Thread(
        target=handle_client,
        args=(client_socket, client_address)
    )
    client_thread.start()