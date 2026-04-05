import json
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

from hash_ring import ConsistentHashRing


HOST = "127.0.0.1"
PORT = 8090

NODES = [
    "http://127.0.0.1:8080",
    "http://127.0.0.1:8081",
    "http://127.0.0.1:8082",
]

hash_ring = ConsistentHashRing(NODES, virtual_nodes=5)


def forward_request(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    data = None
    headers = {}

    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url=url, data=data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return resp.status, payload
    except urllib.error.HTTPError as e:
        payload = json.loads(e.read().decode("utf-8"))
        return e.code, payload
    except urllib.error.URLError as e:
        return 503, {"error": f"Unable to reach target node: {e.reason}"}


class RouterHandler(BaseHTTPRequestHandler):
    def _send_json(self, status_code: int, payload: dict) -> None:
        response_body = json.dumps(payload).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body)

    def _read_json_body(self) -> dict:
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length).decode("utf-8")

        if not body:
            return {}

        try:
            data = json.loads(body)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON body: {e}") from e

        if not isinstance(data, dict):
            raise ValueError("JSON body must be an object.")

        return data

    def _extract_key_from_path(self) -> str | None:
        parsed = urlparse(self.path)
        path_parts = parsed.path.strip("/").split("/")

        if len(path_parts) == 2 and path_parts[0] == "kv":
            return path_parts[1]

        return None

    def do_GET(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path == "/ring":
            self._send_json(200, {
                "nodes": NODES,
                "virtual_nodes": hash_ring.virtual_nodes,
                "ring": hash_ring.describe_ring(),
            })
            return

        key = self._extract_key_from_path()
        if key is None:
            self._send_json(404, {"error": "Route not found"})
            return

        target_node = hash_ring.get_node(key)
        status, payload = forward_request("GET", f"{target_node}/kv/{key}")

        payload["routed_by"] = "consistent_hash_router"
        payload["target_node"] = target_node
        self._send_json(status, payload)

    def do_PUT(self) -> None:
        key = self._extract_key_from_path()
        if key is None:
            self._send_json(404, {"error": "Route not found"})
            return

        try:
            body = self._read_json_body()
        except ValueError as e:
            self._send_json(400, {"error": str(e)})
            return

        target_node = hash_ring.get_node(key)
        status, payload = forward_request("PUT", f"{target_node}/kv/{key}", body)

        payload["routed_by"] = "consistent_hash_router"
        payload["target_node"] = target_node
        self._send_json(status, payload)

    def do_DELETE(self) -> None:
        key = self._extract_key_from_path()
        if key is None:
            self._send_json(404, {"error": "Route not found"})
            return

        target_node = hash_ring.get_node(key)
        status, payload = forward_request("DELETE", f"{target_node}/kv/{key}")

        payload["routed_by"] = "consistent_hash_router"
        payload["target_node"] = target_node
        self._send_json(status, payload)

    def log_message(self, format: str, *args) -> None:
        print(f"[router] {self.command} {self.path} - {format % args}")


def run_router() -> None:
    server = HTTPServer((HOST, PORT), RouterHandler)
    print(f"Router running at http://{HOST}:{PORT}")
    print("Nodes:")
    for node in NODES:
        print(f"  - {node}")
    print("Routes:")
    print("  GET    /kv/<key>")
    print("  PUT    /kv/<key>")
    print("  DELETE /kv/<key>")
    print("  GET    /ring")
    server.serve_forever()


if __name__ == "__main__":
    run_router()