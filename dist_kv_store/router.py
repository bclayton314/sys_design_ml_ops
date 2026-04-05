import json
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse

from hash_ring import ConsistentHashRing
from metrics import metrics, Timer


HOST = "127.0.0.1"
PORT = 8090

NODES = [
    "http://127.0.0.1:8080",
    "http://127.0.0.1:8081",
    "http://127.0.0.1:8082",
]

hash_ring = ConsistentHashRing(NODES, virtual_nodes=5)

# Quorum settings
N_REPLICAS = 3
WRITE_QUORUM = 2
READ_QUORUM = 2


def forward_request(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    data = None
    headers = {}

    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url=url, data=data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=2) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return resp.status, payload

    except urllib.error.HTTPError as e:
        try:
            payload = json.loads(e.read().decode("utf-8"))
        except Exception:
            payload = {"error": f"HTTP error {e.code}"}
        return e.code, payload

    except urllib.error.URLError as e:
        return 503, {
            "error": f"Unable to reach target node: {e.reason}",
            "unavailable": True,
        }

    except TimeoutError:
        return 503, {
            "error": "Target node request timed out",
            "unavailable": True,
        }


def check_node_health(node_url: str) -> dict:
    try:
        req = urllib.request.Request(f"{node_url}/health", method="GET")
        with urllib.request.urlopen(req, timeout=1) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            return {
                "node": node_url,
                "reachable": True,
                "status_code": resp.status,
                "payload": payload,
            }
    except Exception as e:
        return {
            "node": node_url,
            "reachable": False,
            "error": str(e),
        }


def repair_replicas(key: str, correct_value: str, stale_nodes: list[str]) -> dict:
    """
    Send PUT requests to stale replicas to repair them.
    """
    metrics.inc("read_repair_attempts")

    repaired = 0

    for node in stale_nodes:
        status, _ = forward_request(
            "PUT",
            f"{node}/kv/{key}",
            {"value": correct_value},
        )

        if 200 <= status < 300:
            repaired += 1

    if repaired == len(stale_nodes):
        metrics.inc("read_repair_success")
    else:
        metrics.inc("read_repair_failures")

    return {
        "attempted": len(stale_nodes),
        "repaired": repaired,
        "stale_nodes": stale_nodes,
        "correct_value": correct_value,
    }


def quorum_put(key: str, value: str) -> tuple[int, dict]:
    metrics.inc("put_requests")

    with Timer("put_latency_ms"):
        replicas = hash_ring.get_nodes(key, N_REPLICAS)

        successes = []
        failures = []

        for node in replicas:
            status, payload = forward_request(
                "PUT",
                f"{node}/kv/{key}",
                {"value": value},
            )

            result = {
                "node": node,
                "status": status,
                "payload": payload,
            }

            if 200 <= status < 300:
                successes.append(result)
            else:
                failures.append(result)

        if len(successes) >= WRITE_QUORUM:
            metrics.inc("put_quorum_success")
            return 200, {
                "message": "write quorum satisfied",
                "key": key,
                "value": value,
                "replicas": replicas,
                "write_quorum": WRITE_QUORUM,
                "acks": len(successes),
                "successes": successes,
                "failures": failures,
            }

        metrics.inc("put_quorum_failure")
        return 503, {
            "error": "write quorum not satisfied",
            "key": key,
            "value": value,
            "replicas": replicas,
            "write_quorum": WRITE_QUORUM,
            "acks": len(successes),
            "successes": successes,
            "failures": failures,
        }


def quorum_delete(key: str) -> tuple[int, dict]:
    metrics.inc("delete_requests")

    with Timer("delete_latency_ms"):
        replicas = hash_ring.get_nodes(key, N_REPLICAS)

        successes = []
        failures = []

        for node in replicas:
            status, payload = forward_request("DELETE", f"{node}/kv/{key}")

            result = {
                "node": node,
                "status": status,
                "payload": payload,
            }

            if 200 <= status < 300:
                successes.append(result)
            else:
                failures.append(result)

        if len(successes) >= WRITE_QUORUM:
            metrics.inc("delete_quorum_success")
            return 200, {
                "message": "delete quorum satisfied",
                "key": key,
                "replicas": replicas,
                "write_quorum": WRITE_QUORUM,
                "acks": len(successes),
                "successes": successes,
                "failures": failures,
            }

        metrics.inc("delete_quorum_failure")
        return 503, {
            "error": "delete quorum not satisfied",
            "key": key,
            "replicas": replicas,
            "write_quorum": WRITE_QUORUM,
            "acks": len(successes),
            "successes": successes,
            "failures": failures,
        }


def quorum_get(key: str) -> tuple[int, dict]:
    metrics.inc("get_requests")

    with Timer("get_latency_ms"):
        replicas = hash_ring.get_nodes(key, N_REPLICAS)

        responses = []
        failures = []

        for node in replicas:
            status, payload = forward_request("GET", f"{node}/kv/{key}")

            result = {
                "node": node,
                "status": status,
                "payload": payload,
            }

            if 200 <= status < 300:
                responses.append(result)
            else:
                failures.append(result)

        if len(responses) < READ_QUORUM:
            metrics.inc("get_quorum_failure")
            return 503, {
                "error": "read quorum not satisfied",
                "key": key,
                "replicas": replicas,
                "read_quorum": READ_QUORUM,
                "responses": len(responses),
                "successes": responses,
                "failures": failures,
            }

        metrics.inc("get_quorum_success")

        value_counts: dict[str, int] = {}
        node_values: dict[str, str] = {}

        for result in responses:
            payload = result["payload"]
            value = payload.get("value")
            if value is None:
                continue

            value_counts[value] = value_counts.get(value, 0) + 1
            node_values[result["node"]] = value

        if not value_counts:
            return 404, {
                "error": f"Key '{key}' not found on quorum read",
                "key": key,
                "replicas": replicas,
                "read_quorum": READ_QUORUM,
                "successes": responses,
                "failures": failures,
            }

        # Majority value wins
        correct_value = max(value_counts, key=value_counts.get)

        stale_nodes = [
            node for node, val in node_values.items()
            if val != correct_value
        ]

        repair_result = None
        if stale_nodes:
            repair_result = repair_replicas(key, correct_value, stale_nodes)

        return 200, {
            "message": "read quorum satisfied",
            "key": key,
            "value": correct_value,
            "replicas": replicas,
            "read_quorum": READ_QUORUM,
            "responses": len(responses),
            "value_votes": value_counts,
            "successes": responses,
            "failures": failures,
            "read_repair": repair_result,
        }


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

        if parsed.path == "/cluster/health":
            health = [check_node_health(node) for node in NODES]
            self._send_json(200, {"cluster_health": health})
            return

        if parsed.path == "/quorum/config":
            self._send_json(200, {
                "nodes": NODES,
                "N_REPLICAS": N_REPLICAS,
                "WRITE_QUORUM": WRITE_QUORUM,
                "READ_QUORUM": READ_QUORUM,
            })
            return

        if parsed.path == "/metrics":
            self._send_json(200, metrics.summary())
            return

        key = self._extract_key_from_path()
        if key is None:
            self._send_json(404, {"error": "Route not found"})
            return

        status, payload = quorum_get(key)
        payload["routed_by"] = "quorum_router"
        payload["key"] = key
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

        if "value" not in body:
            self._send_json(400, {"error": "Missing 'value' in request body"})
            return

        value = body["value"]
        if not isinstance(value, str):
            self._send_json(400, {"error": "'value' must be a string"})
            return

        status, payload = quorum_put(key, value)
        payload["routed_by"] = "quorum_router"
        payload["key"] = key
        self._send_json(status, payload)

    def do_DELETE(self) -> None:
        key = self._extract_key_from_path()
        if key is None:
            self._send_json(404, {"error": "Route not found"})
            return

        status, payload = quorum_delete(key)
        payload["routed_by"] = "quorum_router"
        payload["key"] = key
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
    print("  GET    /cluster/health")
    print("  GET    /quorum/config")
    print("  GET    /metrics")
    server.serve_forever()


if __name__ == "__main__":
    run_router()