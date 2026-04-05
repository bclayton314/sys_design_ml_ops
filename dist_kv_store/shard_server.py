import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse

from kv_store import KeyValueStore


def make_handler(kv_store: KeyValueStore, shard_id: int):
    class KVRequestHandler(BaseHTTPRequestHandler):
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

            if parsed.path == "/health":
                self._send_json(200, {
                    "status": "ok",
                    "shard_id": shard_id,
                })
                return

            if parsed.path == "/kv":
                self._send_json(200, {
                    "shard_id": shard_id,
                    "store": kv_store.show_all(),
                })
                return

            if parsed.path == "/snapshot":
                snapshot = kv_store.load_snapshot_contents()
                if snapshot is None:
                    self._send_json(404, {
                        "error": "No snapshot found",
                        "shard_id": shard_id,
                    })
                else:
                    self._send_json(200, {
                        "snapshot": snapshot,
                        "shard_id": shard_id,
                    })
                return

            key = self._extract_key_from_path()
            if key is not None:
                value = kv_store.get(key)
                if value is None:
                    self._send_json(404, {
                        "error": f"Key '{key}' not found",
                        "shard_id": shard_id,
                    })
                else:
                    self._send_json(200, {
                        "key": key,
                        "value": value,
                        "shard_id": shard_id,
                    })
                return

            self._send_json(404, {
                "error": "Route not found",
                "shard_id": shard_id,
            })

        def do_PUT(self) -> None:
            key = self._extract_key_from_path()
            if key is None:
                self._send_json(404, {
                    "error": "Route not found",
                    "shard_id": shard_id,
                })
                return

            try:
                body = self._read_json_body()
            except ValueError as e:
                self._send_json(400, {
                    "error": str(e),
                    "shard_id": shard_id,
                })
                return

            if "value" not in body:
                self._send_json(400, {
                    "error": "Missing 'value' in request body",
                    "shard_id": shard_id,
                })
                return

            value = body["value"]
            if not isinstance(value, str):
                self._send_json(400, {
                    "error": "'value' must be a string",
                    "shard_id": shard_id,
                })
                return

            kv_store.put(key, value)
            self._send_json(200, {
                "message": "stored",
                "key": key,
                "value": value,
                "shard_id": shard_id,
            })

        def do_DELETE(self) -> None:
            key = self._extract_key_from_path()
            if key is None:
                self._send_json(404, {
                    "error": "Route not found",
                    "shard_id": shard_id,
                })
                return

            deleted = kv_store.delete(key)
            if deleted:
                self._send_json(200, {
                    "message": "deleted",
                    "key": key,
                    "shard_id": shard_id,
                })
            else:
                self._send_json(404, {
                    "error": f"Key '{key}' not found",
                    "shard_id": shard_id,
                })

        def do_POST(self) -> None:
            parsed = urlparse(self.path)

            if parsed.path == "/snapshot":
                kv_store.create_snapshot()
                self._send_json(200, {
                    "message": "snapshot created",
                    "shard_id": shard_id,
                })
                return

            self._send_json(404, {
                "error": "Route not found",
                "shard_id": shard_id,
            })

        def log_message(self, format: str, *args) -> None:
            print(f"[shard {shard_id}] {self.command} {self.path} - {format % args}")

    return KVRequestHandler


def run_server(shard_id: int, port: int) -> None:
    data_dir = Path("data") / f"shard_{shard_id}"
    wal_path = data_dir / "kv_store.wal"
    snapshot_path = data_dir / "kv_store.snapshot.json"

    kv_store = KeyValueStore(wal_path, snapshot_path)
    handler = make_handler(kv_store, shard_id)
    server = HTTPServer(("127.0.0.1", port), handler)

    print(f"Shard {shard_id} running at http://127.0.0.1:{port}")
    print(f"  data dir: {data_dir}")
    server.serve_forever()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("Usage: python shard_server.py <shard_id> <port>")

    shard_id = int(sys.argv[1])
    port = int(sys.argv[2])
    run_server(shard_id, port)