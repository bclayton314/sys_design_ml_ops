import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import urlparse
import os


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WAL_PATH = Path(SCRIPT_DIR) / "kv_store.wal"
SNAPSHOT_PATH = Path(SCRIPT_DIR) / "kv_store.snapshot.json"

HOST = "127.0.0.1"
PORT = 8080


class KeyValueStore:
    def __init__(self, wal_path: Path, snapshot_path: Path) -> None:
        """
        Initialize the store and recover state from snapshot + WAL tail.
        """
        self.store: dict[str, str] = {}
        self.wal_path = wal_path
        self.snapshot_path = snapshot_path

        self.wal_path.touch(exist_ok=True)
        self.recover()

    def append_to_wal(self, record: dict) -> None:
        with self.wal_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def count_wal_lines(self) -> int:
        count = 0
        with self.wal_path.open("r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    count += 1
        return count

    def load_snapshot(self) -> tuple[dict[str, str], int]:
        if not self.snapshot_path.exists():
            return {}, 0

        raw_text = self.snapshot_path.read_text(encoding="utf-8").strip()
        if not raw_text:
            return {}, 0

        try:
            snapshot_data = json.loads(raw_text)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Invalid JSON in snapshot file {self.snapshot_path}: {e}"
            ) from e

        if not isinstance(snapshot_data, dict):
            raise ValueError(
                f"Snapshot file must contain a JSON object: {self.snapshot_path}"
            )

        store_data = snapshot_data.get("store", {})
        last_wal_line = snapshot_data.get("last_wal_line", 0)

        if not isinstance(store_data, dict):
            raise ValueError("Snapshot field 'store' must be a JSON object.")

        if not isinstance(last_wal_line, int) or last_wal_line < 0:
            raise ValueError("Snapshot field 'last_wal_line' must be a non-negative integer.")

        return store_data, last_wal_line

    def replay_wal_from_line(self, start_line: int) -> None:
        with self.wal_path.open("r", encoding="utf-8") as f:
            for line_number, line in enumerate(f, start=1):
                if line_number <= start_line:
                    continue

                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError as e:
                    raise ValueError(
                        f"Invalid JSON in WAL at line {line_number}: {e}"
                    ) from e

                op = record.get("op")
                key = record.get("key")

                if op == "PUT":
                    value = record.get("value")
                    if key is None or value is None:
                        raise ValueError(
                            f"Invalid PUT record in WAL at line {line_number}: {record}"
                        )
                    self.store[key] = value

                elif op == "DELETE":
                    if key is None:
                        raise ValueError(
                            f"Invalid DELETE record in WAL at line {line_number}: {record}"
                        )
                    self.store.pop(key, None)

                else:
                    raise ValueError(
                        f"Unknown WAL operation at line {line_number}: {record}"
                    )

    def recover(self) -> None:
        snapshot_store, last_wal_line = self.load_snapshot()
        self.store = dict(snapshot_store)
        self.replay_wal_from_line(last_wal_line)

    def create_snapshot(self) -> None:
        snapshot_record = {
            "store": dict(self.store),
            "last_wal_line": self.count_wal_lines(),
        }

        with self.snapshot_path.open("w", encoding="utf-8") as f:
            json.dump(snapshot_record, f, indent=2)

    def load_snapshot_contents(self) -> dict | None:
        if not self.snapshot_path.exists():
            return None

        raw_text = self.snapshot_path.read_text(encoding="utf-8").strip()
        if not raw_text:
            return None

        try:
            snapshot_data = json.loads(raw_text)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Invalid JSON in snapshot file {self.snapshot_path}: {e}"
            ) from e

        if not isinstance(snapshot_data, dict):
            raise ValueError(
                f"Snapshot file must contain a JSON object: {self.snapshot_path}"
            )

        return snapshot_data

    def put(self, key: str, value: str) -> None:
        record = {
            "op": "PUT",
            "key": key,
            "value": value,
        }
        self.append_to_wal(record)
        self.store[key] = value

    def get(self, key: str) -> str | None:
        return self.store.get(key)

    def delete(self, key: str) -> bool:
        if key in self.store:
            record = {
                "op": "DELETE",
                "key": key,
            }
            self.append_to_wal(record)
            del self.store[key]
            return True
        return False

    def show_all(self) -> dict[str, str]:
        return dict(self.store)


kv_store = KeyValueStore(WAL_PATH, SNAPSHOT_PATH)


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

        if parsed.path == "/kv":
            self._send_json(200, {"store": kv_store.show_all()})
            return

        if parsed.path == "/snapshot":
            snapshot = kv_store.load_snapshot_contents()
            if snapshot is None:
                self._send_json(404, {"error": "No snapshot found"})
            else:
                self._send_json(200, {"snapshot": snapshot})
            return

        key = self._extract_key_from_path()
        if key is not None:
            value = kv_store.get(key)
            if value is None:
                self._send_json(404, {"error": f"Key '{key}' not found"})
            else:
                self._send_json(200, {"key": key, "value": value})
            return

        self._send_json(404, {"error": "Route not found"})

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

        kv_store.put(key, value)
        self._send_json(200, {"message": "stored", "key": key, "value": value})

    def do_DELETE(self) -> None:
        key = self._extract_key_from_path()
        if key is None:
            self._send_json(404, {"error": "Route not found"})
            return

        deleted = kv_store.delete(key)
        if deleted:
            self._send_json(200, {"message": "deleted", "key": key})
        else:
            self._send_json(404, {"error": f"Key '{key}' not found"})

    def do_POST(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path == "/snapshot":
            kv_store.create_snapshot()
            self._send_json(200, {"message": "snapshot created"})
            return

        self._send_json(404, {"error": "Route not found"})

    def log_message(self, format: str, *args) -> None:
        """
        Keep the default server logging minimal and readable.
        """
        print(f"{self.command} {self.path} - {format % args}")


def run_server() -> None:
    server = HTTPServer((HOST, PORT), KVRequestHandler)
    print(f"KV store HTTP server running at http://{HOST}:{PORT}")
    print("Routes:")
    print("  GET    /kv")
    print("  GET    /kv/<key>")
    print("  PUT    /kv/<key>")
    print("  DELETE /kv/<key>")
    print("  POST   /snapshot")
    print("  GET    /snapshot")
    server.serve_forever()


if __name__ == "__main__":
    run_server()