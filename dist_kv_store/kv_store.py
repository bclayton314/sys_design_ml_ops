import json
from pathlib import Path
from typing import Optional


class KeyValueStore:
    def __init__(self, wal_path: Path, snapshot_path: Path) -> None:
        self.store: dict[str, str] = {}
        self.wal_path = wal_path
        self.snapshot_path = snapshot_path

        self.wal_path.parent.mkdir(parents=True, exist_ok=True)
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
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
            raise ValueError(
                "Snapshot field 'last_wal_line' must be a non-negative integer."
            )

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

    def load_snapshot_contents(self) -> Optional[dict]:
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
        record = {"op": "PUT", "key": key, "value": value}
        self.append_to_wal(record)
        self.store[key] = value

    def get(self, key: str) -> Optional[str]:
        return self.store.get(key)

    def delete(self, key: str) -> bool:
        if key in self.store:
            record = {"op": "DELETE", "key": key}
            self.append_to_wal(record)
            del self.store[key]
            return True
        return False

    def show_all(self) -> dict[str, str]:
        return dict(self.store)