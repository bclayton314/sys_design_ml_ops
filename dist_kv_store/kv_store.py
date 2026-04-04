import json
from pathlib import Path
import os


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WAL_PATH = Path(SCRIPT_DIR) / "kv_store.wal"


class KeyValueStore:
    def __init__(self, wal_path: Path) -> None:
        """
        Initialize an in-memory key-value store and recover state from WAL.
        """
        self.store: dict[str, str] = {}
        self.wal_path = wal_path

        # Ensure the WAL file exists.
        self.wal_path.touch(exist_ok=True)

        # Recover in-memory state from WAL.
        self.replay_wal()

    def append_to_wal(self, record: dict) -> None:
        """
        Append one operation record to the write-ahead log.
        """
        with self.wal_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def replay_wal(self) -> None:
        """
        Rebuild in-memory state by replaying WAL records in order.
        """
        with self.wal_path.open("r", encoding="utf-8") as f:
            for line_number, line in enumerate(f, start=1):
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

    def put(self, key: str, value: str) -> None:
        """
        Log the PUT operation, then apply it to memory.
        """
        record = {
            "op": "PUT",
            "key": key,
            "value": value,
        }
        self.append_to_wal(record)
        self.store[key] = value

    def get(self, key: str) -> str | None:
        """
        Return the value for a key, or None if the key does not exist.
        """
        return self.store.get(key)

    def delete(self, key: str) -> bool:
        """
        Log the DELETE operation if the key exists,
        then remove it from memory.
        Returns True if deleted, False otherwise.
        """
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
        """
        Return a shallow copy of all key-value pairs.
        """
        return dict(self.store)


def print_help() -> None:
    print("\nAvailable commands:")
    print("  PUT <key> <value>")
    print("  GET <key>")
    print("  DELETE <key>")
    print("  SHOW")
    print("  HELP")
    print("  EXIT\n")


def run_repl() -> None:
    kv = KeyValueStore(WAL_PATH)

    print("Simple Redis-lite KV Store with WAL Recovery")
    print(f"WAL file: {WAL_PATH}")
    print("Type HELP for available commands.")

    while True:
        try:
            raw_command = input("kv> ").strip()
        except EOFError:
            print("\nExiting.")
            break
        except KeyboardInterrupt:
            print("\nExiting.")
            break

        if not raw_command:
            continue

        parts = raw_command.split(maxsplit=2)
        command = parts[0].upper()

        if command == "PUT":
            if len(parts) < 3:
                print("Error: PUT requires a key and a value.")
                continue

            key = parts[1]
            value = parts[2]
            kv.put(key, value)
            print(f"OK: stored key '{key}'")

        elif command == "GET":
            if len(parts) != 2:
                print("Error: GET requires exactly one key.")
                continue

            key = parts[1]
            value = kv.get(key)

            if value is None:
                print(f"NOT FOUND: '{key}'")
            else:
                print(value)

        elif command == "DELETE":
            if len(parts) != 2:
                print("Error: DELETE requires exactly one key.")
                continue

            key = parts[1]
            deleted = kv.delete(key)

            if deleted:
                print(f"OK: deleted key '{key}'")
            else:
                print(f"NOT FOUND: '{key}'")

        elif command == "SHOW":
            if len(parts) != 1:
                print("Error: SHOW does not take arguments.")
                continue

            current = kv.show_all()
            if not current:
                print("{}")
            else:
                print(current)

        elif command == "HELP":
            print_help()

        elif command == "EXIT":
            print("Exiting.")
            break

        else:
            print(f"Error: unknown command '{command}'. Type HELP for help.")


if __name__ == "__main__":
    run_repl()