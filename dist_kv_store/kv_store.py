class KeyValueStore:
    def __init__(self) -> None:
        """
        Initialize an empty in-memory key-value store.
        """
        self.store: dict[str, str] = {}

    def put(self, key: str, value: str) -> None:
        """
        Insert a new key or overwrite an existing key.
        """
        self.store[key] = value

    def get(self, key: str) -> str | None:
        """
        Return the value for a key, or None if the key does not exist.
        """
        return self.store.get(key)

    def delete(self, key: str) -> bool:
        """
        Delete a key if it exists.
        Returns True if deleted, False if the key was not present.
        """
        if key in self.store:
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
    kv = KeyValueStore()

    print("Simple Redis-lite KV Store")
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