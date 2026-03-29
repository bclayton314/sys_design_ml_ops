import json
from datetime import datetime, timezone
from pathlib import Path
import os


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = Path(SCRIPT_DIR) / "logs"
OFFSET_DIR = Path(SCRIPT_DIR) / "offsets"


# ------------------------
# Setup
# ------------------------
def ensure_dirs_exist() -> None:
    LOG_DIR.mkdir(exist_ok=True)
    OFFSET_DIR.mkdir(exist_ok=True)


def get_log_path(topic: str) -> Path:
    return LOG_DIR / f"{topic}.log"


def get_offset_path(consumer_name: str) -> Path:
    return OFFSET_DIR / f"{consumer_name}.json"


# ------------------------
# Event writing
# ------------------------
def append_event(topic: str, event: dict) -> None:
    log_path = get_log_path(topic)

    with log_path.open("a", encoding="utf-8") as f:
        json_record = json.dumps(event)
        f.write(json_record + "\n")


# ------------------------
# Event reading
# ------------------------
def read_events(topic: str) -> list[dict]:
    log_path = get_log_path(topic)

    if not log_path.exists():
        return []

    events = []

    with log_path.open("r", encoding="utf-8") as f:
        for line_number, line in enumerate(f, start=0):
            line = line.strip()

            if not line:
                continue

            try:
                event = json.loads(line)
                event["_offset"] = line_number
                event["_topic"] = topic
                events.append(event)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"{topic}: invalid JSON on line {line_number + 1}: {e}"
                ) from e

    return events


def read_events_from_offset(topic: str, start_offset: int) -> list[dict]:
    all_events = read_events(topic)
    return [event for event in all_events if event["_offset"] >= start_offset]


# ------------------------
# Offset storage
# ------------------------
def load_consumer_offsets(consumer_name: str) -> dict:
    offset_path = get_offset_path(consumer_name)

    if not offset_path.exists():
        return {}

    with offset_path.open("r", encoding="utf-8") as f:
        content = f.read().strip()
        if not content:
            return {}
        return json.loads(content)


def save_consumer_offsets(consumer_name: str, offsets: dict) -> None:
    offset_path = get_offset_path(consumer_name)

    with offset_path.open("w", encoding="utf-8") as f:
        json.dump(offsets, f, indent=2)


def get_consumer_offset(consumer_name: str, topic: str) -> int:
    offsets = load_consumer_offsets(consumer_name)
    return offsets.get(topic, 0)


def set_consumer_offset(consumer_name: str, topic: str, offset: int) -> None:
    offsets = load_consumer_offsets(consumer_name)
    offsets[topic] = offset
    save_consumer_offsets(consumer_name, offsets)


# ------------------------
# Consumer API
# ------------------------
def consume_new_events(consumer_name: str, topic: str) -> list[dict]:
    start_offset = get_consumer_offset(consumer_name, topic)
    new_events = read_events_from_offset(topic, start_offset)

    if new_events:
        next_offset = max(event["_offset"] for event in new_events) + 1
        set_consumer_offset(consumer_name, topic, next_offset)

    return new_events


# ------------------------
# Event creation
# ------------------------
def build_event(event_type: str, payload: dict) -> dict:
    return {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


# ------------------------
# Replay logic
# ------------------------
def replay_events(events: list[dict]) -> dict:
    state = {
        "users": {},
        "orders": {},
        "order_count_by_user": {},
        "signup_count": 0,
        "total_order_amount": 0.0,
    }

    for event in events:
        event_type = event["event_type"]
        payload = event["payload"]

        if event_type == "user_signup":
            user_id = payload["user_id"]
            email = payload["email"]

            state["users"][user_id] = {"email": email}
            state["signup_count"] += 1

        elif event_type == "order_created":
            order_id = payload["order_id"]
            user_id = payload["user_id"]
            amount = payload["amount"]

            state["orders"][order_id] = {
                "user_id": user_id,
                "amount": amount,
            }

            state["order_count_by_user"].setdefault(user_id, 0)
            state["order_count_by_user"][user_id] += 1
            state["total_order_amount"] += amount

        else:
            print(f"Skipping unknown event type: {event_type}")

    return state


# ------------------------
# Demo
# ------------------------
def main():
    ensure_dirs_exist()

    append_event(
        "users",
        build_event(
            "user_signup",
            {"user_id": 123, "email": "user@example.com"},
        ),
    )

    append_event(
        "orders",
        build_event(
            "order_created",
            {"order_id": 456, "user_id": 123, "amount": 29.99},
        ),
    )

    append_event(
        "orders",
        build_event(
            "order_created",
            {"order_id": 789, "user_id": 123, "amount": 10.00},
        ),
    )

    consumer_name = "analytics"

    print("First consume from orders:")
    batch_1 = consume_new_events(consumer_name, "orders")
    for event in batch_1:
        print(event)

    print("\nConsumer offsets after first consume:")
    print(load_consumer_offsets(consumer_name))

    print("\nSecond consume from orders (should be empty if nothing new):")
    batch_2 = consume_new_events(consumer_name, "orders")
    print(batch_2)

    print("\nAppend one more order event...")
    append_event(
        "orders",
        build_event(
            "order_created",
            {"order_id": 999, "user_id": 123, "amount": 50.00},
        ),
    )

    print("\nThird consume from orders (should only get the new event):")
    batch_3 = consume_new_events(consumer_name, "orders")
    for event in batch_3:
        print(event)

    print("\nFinal consumer offsets:")
    print(load_consumer_offsets(consumer_name))


if __name__ == "__main__":
    main()