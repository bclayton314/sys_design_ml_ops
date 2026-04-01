import json
from datetime import datetime, timezone
from pathlib import Path


LOG_DIR = Path("logs")
OFFSET_DIR = Path("offsets")


# ------------------------
# Setup
# ------------------------
def ensure_dirs_exist() -> None:
    LOG_DIR.mkdir(exist_ok=True)
    OFFSET_DIR.mkdir(exist_ok=True)


def reset_demo_state() -> None:
    """
    Remove existing demo log and offset files so each run starts clean.
    """
    for path in LOG_DIR.glob("*.log"):
        path.unlink(missing_ok=True)

    for path in OFFSET_DIR.glob("*.json"):
        path.unlink(missing_ok=True)


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
    """
    Load offsets for a consumer. If the file is missing or empty, return {}.
    If the file contains invalid JSON, raise a clearer error.
    """
    offset_path = get_offset_path(consumer_name)

    if not offset_path.exists():
        return {}

    try:
        with offset_path.open("r", encoding="utf-8") as f:
            raw_text = f.read().strip()

        if not raw_text:
            return {}

        offsets = json.loads(raw_text)

        if not isinstance(offsets, dict):
            raise ValueError(
                f"Offset file for consumer '{consumer_name}' must contain a JSON object"
            )

        return offsets

    except json.JSONDecodeError as e:
        raise ValueError(
            f"Offset file for consumer '{consumer_name}' is not valid JSON: {offset_path}"
        ) from e


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


def describe_consumer(consumer_name: str, topic: str) -> dict:
    current_offset = get_consumer_offset(consumer_name, topic)
    total_events = len(read_events(topic))
    lag = max(total_events - current_offset, 0)

    return {
        "consumer": consumer_name,
        "topic": topic,
        "current_offset": current_offset,
        "total_events": total_events,
        "lag": lag,
    }


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
# Demo helpers
# ------------------------
def print_consumer_status(consumer_name: str, topic: str) -> None:
    status = describe_consumer(consumer_name, topic)
    print(
        f"consumer={status['consumer']} "
        f"topic={status['topic']} "
        f"offset={status['current_offset']} "
        f"total_events={status['total_events']} "
        f"lag={status['lag']}"
    )


def print_batch(title: str, events: list[dict]) -> None:
    print(title)
    if not events:
        print("  []")
        return

    for event in events:
        print(f"  {event}")


# ------------------------
# Demo
# ------------------------
def main():
    ensure_dirs_exist()
    reset_demo_state()

    # Seed the orders topic
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

    append_event(
        "orders",
        build_event(
            "order_created",
            {"order_id": 999, "user_id": 456, "amount": 50.00},
        ),
    )

    consumers = ["analytics", "email_service", "ml_features"]
    topic = "orders"

    print("\nInitial consumer status:")
    for consumer in consumers:
        print_consumer_status(consumer, topic)

    # analytics consumes everything
    analytics_batch = consume_new_events("analytics", topic)
    print_batch("\nanalytics consumes:", analytics_batch)

    print("\nStatus after analytics consumes:")
    for consumer in consumers:
        print_consumer_status(consumer, topic)

    # email_service consumes everything
    email_batch = consume_new_events("email_service", topic)
    print_batch("\nemail_service consumes:", email_batch)

    print("\nStatus after email_service consumes:")
    for consumer in consumers:
        print_consumer_status(consumer, topic)

    # append another event
    append_event(
        "orders",
        build_event(
            "order_created",
            {"order_id": 1111, "user_id": 123, "amount": 75.00},
        ),
    )

    print("\nStatus after appending one more order:")
    for consumer in consumers:
        print_consumer_status(consumer, topic)

    # analytics consumes only the new event
    analytics_batch_2 = consume_new_events("analytics", topic)
    print_batch("\nanalytics consumes again:", analytics_batch_2)

    # ml_features consumes for the first time, so it sees everything
    ml_batch = consume_new_events("ml_features", topic)
    print_batch("\nml_features consumes for the first time:", ml_batch)

    print("\nFinal consumer status:")
    for consumer in consumers:
        print_consumer_status(consumer, topic)

    print("\nFinal offset files:")
    for consumer in consumers:
        print(f"{consumer}: {load_consumer_offsets(consumer)}")


if __name__ == "__main__":
    main()