import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
import os


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = Path(SCRIPT_DIR) / "logs"
OFFSET_DIR = Path(SCRIPT_DIR) / "offsets"

NUM_PARTITIONS = 3


# ------------------------
# Setup
# ------------------------
def ensure_dirs_exist() -> None:
    LOG_DIR.mkdir(exist_ok=True)
    OFFSET_DIR.mkdir(exist_ok=True)


def reset_demo_state() -> None:
    for path in LOG_DIR.glob("*.log"):
        path.unlink(missing_ok=True)

    for path in OFFSET_DIR.glob("*.json"):
        path.unlink(missing_ok=True)


# ------------------------
# Partitioning
# ------------------------
def get_partition(topic: str, key: str) -> int:
    """
    Deterministically assign a key to a partition.
    """
    key_bytes = key.encode("utf-8")
    hash_val = int(hashlib.md5(key_bytes).hexdigest(), 16)
    return hash_val % NUM_PARTITIONS


def get_log_path(topic: str, partition: int) -> Path:
    return LOG_DIR / f"{topic}_{partition}.log"


# ------------------------
# Event writing
# ------------------------
def append_event(topic: str, key: str, event: dict) -> int:
    partition = get_partition(topic, key)
    log_path = get_log_path(topic, partition)

    record = {
        **event,
        "_partition": partition,
        "_key": key,
    }

    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

    return partition


# ------------------------
# Event reading
# ------------------------
def read_partition(topic: str, partition: int) -> list[dict]:
    log_path = get_log_path(topic, partition)

    if not log_path.exists():
        return []

    events = []

    with log_path.open("r", encoding="utf-8") as f:
        for offset, line in enumerate(f):
            line = line.strip()
            if not line:
                continue

            try:
                event = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"{topic} partition {partition}: invalid JSON at line {offset + 1}: {e}"
                ) from e

            event["_offset"] = offset
            events.append(event)

    return events


# ------------------------
# Offset handling
# ------------------------
def get_offset_path(consumer: str) -> Path:
    return OFFSET_DIR / f"{consumer}.json"


def load_offsets(consumer: str) -> dict:
    path = get_offset_path(consumer)

    if not path.exists():
        return {}

    raw_text = path.read_text(encoding="utf-8").strip()
    if not raw_text:
        return {}

    try:
        offsets = json.loads(raw_text)
    except json.JSONDecodeError as e:
        raise ValueError(
            f"Offset file for consumer '{consumer}' is not valid JSON: {path}"
        ) from e

    if not isinstance(offsets, dict):
        raise ValueError(
            f"Offset file for consumer '{consumer}' must contain a JSON object"
        )

    return offsets


def save_offsets(consumer: str, offsets: dict) -> None:
    path = get_offset_path(consumer)
    path.write_text(json.dumps(offsets, indent=2), encoding="utf-8")


def get_offset(consumer: str, topic: str, partition: int) -> int:
    offsets = load_offsets(consumer)
    return offsets.get(topic, {}).get(str(partition), 0)


def set_offset(consumer: str, topic: str, partition: int, offset: int) -> None:
    offsets = load_offsets(consumer)

    if topic not in offsets:
        offsets[topic] = {}

    offsets[topic][str(partition)] = offset
    save_offsets(consumer, offsets)


# ------------------------
# Batch consumption
# ------------------------
def consume_batch(consumer: str, topic: str, batch_size: int) -> list[dict]:
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    results = []

    for partition in range(NUM_PARTITIONS):
        start_offset = get_offset(consumer, topic, partition)
        events = read_partition(topic, partition)

        batch = [e for e in events if e["_offset"] >= start_offset][:batch_size]

        if batch:
            next_offset = batch[-1]["_offset"] + 1
            set_offset(consumer, topic, partition, next_offset)

        results.extend(batch)

    return results


# ------------------------
# Helpers
# ------------------------
def build_event(event_type: str, payload: dict) -> dict:
    return {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


def describe_consumer(consumer: str, topic: str) -> None:
    offsets = load_offsets(consumer)

    print(f"\nConsumer: {consumer}")
    for partition in range(NUM_PARTITIONS):
        current = offsets.get(topic, {}).get(str(partition), 0)
        total = len(read_partition(topic, partition))
        lag = total - current

        print(
            f"  partition={partition} "
            f"offset={current} "
            f"total={total} "
            f"lag={lag}"
        )


# ------------------------
# Demo
# ------------------------
def main() -> None:
    ensure_dirs_exist()
    reset_demo_state()

    topic = "orders"
    consumer = "analytics"

    orders = [
        {"order_id": 1, "user_id": "A", "amount": 10},
        {"order_id": 2, "user_id": "B", "amount": 20},
        {"order_id": 3, "user_id": "C", "amount": 30},
        {"order_id": 4, "user_id": "A", "amount": 40},
        {"order_id": 5, "user_id": "B", "amount": 50},
        {"order_id": 6, "user_id": "C", "amount": 60},
    ]

    print("\nProducing events:")
    for order in orders:
        partition = append_event(
            topic=topic,
            key=order["user_id"],
            event=build_event("order_created", order),
        )
        print(f"order_id={order['order_id']} -> partition {partition}")

    describe_consumer(consumer, topic)

    print("\nFirst batch consume:")
    batch1 = consume_batch(consumer, topic, batch_size=2)
    for event in batch1:
        print(event)

    describe_consumer(consumer, topic)

    print("\nSecond batch consume:")
    batch2 = consume_batch(consumer, topic, batch_size=2)
    for event in batch2:
        print(event)

    describe_consumer(consumer, topic)


if __name__ == "__main__":
    main()