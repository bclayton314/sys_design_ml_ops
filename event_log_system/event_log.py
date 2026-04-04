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
    key_bytes = key.encode("utf-8")
    hash_val = int(hashlib.md5(key_bytes).hexdigest(), 16)
    return hash_val % NUM_PARTITIONS


def get_leader_log_path(topic: str, partition: int) -> Path:
    return LOG_DIR / f"{topic}_{partition}_leader.log"


def get_replica_log_path(topic: str, partition: int) -> Path:
    return LOG_DIR / f"{topic}_{partition}_replica.log"


# ------------------------
# Log file helpers
# ------------------------
def read_log_file(path: Path, topic: str, partition: int, role: str) -> list[dict]:
    if not path.exists():
        return []

    events = []

    with path.open("r", encoding="utf-8") as f:
        for offset, line in enumerate(f):
            line = line.strip()
            if not line:
                continue

            try:
                event = json.loads(line)
            except json.JSONDecodeError as e:
                raise ValueError(
                    f"{topic} partition {partition} {role}: invalid JSON at line {offset + 1}: {e}"
                ) from e

            event["_offset"] = offset
            event["_read_role"] = role
            events.append(event)

    return events


def append_record_to_path(path: Path, record: dict) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")


# ------------------------
# Event writing
# ------------------------
def append_event(topic: str, key: str, event: dict) -> int:
    """
    Asynchronous replication model:
    write goes to leader only.
    Replica catches up later via replicate_partition().
    """
    partition = get_partition(topic, key)
    leader_path = get_leader_log_path(topic, partition)

    record = {
        **event,
        "_partition": partition,
        "_key": key,
    }

    append_record_to_path(leader_path, record)
    return partition


# ------------------------
# Replication
# ------------------------
def replicate_partition(topic: str, partition: int, max_events: int | None = None) -> int:
    """
    Copy missing events from leader to replica.
    If max_events is provided, only replicate up to that many events.
    Returns the number of events copied.
    """
    leader_events = read_log_file(
        get_leader_log_path(topic, partition), topic, partition, role="leader"
    )
    replica_events = read_log_file(
        get_replica_log_path(topic, partition), topic, partition, role="replica"
    )

    leader_count = len(leader_events)
    replica_count = len(replica_events)

    if replica_count > leader_count:
        raise ValueError(
            f"Replica for topic={topic}, partition={partition} is ahead of leader"
        )

    missing_events = leader_events[replica_count:]

    if max_events is not None:
        if max_events < 0:
            raise ValueError("max_events must be non-negative")
        missing_events = missing_events[:max_events]

    replica_path = get_replica_log_path(topic, partition)

    copied = 0
    for event in missing_events:
        record = {k: v for k, v in event.items() if not k.startswith("_")}
        append_record_to_path(replica_path, record)
        copied += 1

    return copied


def replicate_all_partitions(topic: str, max_events_per_partition: int | None = None) -> dict:
    """
    Replicate each partition independently.
    Returns a summary of how many events were copied per partition.
    """
    summary = {}

    for partition in range(NUM_PARTITIONS):
        copied = replicate_partition(topic, partition, max_events=max_events_per_partition)
        summary[partition] = copied

    return summary


# ------------------------
# Event reading
# ------------------------
def read_partition_from_role(topic: str, partition: int, role: str) -> list[dict]:
    if role == "leader":
        return read_log_file(
            get_leader_log_path(topic, partition), topic, partition, role="leader"
        )
    if role == "replica":
        return read_log_file(
            get_replica_log_path(topic, partition), topic, partition, role="replica"
        )

    raise ValueError("role must be either 'leader' or 'replica'")


def read_partition(topic: str, partition: int, prefer: str = "leader") -> list[dict]:
    """
    Read from leader or replica explicitly.
    Fallback behavior:
    - prefer='leader': use leader if it exists and has events, else replica
    - prefer='replica': use replica if it exists and has events, else leader
    """
    leader_events = read_partition_from_role(topic, partition, role="leader")
    replica_events = read_partition_from_role(topic, partition, role="replica")

    if prefer == "leader":
        if leader_events:
            return leader_events
        return replica_events

    if prefer == "replica":
        if replica_events:
            return replica_events
        return leader_events

    raise ValueError("prefer must be either 'leader' or 'replica'")


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
def consume_batch(
    consumer: str,
    topic: str,
    batch_size: int,
    read_preference: str = "leader",
) -> list[dict]:
    if batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")

    results = []

    for partition in range(NUM_PARTITIONS):
        start_offset = get_offset(consumer, topic, partition)
        events = read_partition(topic, partition, prefer=read_preference)

        batch = [e for e in events if e["_offset"] >= start_offset][:batch_size]

        if batch:
            next_offset = batch[-1]["_offset"] + 1
            set_offset(consumer, topic, partition, next_offset)

        results.extend(batch)

    return results


# ------------------------
# Replication inspection
# ------------------------
def get_replication_status(topic: str, partition: int) -> dict:
    leader_events = read_partition_from_role(topic, partition, role="leader")
    replica_events = read_partition_from_role(topic, partition, role="replica")

    leader_count = len(leader_events)
    replica_count = len(replica_events)
    lag = leader_count - replica_count
    in_sync = lag == 0

    return {
        "topic": topic,
        "partition": partition,
        "leader_count": leader_count,
        "replica_count": replica_count,
        "replication_lag": lag,
        "in_sync": in_sync,
    }


def print_replication_status(topic: str) -> None:
    print("\nReplication status:")
    for partition in range(NUM_PARTITIONS):
        status = get_replication_status(topic, partition)
        print(
            f"  partition={status['partition']} "
            f"leader_count={status['leader_count']} "
            f"replica_count={status['replica_count']} "
            f"replication_lag={status['replication_lag']} "
            f"in_sync={status['in_sync']}"
        )


# ------------------------
# Helpers
# ------------------------
def build_event(event_type: str, payload: dict) -> dict:
    return {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


def describe_consumer(consumer: str, topic: str, read_preference: str = "leader") -> None:
    offsets = load_offsets(consumer)

    print(f"\nConsumer: {consumer} (read_preference={read_preference})")
    for partition in range(NUM_PARTITIONS):
        current = offsets.get(topic, {}).get(str(partition), 0)
        total = len(read_partition(topic, partition, prefer=read_preference))
        lag = total - current

        print(
            f"  partition={partition} "
            f"offset={current} "
            f"visible_events={total} "
            f"consumer_lag={lag}"
        )


# ------------------------
# Demo
# ------------------------
def main() -> None:
    ensure_dirs_exist()
    reset_demo_state()

    topic = "orders"
    leader_consumer = "analytics_leader"
    replica_consumer = "analytics_replica"

    orders = [
        {"order_id": 1, "user_id": "A", "amount": 10},
        {"order_id": 2, "user_id": "B", "amount": 20},
        {"order_id": 3, "user_id": "C", "amount": 30},
        {"order_id": 4, "user_id": "A", "amount": 40},
        {"order_id": 5, "user_id": "B", "amount": 50},
        {"order_id": 6, "user_id": "C", "amount": 60},
    ]

    print("\nProducing events to leaders only:")
    for order in orders:
        partition = append_event(
            topic=topic,
            key=order["user_id"],
            event=build_event("order_created", order),
        )
        print(f"order_id={order['order_id']} -> leader partition {partition}")

    print_replication_status(topic)

    print("\nConsume from leader before replication:")
    leader_batch = consume_batch(
        leader_consumer,
        topic,
        batch_size=2,
        read_preference="leader",
    )
    for event in leader_batch:
        print(event)

    describe_consumer(leader_consumer, topic, read_preference="leader")

    print("\nConsume from replica before replication (should likely see less or nothing):")
    replica_batch_before = consume_batch(
        replica_consumer,
        topic,
        batch_size=2,
        read_preference="replica",
    )
    for event in replica_batch_before:
        print(event)

    describe_consumer(replica_consumer, topic, read_preference="replica")

    print("\nReplicating only 1 event per partition...")
    replication_summary_1 = replicate_all_partitions(topic, max_events_per_partition=1)
    print(f"Replication summary: {replication_summary_1}")

    print_replication_status(topic)

    print("\nConsume from replica after partial replication:")
    replica_batch_after_partial = consume_batch(
        replica_consumer,
        topic,
        batch_size=2,
        read_preference="replica",
    )
    for event in replica_batch_after_partial:
        print(event)

    describe_consumer(replica_consumer, topic, read_preference="replica")

    print("\nReplicating remaining events...")
    replication_summary_2 = replicate_all_partitions(topic)
    print(f"Replication summary: {replication_summary_2}")

    print_replication_status(topic)

    print("\nConsume from replica after full catch-up:")
    replica_batch_after_full = consume_batch(
        replica_consumer,
        topic,
        batch_size=2,
        read_preference="replica",
    )
    for event in replica_batch_after_full:
        print(event)

    describe_consumer(replica_consumer, topic, read_preference="replica")


if __name__ == "__main__":
    main()