import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
import os


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = Path(SCRIPT_DIR) / "logs"
OFFSET_DIR = Path(SCRIPT_DIR) / "offsets"
STATE_DIR = Path(SCRIPT_DIR) / "state"


NUM_PARTITIONS = 3


# ------------------------
# Setup
# ------------------------
def ensure_dirs_exist() -> None:
    LOG_DIR.mkdir(exist_ok=True)
    OFFSET_DIR.mkdir(exist_ok=True)
    STATE_DIR.mkdir(exist_ok=True)


def reset_demo_state() -> None:
    for path in LOG_DIR.glob("*.log"):
        path.unlink(missing_ok=True)
    for path in OFFSET_DIR.glob("*.json"):
        path.unlink(missing_ok=True)
    for path in STATE_DIR.glob("*.json"):
        path.unlink(missing_ok=True)


# ------------------------
# Partitioning
# ------------------------
def get_partition(topic: str, key: str) -> int:
    hash_val = int(hashlib.md5(key.encode("utf-8")).hexdigest(), 16)
    return hash_val % NUM_PARTITIONS


def get_leader_log_path(topic: str, partition: int) -> Path:
    return LOG_DIR / f"{topic}_{partition}_leader.log"


# ------------------------
# Event writing
# ------------------------
def append_event(topic: str, key: str, event: dict) -> int:
    partition = get_partition(topic, key)
    path = get_leader_log_path(topic, partition)

    record = {
        **event,
        "_partition": partition,
        "_key": key,
    }

    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

    return partition


# ------------------------
# Event reading
# ------------------------
def read_partition(topic: str, partition: int) -> list[dict]:
    path = get_leader_log_path(topic, partition)
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
                    f"{topic} partition {partition}: invalid JSON at line {offset + 1}: {e}"
                ) from e

            event["_offset"] = offset
            events.append(event)

    return events


# ------------------------
# Offsets
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
    offsets.setdefault(topic, {})
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
# Stateful stream processing
# ------------------------
def get_state_path(processor_name: str) -> Path:
    return STATE_DIR / f"{processor_name}_state.json"


def initial_state() -> dict:
    return {
        "order_count_by_user": {},
        "total_amount_by_user": {},
        "total_orders": 0,
        "total_revenue": 0.0,
        "processed_event_ids": [],
    }


def load_state(processor_name: str) -> dict:
    path = get_state_path(processor_name)

    if not path.exists():
        return initial_state()

    raw_text = path.read_text(encoding="utf-8").strip()
    if not raw_text:
        return initial_state()

    return json.loads(raw_text)


def save_state(processor_name: str, state: dict) -> None:
    path = get_state_path(processor_name)
    path.write_text(json.dumps(state, indent=2), encoding="utf-8")


def event_id(event: dict) -> str:
    return f"{event['_partition']}:{event['_offset']}"


def apply_order_event_to_state(state: dict, event: dict, idempotent: bool = True) -> bool:
    eid = event_id(event)

    if idempotent and eid in state["processed_event_ids"]:
        return False

    event_type = event["event_type"]
    payload = event["payload"]

    if event_type != "order_created":
        return False

    user_id = str(payload["user_id"])
    amount = float(payload["amount"])

    state["order_count_by_user"].setdefault(user_id, 0)
    state["total_amount_by_user"].setdefault(user_id, 0.0)

    state["order_count_by_user"][user_id] += 1
    state["total_amount_by_user"][user_id] += amount
    state["total_orders"] += 1
    state["total_revenue"] += amount

    if idempotent:
        state["processed_event_ids"].append(eid)

    return True


def process_batch_into_state(
    processor_name: str,
    events: list[dict],
    idempotent: bool = True,
) -> dict:
    state = load_state(processor_name)

    applied_count = 0
    skipped_count = 0

    for event in events:
        changed = apply_order_event_to_state(state, event, idempotent=idempotent)
        if changed:
            applied_count += 1
        else:
            skipped_count += 1

    state["_last_batch_applied"] = applied_count
    state["_last_batch_skipped"] = skipped_count

    save_state(processor_name, state)
    return state


# ------------------------
# Feature Store Layer
# ------------------------
def build_user_features_from_state(state: dict, user_id: str) -> dict:
    """
    Build ML-friendly online features for one user from the materialized state.
    """
    user_id = str(user_id)

    order_count = int(state["order_count_by_user"].get(user_id, 0))
    total_spent = float(state["total_amount_by_user"].get(user_id, 0.0))
    avg_order_value = total_spent / order_count if order_count > 0 else 0.0

    return {
        "user_id": user_id,
        "features": {
            "order_count": order_count,
            "total_spent": round(total_spent, 4),
            "avg_order_value": round(avg_order_value, 4),
        },
    }


def get_user_features(processor_name: str, user_id: str) -> dict:
    """
    Online feature lookup for a single user.
    """
    state = load_state(processor_name)
    return build_user_features_from_state(state, user_id)


def get_all_user_features(processor_name: str) -> list[dict]:
    """
    Build feature rows for every user currently present in state.
    """
    state = load_state(processor_name)

    user_ids = set(state["order_count_by_user"].keys()) | set(
        state["total_amount_by_user"].keys()
    )

    return [
        build_user_features_from_state(state, user_id)
        for user_id in sorted(user_ids)
    ]


# ------------------------
# Mock Prediction Layer
# ------------------------
def score_user_from_features(features: dict) -> float:
    """
    Very simple mock model.
    This is just a hand-crafted scoring function to simulate inference.
    """
    order_count = float(features["order_count"])
    total_spent = float(features["total_spent"])
    avg_order_value = float(features["avg_order_value"])

    score = (
        0.2 * order_count
        + 0.02 * total_spent
        + 0.1 * avg_order_value
    )
    return round(score, 4)


def predict_user_value(processor_name: str, user_id: str) -> dict:
    """
    Online inference for one user using features from the feature store layer.
    """
    feature_row = get_user_features(processor_name, user_id)
    features = feature_row["features"]
    prediction = score_user_from_features(features)

    return {
        "user_id": str(user_id),
        "model_name": "mock_user_value_model",
        "model_version": "v1.0.0",
        "features": features,
        "prediction": {
            "user_value_score": prediction
        },
    }


def predict_all_users(processor_name: str) -> list[dict]:
    """
    Batch-style inference over all users currently present in state.
    """
    feature_rows = get_all_user_features(processor_name)

    predictions = []
    for row in feature_rows:
        features = row["features"]
        prediction = score_user_from_features(features)

        predictions.append({
            "user_id": row["user_id"],
            "model_name": "mock_user_value_model",
            "model_version": "v1.0.0",
            "features": features,
            "prediction": {
                "user_value_score": prediction
            },
        })

    return predictions


# ------------------------
# Helpers
# ------------------------
def build_event(event_type: str, payload: dict) -> dict:
    return {
        "event_type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }


def print_state(processor_name: str) -> None:
    state = load_state(processor_name)
    print(f"\nState for processor '{processor_name}':")
    print(json.dumps(state, indent=2))


def print_feature_store(processor_name: str) -> None:
    print(f"\nFeature store snapshot for processor '{processor_name}':")
    rows = get_all_user_features(processor_name)
    print(json.dumps(rows, indent=2))


# ------------------------
# Demo
# ------------------------
def main() -> None:
    ensure_dirs_exist()
    reset_demo_state()

    topic = "orders"
    consumer = "analytics_consumer"
    processor = "analytics"

    orders = [
        {"order_id": 1, "user_id": "A", "amount": 10.0},
        {"order_id": 2, "user_id": "B", "amount": 20.0},
        {"order_id": 3, "user_id": "A", "amount": 30.0},
        {"order_id": 4, "user_id": "C", "amount": 15.0},
        {"order_id": 5, "user_id": "B", "amount": 25.0},
        {"order_id": 6, "user_id": "A", "amount": 5.0},
    ]

    print("\nProducing order events...")
    for order in orders:
        partition = append_event(
            topic=topic,
            key=order["user_id"],
            event=build_event("order_created", order),
        )
        print(f"order_id={order['order_id']} -> partition {partition}")

    print("\nConsume first batch and update state:")
    batch1 = consume_batch(consumer, topic, batch_size=2)
    for event in batch1:
        print(event)

    process_batch_into_state(processor, batch1, idempotent=True)
    print_state(processor)
    print_feature_store(processor)

    print("\nConsume second batch and update state:")
    batch2 = consume_batch(consumer, topic, batch_size=2)
    for event in batch2:
        print(event)

    process_batch_into_state(processor, batch2, idempotent=True)
    print_state(processor)
    print_feature_store(processor)

    print("\nOnline feature lookup examples:")
    print(json.dumps(get_user_features(processor, "A"), indent=2))
    print(json.dumps(get_user_features(processor, "B"), indent=2))
    print(json.dumps(get_user_features(processor, "Z"), indent=2))

    print("\nMock prediction examples:")
    print(json.dumps(predict_user_value(processor, "A"), indent=2))
    print(json.dumps(predict_user_value(processor, "B"), indent=2))
    print(json.dumps(predict_user_value(processor, "Z"), indent=2))

    print("\nBatch mock predictions for all known users:")
    print(json.dumps(predict_all_users(processor), indent=2))


if __name__ == "__main__":
    main()