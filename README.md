# Systems Design & ML Ops

A demonstration project showcasing three core distributed systems components: a distributed key-value store, an event log system, and an ML model serving infrastructure.

## Project Structure

```
sys_design_ml_ops/
├── dist_kv_store/       # Distributed key-value store
├── event_log_system/    # Event log / message queue system
└── ml_http_server/      # ML model serving HTTP server
```

---

## 1. Distributed Key-Value Store (`dist_kv_store/`)

A distributed key-value store implementing consistent hashing, quorum-based replication, and durability via write-ahead logs (WAL) and snapshots.

### Components

| File | Description |
|------|-------------|
| `hash_ring.py` | Consistent hashing implementation with virtual nodes |
| `kv_store.py` | Core key-value store with WAL and snapshot persistence |
| `router.py` | HTTP router with quorum reads/writes and read repair |
| `shard_server.py` | HTTP server exposing KV operations per shard |
| `metrics.py` | Metrics collection for observability |

### Architecture

- **Sharding**: Uses consistent hashing to distribute keys across nodes
- **Replication**: Configurable replica count with quorum-based operations
- **Durability**: Write-ahead log + periodic snapshots
- **Health**: Node health checking and automatic read repair

### Usage

Start shard servers on ports 8080, 8081, 8082, then use the router at port 8090:

```bash
# Start shard 0
python shard_server.py 0 8080

# Start shard 1
python shard_server.py 1 8081

# Start shard 2
python shard_server.py 2 8082

# Start router
python router.py
```

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/kv/{key}` | Get value by key |
| PUT | `/kv/{key}` | Set key-value pair |
| DELETE | `/kv/{key}` | Delete key |
| GET | `/health` | Health check |
| GET | `/metrics` | Get metrics summary |

---

## 2. Event Log System (`event_log_system/`)

A partitioned event log system with consumer offset tracking and state management.

### Components

| File | Description |
|------|-------------|
| `event_log.py` | Core event log operations |

### Features

- **Partitioned Topics**: Events partitioned by key hash
- **Consumer Offsets**: Track read position per consumer
- **State Management**: Persist consumer state
- **Multiple Consumers**: Support for analytics consumers

### Usage

```python
from event_log import append_event, read_partition, get_partition

# Write event
partition = append_event("orders", "order_123", {"type": "created", "amount": 100})

# Read events from partition
events = read_partition("orders", partition)
```

### Key Functions

| Function | Description |
|----------|-------------|
| `append_event(topic, key, event)` | Append event to topic |
| `read_partition(topic, partition)` | Read all events from partition |
| `get_partition(topic, key)` | Get partition for key |
| `get_offset_path(consumer)` | Get offset file path for consumer |

---

## 3. ML HTTP Server (`ml_http_server/`)

An HTTP server for serving ML models with support for multiple versions, hot reloading, and comprehensive metrics.

### Components

| File | Description |
|------|-------------|
| `server.py` | HTTP server with prediction endpoints |
| `train_model.py` | Model training utilities |
| `utils.py` | Helper functions |
| `model_v1.json` | Trained model (v1) |
| `model_v2.json` | Trained model (v2) |

### Features

- **Multi-Version Models**: Support for multiple model versions
- **Hot Reloading**: Dynamically reload models without restart
- **Metrics**: Request counts, latencies, success rates
- **Batch Predictions**: Support for single and batch predictions

### Usage

```bash
# Start server
python server.py
```

### API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/models` | List available models |
| POST | `/predict` | Make prediction |
| POST | `/predict/batch` | Batch predictions |
| PUT | `/models/{version}/reload` | Reload model |
| GET | `/metrics` | Get metrics |

### Prediction Request Format

```json
{
  "features": [1.0, 2.0]
}
```

### Batch Prediction Request Format

```json
{
  "instances": [
    [1.0, 2.0],
    [3.0, 4.0]
  ]
}
```

---

## Running the Full System

1. **Start Distributed KV Store**:
   ```bash
   cd dist_kv_store
   # Start 3 shard servers in separate terminals
   python shard_server.py 0 8080 &
   python shard_server.py 1 8081 &
   python shard_server.py 2 8082 &
   # Start router
   python router.py
   ```

2. **Use Event Log System**:
   ```python
   from event_log_system.event_log import append_event, read_partition
   # Write and read events
   ```

3. **Start ML Server**:
   ```bash
   cd ml_http_server
   python server.py
   ```

---

## Dependencies

- Python 3.10+
- `numpy` (for ML training)

Install dependencies:
```bash
pip install numpy
```

---

## License

MIT