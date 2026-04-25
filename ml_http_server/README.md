# ML HTTP Server

An HTTP server for serving ML models with support for multiple versions, hot reloading, and comprehensive metrics tracking.

## Overview

This module provides a production-ready model serving infrastructure with:
- Multi-version model support
- Hot reloading without restart
- Comprehensive request metrics
- Batch prediction capabilities
- Thread-safe operations

## Project Structure

```
ml_http_server/
├── server.py        # Main HTTP server
├── train_model.py   # Model training utilities
├── utils.py         # Helper functions (reserved for future use)
├── model_v1.json    # Trained model (v1.0.0)
├── model_v2.json    # Trained model (v2.0.0)
└── server.log       # Server activity log
```

## Quick Start

```bash
# Start the server
python server.py
```

The server will start on `http://127.0.0.1:8080` by default.

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/health` | Health check |
| GET | `/models` | List all available models |
| GET | `/models/active` | Get currently active model |
| POST | `/predict` | Make a single prediction |
| POST | `/predict/batch` | Make batch predictions |
| PUT | `/models/{version}/reload` | Reload a specific model |
| PUT | `/models/switch/{version}` | Switch active model |
| GET | `/metrics` | Get server metrics |

### Health Check

```bash
curl http://127.0.0.1:8080/health
```

Response:
```json
{
  "status": "ok",
  "active_model": "v1.0.0"
}
```

### List Models

```bash
curl http://127.0.0.1:8080/models
```

Response:
```json
{
  "available_models": ["v1.0.0", "v2.0.0"],
  "active_model": "v1.0.0"
}
```

### Single Prediction

```bash
curl -X POST http://127.0.0.1:8080/predict \
  -H "Content-Type: application/json" \
  -d '{"features": [1.0, 2.0]}'
```

Response:
```json
{
  "prediction": 5.415,
  "model_version": "v1.0.0"
}
```

### Batch Predictions

```bash
curl -X POST http://127.0.0.1:8080/predict/batch \
  -H "Content-Type: application/json" \
  -d '{"instances": [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]}'
```

Response:
```json
{
  "predictions": [5.415, 9.458, 13.501],
  "model_version": "v1.0.0",
  "count": 3
}
```

### Switch Model Version

```bash
curl -X PUT http://127.0.0.1:8080/models/switch/v2.0.0
```

Response:
```json
{
  "status": "ok",
  "previous_version": "v1.0.0",
  "active_version": "v2.0.0"
}
```

### Reload Model from Disk

```bash
curl -X PUT http://127.0.0.1:8080/models/v1.0.0/reload
```

Response:
```json
{
  "status": "ok",
  "model_version": "v1.0.0",
  "reloaded": true
}
```

### Get Metrics

```bash
curl http://127.0.0.1:8080/metrics
```

Response:
```json
{
  "total_requests": 150,
  "prediction_requests": 120,
  "prediction_successes": 118,
  "prediction_errors": 2,
  "batch_prediction_requests": 30,
  "batch_prediction_successes": 30,
  "batch_prediction_errors": 0,
  "avg_prediction_latency_ms": 0.45,
  "avg_batch_prediction_latency_ms": 1.2,
  "model_switches": 2,
  "model_reloads": 1
}
```

## Model Format

Models are stored as JSON files with the following structure:

```json
{
  "model_name": "linear_regression_demo",
  "model_version": "v1.0.0",
  "trained_at": "2026-03-28T20:05:34.460249+00:00",
  "bias": 1.374,
  "weights": [2.021, 0.999],
  "training_summary": {
    "train_size": 160,
    "eval_size": 40,
    "num_features": 2
  },
  "evaluation_metrics": {
    "mse": 0.276,
    "rmse": 0.525,
    "mae": 0.427,
    "r2": 0.992
  }
}
```

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `model_name` | string | Name of the model |
| `model_version` | string | Version identifier (e.g., "v1.0.0") |
| `trained_at` | string | ISO timestamp of training time |
| `bias` | number | Bias term (intercept) |
| `weights` | array | List of feature weights |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| `training_summary` | object | Training metadata |
| `evaluation_metrics` | object | Model evaluation metrics (MSE, RMSE, MAE, R²) |

## Training New Models

Use the `train_model.py` module to train new models:

```python
from train_model import (
    generate_dataset_v1,
    generate_dataset_v2,
    train_eval_split,
    fit_linear_regression,
    compute_regression_metrics,
)
import json
from datetime import datetime, timezone

# Generate training data
X, y = generate_dataset_v1()

# Split into train/eval sets
X_train, X_eval, y_train, y_eval = train_eval_split(X, y)

# Train model
bias, weights = fit_linear_regression(X_train, y_train)

# Evaluate
predictions = bias + X_eval @ weights
metrics = compute_regression_metrics(y_eval, predictions)

# Create model dictionary
model = {
    "model_name": "linear_regression_demo",
    "model_version": "v1.0.0",
    "trained_at": datetime.now(timezone.utc).isoformat(),
    "bias": bias,
    "weights": weights,
    "training_summary": {
        "train_size": len(X_train),
        "eval_size": len(X_eval),
        "num_features": X_train.shape[1]
    },
    "evaluation_metrics": metrics
}

# Save to JSON
with open("model_v1.json", "w") as f:
    json.dump(model, f, indent=2)
```

## Architecture

### Thread Safety

- All model operations are protected by `model_lock`
- Metrics updates are protected by `metrics_lock`
- Multiple concurrent requests are handled safely

### Metrics Collection

The server tracks the following metrics:

| Metric | Description |
|--------|-------------|
| `total_requests` | Total HTTP requests received |
| `prediction_requests` | Single prediction requests |
| `prediction_successes` | Successful predictions |
| `prediction_errors` | Failed predictions |
| `batch_prediction_requests` | Batch prediction requests |
| `batch_prediction_successes` | Successful batch predictions |
| `batch_prediction_errors` | Failed batch predictions |
| `avg_prediction_latency_ms` | Average prediction latency |
| `avg_batch_prediction_latency_ms` | Average batch prediction latency |
| `model_switches` | Number of model switches |
| `model_reloads` | Number of model reloads |

### Logging

All server activity is logged to `server.log` with timestamps:

```
[2026-04-25 10:30:45] Initial models loaded: versions=['v1.0.0', 'v2.0.0'], active_model=v1.0.0
[2026-04-25 10:31:12] Switched active model from v1.0.0 to v2.0.0
```

## Configuration

### Server Configuration

Edit these constants in `server.py`:

```python
HOST = "127.0.0.1"  # Server host
PORT = 8080         # Server port

MODEL_PATHS = [     # Model file paths
    "model_v1.json",
    "model_v2.json",
]
```

### Adding New Models

1. Create a new model JSON file (e.g., `model_v3.json`)
2. Add the path to `MODEL_PATHS` in `server.py`
3. Restart the server (or use the reload endpoint)

## Dependencies

- Python 3.10+
- `numpy` (for model training)

Install dependencies:
```bash
pip install numpy
```

## Example Usage

```python
import requests
import json

BASE_URL = "http://127.0.0.1:8080"

# Health check
resp = requests.get(f"{BASE_URL}/health")
print(resp.json())

# Make prediction
resp = requests.post(
    f"{BASE_URL}/predict",
    json={"features": [2.5, 3.1]}
)
print(resp.json())

# Batch prediction
resp = requests.post(
    f"{BASE_URL}/predict/batch",
    json={"instances": [[1, 2], [3, 4], [5, 6]]}
)
print(resp.json())

# Get metrics
resp = requests.get(f"{BASE_URL}/metrics")
print(resp.json())
```

## License

MIT
