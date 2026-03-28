import socket
import json
import threading
import time
import os
from datetime import datetime

HOST = "127.0.0.1"
PORT = 8080

# Use the script's directory to find model.json and server.log
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MODEL_PATHS = [
    os.path.join(SCRIPT_DIR, "model_v1.json"),
    os.path.join(SCRIPT_DIR, "model_v2.json"),
]
LOG_PATH = os.path.join(SCRIPT_DIR, "server.log")


# ------------------------
# Metrics (shared state)
# ------------------------
metrics = {
    "total_requests": 0,
    "prediction_requests": 0,
    "prediction_successes": 0,
    "prediction_errors": 0,
    "batch_prediction_requests": 0,
    "batch_prediction_successes": 0,
    "batch_prediction_errors": 0,
    "total_prediction_latency_ms": 0.0,
    "total_batch_prediction_latency_ms": 0.0,
    "total_batch_instances": 0,
    "model_switches": 0,
    "model_reloads": 0,
}

metrics_lock = threading.Lock()


# ------------------------
# Logging
# ------------------------
def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    print(full_message)

    with open(LOG_PATH, "a", encoding="utf-8") as f:
        f.write(full_message + "\n")


# ------------------------
# Model Loading / Registry
# ------------------------
def load_model(path):
    with open(path, "r", encoding="utf-8") as f:
        model_data = json.load(f)

    required_fields = [
        "model_name",
        "model_version",
        "trained_at",
        "bias",
        "weights",
    ]

    for field in required_fields:
        if field not in model_data:
            raise ValueError(f"Missing required model field: {field}")

    if not isinstance(model_data["weights"], list):
        raise ValueError("Model field 'weights' must be a list")

    model = {
        "model_name": model_data["model_name"],
        "model_version": model_data["model_version"],
        "trained_at": model_data["trained_at"],
        "bias": model_data["bias"],
        "weights": model_data["weights"],
        "training_summary": model_data.get("training_summary", {}),
        "evaluation_metrics": model_data.get("evaluation_metrics", {}),
        "source_path": path,
    }

    return model


def load_models(paths):
    loaded_models = {}

    for path in paths:
        model = load_model(path)
        version = model["model_version"]

        if version in loaded_models:
            raise ValueError(f"Duplicate model version detected: {version}")

        loaded_models[version] = model

    if not loaded_models:
        raise ValueError("No models loaded")

    return loaded_models


models = load_models(MODEL_PATHS)
active_model_version = "v1.0.0"
model_lock = threading.Lock()

if active_model_version not in models:
    raise ValueError(f"Configured active model version not found: {active_model_version}")

log_message(
    f"Initial models loaded: versions={list(models.keys())}, "
    f"active_model={active_model_version}"
)


def get_active_model():
    with model_lock:
        return models[active_model_version]


def get_active_model_version():
    with model_lock:
        return active_model_version


def switch_active_model(new_version):
    global active_model_version

    with model_lock:
        if new_version not in models:
            raise ValueError(f"Unknown model version: {new_version}")

        old_version = active_model_version
        active_model_version = new_version

    with metrics_lock:
        metrics["model_switches"] += 1

    log_message(f"Switched active model from {old_version} to {new_version}")


def reload_models_from_disk():
    global models, active_model_version

    new_models = load_models(MODEL_PATHS)

    with model_lock:
        old_versions = list(models.keys())
        old_active_version = active_model_version

        models = new_models

        if old_active_version in models:
            active_model_version = old_active_version
            preserved = True
        else:
            active_model_version = sorted(models.keys())[0]
            preserved = False

        new_versions = list(models.keys())
        new_active_version = active_model_version

    with metrics_lock:
        metrics["model_reloads"] += 1

    log_message(
        f"Reloaded models from disk. "
        f"old_versions={old_versions} "
        f"new_versions={new_versions} "
        f"old_active={old_active_version} "
        f"new_active={new_active_version} "
        f"preserved_active={preserved}"
    )

    return {
        "available_versions": new_versions,
        "old_active_version": old_active_version,
        "new_active_version": new_active_version,
        "preserved_active": preserved,
    }


# ------------------------
# Prediction helpers
# ------------------------
def validate_features(features, expected_length):
    if not isinstance(features, list):
        raise ValueError("Each feature vector must be a list")

    if len(features) != expected_length:
        raise ValueError(f"Expected {expected_length} features per instance")


def predict_one(features):
    active_model = get_active_model()
    weights = active_model["weights"]
    bias = active_model["bias"]

    validate_features(features, len(weights))

    result = bias
    for x, w in zip(features, weights):
        result += x * w

    return result, active_model


def predict_batch(instances):
    if not isinstance(instances, list):
        raise ValueError("'instances' must be a list")

    if len(instances) == 0:
        raise ValueError("'instances' must not be empty")

    active_model = get_active_model()
    weights = active_model["weights"]
    bias = active_model["bias"]

    predictions = []

    for features in instances:
        validate_features(features, len(weights))

        result = bias
        for x, w in zip(features, weights):
            result += x * w

        predictions.append(result)

    return predictions, active_model


# ------------------------
# HTTP Helpers
# ------------------------
def json_response(status_line, body_dict):
    response_body = json.dumps(body_dict)
    response_body_bytes = response_body.encode("utf-8")

    response_headers = (
        f"{status_line}\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(response_body_bytes)}\r\n"
        "\r\n"
    )

    return response_headers.encode("utf-8") + response_body_bytes


def parse_http_request(request_text):
    parts = request_text.split("\r\n\r\n", 1)

    header_text = parts[0]
    body_text = parts[1] if len(parts) > 1 else ""

    header_lines = header_text.split("\r\n")
    request_line = header_lines[0]
    request_line_parts = request_line.split()

    if len(request_line_parts) != 3:
        raise ValueError("Malformed request line")

    method, path, version = request_line_parts
    return method, path, version, body_text


def get_metrics_snapshot():
    active_model = get_active_model()

    with metrics_lock:
        prediction_successes = metrics["prediction_successes"]
        batch_prediction_successes = metrics["batch_prediction_successes"]

        avg_prediction_latency = (
            metrics["total_prediction_latency_ms"] / prediction_successes
            if prediction_successes > 0
            else 0.0
        )

        avg_batch_prediction_latency = (
            metrics["total_batch_prediction_latency_ms"] / batch_prediction_successes
            if batch_prediction_successes > 0
            else 0.0
        )

        avg_batch_size = (
            metrics["total_batch_instances"] / metrics["batch_prediction_requests"]
            if metrics["batch_prediction_requests"] > 0
            else 0.0
        )

        return {
            "total_requests": metrics["total_requests"],
            "prediction_requests": metrics["prediction_requests"],
            "prediction_successes": metrics["prediction_successes"],
            "prediction_errors": metrics["prediction_errors"],
            "batch_prediction_requests": metrics["batch_prediction_requests"],
            "batch_prediction_successes": metrics["batch_prediction_successes"],
            "batch_prediction_errors": metrics["batch_prediction_errors"],
            "model_switches": metrics["model_switches"],
            "model_reloads": metrics["model_reloads"],
            "avg_prediction_latency_ms": round(avg_prediction_latency, 4),
            "avg_batch_prediction_latency_ms": round(avg_batch_prediction_latency, 4),
            "avg_batch_size": round(avg_batch_size, 4),
            "total_batch_instances": metrics["total_batch_instances"],
            "active_model_name": active_model["model_name"],
            "active_model_version": active_model["model_version"],
        }


def get_available_models():
    with model_lock:
        return [
            {
                "model_name": model["model_name"],
                "model_version": model["model_version"],
                "trained_at": model["trained_at"],
                "num_features": len(model["weights"]),
                "source_path": model["source_path"],
            }
            for model in models.values()
        ]


# ------------------------
# Routing
# ------------------------
def handle_route(method, path, body_text, client_address):
    active_model = get_active_model()

    if method == "GET" and path == "/":
        return "HTTP/1.1 200 OK", {
            "message": "ML inference server running",
            "active_model_name": active_model["model_name"],
            "active_model_version": active_model["model_version"],
        }

    elif method == "GET" and path == "/health":
        return "HTTP/1.1 200 OK", {
            "status": "ok",
            "model_loaded": True,
            "active_model_name": active_model["model_name"],
            "active_model_version": active_model["model_version"],
        }

    elif method == "GET" and path == "/model-info":
        return "HTTP/1.1 200 OK", {
            "model_name": active_model["model_name"],
            "model_version": active_model["model_version"],
            "trained_at": active_model["trained_at"],
            "bias": active_model["bias"],
            "weights": active_model["weights"],
            "num_features": len(active_model["weights"]),
            "source_path": active_model["source_path"],
            "training_summary": active_model.get("training_summary", {}),
            "evaluation_metrics": active_model.get("evaluation_metrics", {}),
        }

    elif method == "GET" and path == "/models":
        return "HTTP/1.1 200 OK", {
            "available_models": get_available_models(),
            "active_model_version": get_active_model_version(),
        }

    elif method == "GET" and path == "/active-model":
        return "HTTP/1.1 200 OK", {
            "model_name": active_model["model_name"],
            "model_version": active_model["model_version"],
            "trained_at": active_model["trained_at"],
            "num_features": len(active_model["weights"]),
            "source_path": active_model["source_path"],
        }

    elif method == "GET" and path == "/metrics":
        return "HTTP/1.1 200 OK", get_metrics_snapshot()

    elif method == "POST" and path == "/switch-model":
        try:
            body_data = json.loads(body_text)

            if "model_version" not in body_data:
                return "HTTP/1.1 400 Bad Request", {
                    "error": "Missing 'model_version'"
                }

            new_version = body_data["model_version"]

            if not isinstance(new_version, str):
                return "HTTP/1.1 400 Bad Request", {
                    "error": "'model_version' must be a string"
                }

            switch_active_model(new_version)
            new_active_model = get_active_model()

            return "HTTP/1.1 200 OK", {
                "message": "Active model switched successfully",
                "active_model_name": new_active_model["model_name"],
                "active_model_version": new_active_model["model_version"],
            }

        except json.JSONDecodeError:
            return "HTTP/1.1 400 Bad Request", {
                "error": "Invalid JSON"
            }
        except ValueError as e:
            return "HTTP/1.1 400 Bad Request", {
                "error": str(e)
            }

    elif method == "POST" and path == "/reload-models":
        try:
            reload_result = reload_models_from_disk()
            reloaded_active_model = get_active_model()

            return "HTTP/1.1 200 OK", {
                "message": "Models reloaded successfully",
                "available_versions": reload_result["available_versions"],
                "old_active_version": reload_result["old_active_version"],
                "new_active_version": reload_result["new_active_version"],
                "preserved_active": reload_result["preserved_active"],
                "active_model_name": reloaded_active_model["model_name"],
                "active_model_version": reloaded_active_model["model_version"],
            }
        except Exception as e:
            log_message(f"Model reload failed error={str(e)}")
            return "HTTP/1.1 500 Internal Server Error", {
                "error": "Model reload failed",
                "details": str(e),
            }

    elif method == "POST" and path == "/predict":
        with metrics_lock:
            metrics["prediction_requests"] += 1

        start_time = time.perf_counter()

        try:
            body_data = json.loads(body_text)

            if "features" not in body_data:
                with metrics_lock:
                    metrics["prediction_errors"] += 1
                return "HTTP/1.1 400 Bad Request", {
                    "error": "Missing 'features'"
                }

            features = body_data["features"]
            prediction, model_used = predict_one(features)

            elapsed_ms = (time.perf_counter() - start_time) * 1000.0

            with metrics_lock:
                metrics["prediction_successes"] += 1
                metrics["total_prediction_latency_ms"] += elapsed_ms

            log_message(
                f"POST /predict from {client_address[0]} "
                f"model={model_used['model_name']} "
                f"version={model_used['model_version']} "
                f"features={features} "
                f"prediction={prediction} "
                f"latency_ms={elapsed_ms:.4f}"
            )

            return "HTTP/1.1 200 OK", {
                "prediction": prediction,
                "latency_ms": round(elapsed_ms, 4),
                "model_name": model_used["model_name"],
                "model_version": model_used["model_version"],
            }

        except json.JSONDecodeError:
            with metrics_lock:
                metrics["prediction_errors"] += 1

            current_model = get_active_model()
            log_message(
                f"POST /predict from {client_address[0]} "
                f"model={current_model['model_name']} "
                f"version={current_model['model_version']} "
                f"error=Invalid JSON body"
            )

            return "HTTP/1.1 400 Bad Request", {
                "error": "Invalid JSON"
            }

        except ValueError as e:
            with metrics_lock:
                metrics["prediction_errors"] += 1

            current_model = get_active_model()
            log_message(
                f"POST /predict from {client_address[0]} "
                f"model={current_model['model_name']} "
                f"version={current_model['model_version']} "
                f"error={str(e)}"
            )

            return "HTTP/1.1 400 Bad Request", {
                "error": str(e)
            }

    elif method == "POST" and path == "/predict-batch":
        with metrics_lock:
            metrics["batch_prediction_requests"] += 1

        start_time = time.perf_counter()

        try:
            body_data = json.loads(body_text)

            if "instances" not in body_data:
                with metrics_lock:
                    metrics["batch_prediction_errors"] += 1
                return "HTTP/1.1 400 Bad Request", {
                    "error": "Missing 'instances'"
                }

            instances = body_data["instances"]
            predictions, model_used = predict_batch(instances)

            elapsed_ms = (time.perf_counter() - start_time) * 1000.0

            with metrics_lock:
                metrics["batch_prediction_successes"] += 1
                metrics["total_batch_prediction_latency_ms"] += elapsed_ms
                metrics["total_batch_instances"] += len(instances)

            log_message(
                f"POST /predict-batch from {client_address[0]} "
                f"model={model_used['model_name']} "
                f"version={model_used['model_version']} "
                f"batch_size={len(instances)} "
                f"predictions={predictions} "
                f"latency_ms={elapsed_ms:.4f}"
            )

            return "HTTP/1.1 200 OK", {
                "predictions": predictions,
                "count": len(predictions),
                "latency_ms": round(elapsed_ms, 4),
                "model_name": model_used["model_name"],
                "model_version": model_used["model_version"],
            }

        except json.JSONDecodeError:
            with metrics_lock:
                metrics["batch_prediction_errors"] += 1

            current_model = get_active_model()
            log_message(
                f"POST /predict-batch from {client_address[0]} "
                f"model={current_model['model_name']} "
                f"version={current_model['model_version']} "
                f"error=Invalid JSON body"
            )

            return "HTTP/1.1 400 Bad Request", {
                "error": "Invalid JSON"
            }

        except ValueError as e:
            with metrics_lock:
                metrics["batch_prediction_errors"] += 1

            current_model = get_active_model()
            log_message(
                f"POST /predict-batch from {client_address[0]} "
                f"model={current_model['model_name']} "
                f"version={current_model['model_version']} "
                f"error={str(e)}"
            )

            return "HTTP/1.1 400 Bad Request", {
                "error": str(e)
            }

    else:
        return "HTTP/1.1 404 Not Found", {
            "error": "Not Found"
        }


# ------------------------
# Client Handler
# ------------------------
def handle_client(client_socket, client_address):
    log_message(f"Accepted connection from {client_address}")

    try:
        request_data = client_socket.recv(4096)
        request_text = request_data.decode("utf-8", errors="ignore")

        method, path, version, body_text = parse_http_request(request_text)

        with metrics_lock:
            metrics["total_requests"] += 1

        active_model = get_active_model()
        log_message(
            f"{method} {path} from {client_address[0]} "
            f"active_model={active_model['model_name']}:{active_model['model_version']}"
        )

        status_line, response_body_dict = handle_route(
            method, path, body_text, client_address
        )

    except ValueError as e:
        log_message(f"Bad request from {client_address[0]} error={str(e)}")
        status_line = "HTTP/1.1 400 Bad Request"
        response_body_dict = {"error": str(e)}

    except Exception as e:
        log_message(f"Server error from {client_address[0]} error={str(e)}")
        status_line = "HTTP/1.1 500 Internal Server Error"
        response_body_dict = {"error": "Internal Server Error"}

    response = json_response(status_line, response_body_dict)
    client_socket.sendall(response)
    client_socket.close()


# ------------------------
# Server Loop
# ------------------------
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(5)

log_message(f"Server running at http://{HOST}:{PORT}")

while True:
    client_socket, client_address = server_socket.accept()

    thread = threading.Thread(
        target=handle_client,
        args=(client_socket, client_address),
        daemon=True
    )
    thread.start()