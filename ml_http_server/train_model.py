import json
import os
from datetime import datetime, timezone

import numpy as np


def generate_dataset_v1():
    """
    Synthetic dataset generated from:
    y = 1.5 + 2.0*x1 + 1.0*x2 + noise
    """
    rng = np.random.default_rng(42)
    n_samples = 200

    x1 = rng.uniform(0, 10, size=n_samples)
    x2 = rng.uniform(0, 10, size=n_samples)
    noise = rng.normal(0, 0.5, size=n_samples)

    y = 1.5 + 2.0 * x1 + 1.0 * x2 + noise

    X = np.column_stack([x1, x2])
    return X, y


def generate_dataset_v2():
    """
    Synthetic dataset generated from:
    y = 0.5 + 1.2*x1 + 2.5*x2 + noise
    """
    rng = np.random.default_rng(123)
    n_samples = 200

    x1 = rng.uniform(0, 10, size=n_samples)
    x2 = rng.uniform(0, 10, size=n_samples)
    noise = rng.normal(0, 0.5, size=n_samples)

    y = 0.5 + 1.2 * x1 + 2.5 * x2 + noise

    X = np.column_stack([x1, x2])
    return X, y


def train_eval_split(X, y, eval_ratio=0.2, seed=0):
    rng = np.random.default_rng(seed)
    n_samples = X.shape[0]
    indices = np.arange(n_samples)
    rng.shuffle(indices)

    n_eval = int(n_samples * eval_ratio)
    eval_indices = indices[:n_eval]
    train_indices = indices[n_eval:]

    X_train = X[train_indices]
    y_train = y[train_indices]
    X_eval = X[eval_indices]
    y_eval = y[eval_indices]

    return X_train, X_eval, y_train, y_eval


def fit_linear_regression(X, y):
    """
    Closed-form solution:
        beta = pinv(X_design) @ y
    where X_design includes a leading column of 1s for the bias term.
    """
    n_samples = X.shape[0]
    ones = np.ones((n_samples, 1))
    X_design = np.hstack([ones, X])

    beta = np.linalg.pinv(X_design) @ y

    bias = float(beta[0])
    weights = beta[1:].tolist()
    return bias, weights


def predict_linear_regression(X, bias, weights):
    weights_array = np.asarray(weights)
    return bias + X @ weights_array


def compute_regression_metrics(y_true, y_pred):
    residuals = y_true - y_pred

    mse = float(np.mean(residuals ** 2))
    rmse = float(np.sqrt(mse))
    mae = float(np.mean(np.abs(residuals)))

    ss_res = float(np.sum(residuals ** 2))
    ss_tot = float(np.sum((y_true - np.mean(y_true)) ** 2))
    r2 = float(1.0 - ss_res / ss_tot) if ss_tot > 0 else 0.0

    return {
        "mse": mse,
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
    }


def save_model_artifact(
    model_name,
    model_version,
    bias,
    weights,
    metrics,
    train_size,
    eval_size,
    output_path,
):
    artifact = {
        "model_name": model_name,
        "model_version": model_version,
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "bias": bias,
        "weights": weights,
        "training_summary": {
            "train_size": train_size,
            "eval_size": eval_size,
            "num_features": len(weights),
        },
        "evaluation_metrics": metrics,
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(artifact, f, indent=2)

    print(f"Saved model artifact to {output_path}")
    print(json.dumps(artifact, indent=2))


def train_and_save_model(generate_dataset_fn, model_version, output_path, split_seed):
    X, y = generate_dataset_fn()

    X_train, X_eval, y_train, y_eval = train_eval_split(
        X, y, eval_ratio=0.2, seed=split_seed
    )

    bias, weights = fit_linear_regression(X_train, y_train)

    y_eval_pred = predict_linear_regression(X_eval, bias, weights)
    metrics = compute_regression_metrics(y_eval, y_eval_pred)

    save_model_artifact(
        model_name="linear_regression_demo",
        model_version=model_version,
        bias=bias,
        weights=weights,
        metrics=metrics,
        train_size=len(X_train),
        eval_size=len(X_eval),
        output_path=output_path,
    )


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    train_and_save_model(
        generate_dataset_fn=generate_dataset_v1,
        model_version="v1.0.0",
        output_path=os.path.join(script_dir, "model_v1.json"),
        split_seed=1,
    )

    train_and_save_model(
        generate_dataset_fn=generate_dataset_v2,
        model_version="v2.0.0",
        output_path=os.path.join(script_dir, "model_v2.json"),
        split_seed=2,
    )


if __name__ == "__main__":
    main()