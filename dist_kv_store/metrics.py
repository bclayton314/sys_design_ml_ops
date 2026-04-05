import time
from collections import defaultdict


class Metrics:
    def __init__(self):
        self.counters = defaultdict(int)
        self.latencies = defaultdict(list)

    def inc(self, name: str):
        self.counters[name] += 1

    def observe_latency(self, name: str, value: float):
        self.latencies[name].append(value)

    def summary(self):
        latency_summary = {}

        for key, values in self.latencies.items():
            if not values:
                continue

            latency_summary[key] = {
                "count": len(values),
                "avg_ms": sum(values) / len(values),
                "max_ms": max(values),
                "min_ms": min(values),
            }

        return {
            "counters": dict(self.counters),
            "latencies": latency_summary,
        }


metrics = Metrics()


class Timer:
    def __init__(self, metric_name: str):
        self.metric_name = metric_name

    def __enter__(self):
        self.start = time.time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = (time.time() - self.start) * 1000
        metrics.observe_latency(self.metric_name, elapsed)