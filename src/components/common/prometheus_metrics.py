"""Low-overhead Prometheus metrics shared by benchmark components."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server


_PRODUCER_ACCEPTED = Counter(
    "benchmark_producer_messages_accepted_total",
    "Messages accepted locally by the producer client.",
    ("broker", "phase"),
)
_PRODUCER_ERRORS = Counter(
    "benchmark_producer_publish_errors_total",
    "Producer publish operations that raised an error.",
    ("broker", "phase"),
)
_PRODUCER_PAYLOAD_BYTES = Counter(
    "benchmark_producer_payload_bytes_total",
    "Payload bytes accepted locally by the producer client.",
    ("broker", "phase"),
)
_PRODUCER_IN_FLIGHT = Gauge(
    "benchmark_producer_in_flight",
    "Producer publish operations currently in flight.",
    ("broker",),
)
_CONSUMER_RECEIVED = Counter(
    "benchmark_consumer_messages_received_total",
    "Valid benchmark messages processed by the consumer.",
    ("broker", "phase"),
)
_CONSUMER_LATENCY = Histogram(
    "benchmark_consumer_latency_seconds",
    "Sampled end-to-end latency from producer timestamp to consumer processing.",
    ("broker",),
    buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300),
)


def start_metrics_server(port: int = 8000) -> None:
    """Expose the process registry to Prometheus on all container interfaces."""
    start_http_server(port, addr="0.0.0.0")


class ProducerPrometheusMetrics:
    def __init__(self, broker: str) -> None:
        self._broker = broker
        self._in_flight = _PRODUCER_IN_FLIGHT.labels(broker=broker)
        self._accepted = {
            phase: _PRODUCER_ACCEPTED.labels(broker=broker, phase=phase)
            for phase in ("warmup", "measurement")
        }
        self._errors = {
            phase: _PRODUCER_ERRORS.labels(broker=broker, phase=phase)
            for phase in ("warmup", "measurement")
        }
        self._payload_bytes = {
            phase: _PRODUCER_PAYLOAD_BYTES.labels(broker=broker, phase=phase)
            for phase in ("warmup", "measurement")
        }

    def accepted(self, phase: str, payload_bytes: int) -> None:
        self._accepted[phase].inc()
        self._payload_bytes[phase].inc(payload_bytes)

    def failed(self, phase: str) -> None:
        self._errors[phase].inc()

    def set_in_flight(self, count: int) -> None:
        self._in_flight.set(count)


class ConsumerPrometheusMetrics:
    def __init__(self, broker: str) -> None:
        self._received = {
            phase: _CONSUMER_RECEIVED.labels(broker=broker, phase=phase)
            for phase in ("warmup", "measurement")
        }
        self._latency = _CONSUMER_LATENCY.labels(broker=broker)

    def received(self, phase: str) -> None:
        self._received[phase].inc()

    def observe_latency_seconds(self, latency_seconds: float) -> None:
        self._latency.observe(latency_seconds)
