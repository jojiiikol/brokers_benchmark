"""Low-overhead Prometheus metrics shared by benchmark components."""

from prometheus_client import Counter, Gauge, Histogram, start_http_server

COMPONENT_METRICS_FINAL = Gauge(
    "benchmark_component_metrics_final",
    "Whether the client has stopped updating its benchmark metrics.",
)
CONSUMER_RECEIVED_AT_DRAIN = Gauge(
    "benchmark_consumer_received_at_drain",
    "Measured deliveries processed before the consumer received the drain signal.",
)


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
_CONSUMER_DUPLICATES = Counter(
    "benchmark_consumer_duplicate_messages_total",
    "Benchmark messages delivered more than once to the same consumer.",
    ("broker", "phase"),
)
_CONSUMER_LATENCY = Histogram(
    "benchmark_consumer_latency_seconds",
    "Sampled end-to-end latency from producer timestamp to consumer processing.",
    ("broker",),
    buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30, 60, 300),
)
_CONSUMER_LATENCY_MIN = Gauge(
    "benchmark_consumer_latency_seconds_min",
    "Minimum sampled end-to-end latency in the consumer process.",
    ("broker",),
)
_CONSUMER_LATENCY_MAX = Gauge(
    "benchmark_consumer_latency_seconds_max",
    "Maximum sampled end-to-end latency in the consumer process.",
    ("broker",),
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
        self._duplicates = {
            phase: _CONSUMER_DUPLICATES.labels(broker=broker, phase=phase)
            for phase in ("warmup", "measurement")
        }
        self._latency_min = _CONSUMER_LATENCY_MIN.labels(broker=broker)
        self._latency_max = _CONSUMER_LATENCY_MAX.labels(broker=broker)
        self._minimum_latency: float | None = None
        self._maximum_latency: float | None = None

    def received(self, phase: str) -> None:
        self._received[phase].inc()

    def duplicate(self, phase: str) -> None:
        self._duplicates[phase].inc()

    def observe_latency_seconds(self, latency_seconds: float) -> None:
        self._latency.observe(latency_seconds)
        self._minimum_latency = (
            latency_seconds
            if self._minimum_latency is None
            else min(self._minimum_latency, latency_seconds)
        )
        self._maximum_latency = (
            latency_seconds
            if self._maximum_latency is None
            else max(self._maximum_latency, latency_seconds)
        )
        self._latency_min.set(self._minimum_latency)
        self._latency_max.set(self._maximum_latency)
