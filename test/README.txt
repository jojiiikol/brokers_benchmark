Unit tests
==========

Live smoke test (requires Docker Compose, curl, Python 3 and a running Docker daemon):
  make smoke

Builds and starts all default Compose services, checks API and Grafana health,
checks the Prometheus orchestrator scrape, submits a small RabbitMQ scenario,
waits for FINISHED and verifies nonzero measurement producer/consumer counters.
Returns nonzero and prints Compose status/logs on failure. On failure it requests
cleanup of only its own scenario. The stack and volumes are left running.
The first run may take several minutes to download and build component images.
Customize URLs or the per-check deadline:
  python3 test/smoke.py --timeout 360 --api-url http://localhost:8080

Install test dependencies from the repository root:
  .venv/bin/python -m pip install -r requirements-test.txt

Run tests and enforce at least 80% statement coverage:
  .venv/bin/python test/run.py

Reports:
  Terminal: per-module coverage and missing line numbers
  htmlcov/index.html: interactive line coverage
  coverage.xml: machine-readable report for CI

The measured source is the entire src directory, including namespace packages.
No application modules are excluded to reach the threshold. Tests and the
root-level manual main.py launcher are outside this measurement.

Tests use unittest and mock/AsyncMock for Docker, HTTP and broker transports.
No Docker daemon, Prometheus server, or message broker is required.
Kafka imports use a fake transport so optional native codecs are not required.

test_scenario_lifecycle.py preserves the previous tests/ suite.
test_services.py checks API, orchestration, Docker operations and artifacts.
test_components.py checks adapters, entrypoints, metrics and Docker payloads.
