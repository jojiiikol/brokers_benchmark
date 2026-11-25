"""Live Docker smoke test. Run with make smoke; excluded from unit discovery."""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import urlencode


ROOT = Path(__file__).resolve().parent.parent


def compose(*args, **kwargs):
    return subprocess.run(['docker', 'compose', *args], cwd=ROOT, check=True, **kwargs)


def request(url, payload=None):
    command = ['curl', '--fail', '--silent', '--show-error', '--max-time', '5', url]
    if payload is not None:
        command += ['--header', 'Content-Type: application/json', '--data', json.dumps(payload)]
    response = subprocess.run(command, check=True, capture_output=True, text=True)
    return json.loads(response.stdout)


def wait_for(label, check, timeout):
    deadline = time.monotonic() + timeout
    last_error = 'condition not satisfied'
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                print(f'OK: {label}', flush=True)
                return result
        except (subprocess.CalledProcessError, ValueError, KeyError) as error:
            last_error = str(error)
        time.sleep(1)
    raise RuntimeError(f'Timeout waiting for {label}: {last_error}')


def query(base_url, expression):
    response = request(base_url + '/api/v1/query?' + urlencode({'query': expression}))
    if response.get('status') != 'success':
        raise RuntimeError(f'Prometheus query failed: {response}')
    return response['data']['result']


def positive_metric(base_url, expression):
    return any(float(item['value'][1]) > 0 for item in query(base_url, expression))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--timeout', type=float, default=240,
                        help='Deadline per readiness/scenario check in seconds')
    parser.add_argument('--api-url', default='http://localhost:8080')
    parser.add_argument('--prometheus-url', default='http://localhost:9090')
    parser.add_argument('--grafana-url', default='http://localhost:3003')
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error('--timeout must be positive')
    scenario_id = None
    completed = False
    try:
        compose('config', '--quiet')
        compose('up', '-d', '--build', timeout=600)
        expected = set(compose('config', '--services', capture_output=True, text=True).stdout.split())

        def services_running():
            running = set(compose('ps', '--status', 'running', '--services',
                                  capture_output=True, text=True).stdout.split())
            return expected <= running

        wait_for('all Compose services running', services_running, args.timeout)
        wait_for('API health', lambda: request(args.api_url+'/health').get('status') == 'ok', args.timeout)
        wait_for('Grafana database health',
                 lambda: request(args.grafana_url+'/api/health').get('database') == 'ok', args.timeout)
        wait_for('Prometheus scrapes orchestrator', lambda: positive_metric(
            args.prometheus_url, 'up{job="orchestrator"} == 1'), args.timeout)
        wait_for('orchestrator metrics collected', lambda: positive_metric(
            args.prometheus_url, 'benchmark_orchestrator_up'), args.timeout)

        scenario = request(args.api_url+'/api/v1/scenarios', {
            'broker': {'broker_type': 'rabbit_mq'},
            'consumer': {'count': 1},
            'producer': {'load_type': 'constant', 'payload_size': 1, 'rps': 20,
                         'count': 1, 'max_in_flight': 10, 'guarantee_type': 'at_least_once'},
            'warmup_seconds': 2, 'duration_seconds': 8, 'drain_seconds': 10,
        })
        scenario_id = scenario['id']
        print(f'Smoke scenario: {scenario_id}', flush=True)

        def scenario_finished():
            current = request(args.api_url+'/api/v1/scenarios/'+scenario_id)
            if current['status'] in {'ERROR', 'CANCELLED'}:
                raise RuntimeError(f'Smoke scenario failed: {current}')
            return current['status'] == 'FINISHED'

        wait_for('SUT scenario FINISHED', scenario_finished, args.timeout)
        # Range query keeps the final scrape visible after dynamic containers exit.
        for metric in ['benchmark_producer_messages_accepted_total',
                       'benchmark_consumer_messages_received_total']:
            expression = f'sum(max_over_time({metric}{{scenario_id="{scenario_id}",phase="measurement"}}[5m]))'
            wait_for(metric, lambda expr=expression: positive_metric(args.prometheus_url, expr), args.timeout)
        completed = True
        print('SMOKE PASSED: services, SUT and live message metrics verified.', flush=True)
        return 0
    except (OSError, subprocess.SubprocessError, RuntimeError, ValueError, KeyError) as error:
        print(f'SMOKE FAILED: {error}', file=sys.stderr, flush=True)
        for command in [('ps', '--all'), ('logs', '--no-color', '--tail', '100')]:
            try:
                compose(*command, timeout=30)
            except (OSError, subprocess.SubprocessError):
                pass
        return 1
    finally:
        if scenario_id and not completed:
            try:
                request(args.api_url+'/api/v1/scenarios/'+scenario_id+'/stop', {})
            except (OSError, subprocess.SubprocessError, ValueError):
                pass


if __name__ == '__main__':
    raise SystemExit(main())
