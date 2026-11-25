import asyncio
import io
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi.testclient import TestClient
from src.api.app import create_app
from src.orchestrator import Orchestrator
from src.scenario_service.service import ScenarioService, ScenarioStatus
from src.scenario_service.dto.broker_config import Brokers
from src.docker_service.docker_sdk import SdkDockerService
from src.docker_configurator import ContainerConfig, build_docker_payload
from src.results.service import PrometheusScenarioResultsCollector
from test_scenario_lifecycle import scenario_config, FakeResultService


class ApiTests(unittest.TestCase):
    def test_create_list_get_stop_and_validation(self):
        o = Orchestrator()
        o.scenario_service.cancel_scenario = AsyncMock(
            side_effect=lambda s: setattr(s, 'status', ScenarioStatus.CANCELLED))
        payload = dict(broker={'broker_type': 'kafka'}, consumer={'count': 1},
                       producer=dict(load_type='constant', payload_size=1, rps=10))
        with TestClient(create_app(o, manage_runtime=False)) as c:
            self.assertEqual(c.get('/health').json(), {'status': 'ok'})
            response = c.post('/api/v1/scenarios', json=payload)
            self.assertEqual(response.status_code, 202)
            sid = response.json()['id']
            self.assertEqual(len(c.get('/api/v1/scenarios').json()), 1)
            self.assertEqual(c.get('/api/v1/scenarios/'+sid).json()['id'], sid)
            self.assertEqual(c.post('/api/v1/scenarios/'+sid+'/stop').json()['status'], 'CANCELLED')
            self.assertEqual(c.get('/api/v1/scenarios/missing').status_code, 404)
            self.assertEqual(c.post('/api/v1/scenarios/missing/stop').status_code, 404)
            self.assertEqual(c.post('/api/v1/scenarios', json={}).status_code, 400)
            payload['producer']['load_type'] = 'linear'
            self.assertEqual(c.post('/api/v1/scenarios', json=payload).status_code, 400)


class ServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_initialize_all_brokers_and_failure_cleanup(self):
        for broker in Brokers:
            docker = MagicMock()
            docker.create_network = AsyncMock(return_value=NS(connect=AsyncMock()))
            docker.connect_to_observability_network = AsyncMock()
            docker.clear_scenario_resources = AsyncMock()
            service = ScenarioService(docker, FakeResultService())
            cfg = scenario_config()
            cfg.broker_config.broker_type = broker
            scenario = await service.create_scenario(cfg)
            b, p, c = service._get_creators(cfg)
            b.configurate = AsyncMock(return_value='broker')
            p.configurate = AsyncMock(return_value=['producer'])
            c.configurate = AsyncMock(return_value=['consumer'])
            service._validate_scenario = AsyncMock(return_value=True)
            await service.initialize_scenario(scenario)
            self.assertIs(scenario.status, ScenarioStatus.PENDING)
            self.assertEqual(scenario.containers, ['broker', 'producer', 'consumer'])
            self.assertEqual(docker.connect_to_observability_network.await_count, 2)
            service._validate_scenario.return_value = False
            await service.initialize_scenario(scenario)
            self.assertIs(scenario.status, ScenarioStatus.ERROR)
            docker.clear_scenario_resources.assert_awaited_once_with(scenario.id)

    async def test_validation_and_cancellation(self):
        docker = MagicMock()
        docker._get_resources_by_labels = AsyncMock(return_value=['one'])
        docker.clear_scenario_resources = AsyncMock()
        service = ScenarioService(docker, FakeResultService())
        s = await service.create_scenario(scenario_config())
        self.assertTrue(await service._validate_scenario(s))
        docker._get_resources_by_labels.return_value = []
        self.assertFalse(await service._validate_scenario(s))
        await service.cancel_scenario(s)
        self.assertIs(s.status, ScenarioStatus.CANCELLED)
        await service.cancel_scenario(s)
        docker.clear_scenario_resources.assert_awaited_once()

    async def test_failed_start_is_saved_and_cleaned(self):
        docker = NS(start_container=AsyncMock(return_value=False), clear_scenario_resources=AsyncMock())
        result = FakeResultService()
        service = ScenarioService(docker, result)
        s = await service.create_scenario(scenario_config())
        s.container_ids_by_role['broker'] = ['bad']
        await service.start_scenario(s)
        self.assertIs(s.status, ScenarioStatus.ERROR)
        self.assertIn('Failed to start', s.error)
        self.assertEqual(result.saved, [(s.id, 'ERROR')])

    async def test_orchestrator_queue_tasks_and_shutdown_signal(self):
        o = Orchestrator()
        o.docker_service = NS(clear_resources=AsyncMock())
        o.scenario_service.initialize_scenario = AsyncMock()
        o.scenario_service.start_scenario = AsyncMock()
        sid = await o.create_scenario(scenario_config())
        self.assertIn(sid, o.get_info())
        await o._initialize_scenario()
        s = o.get_scenario(sid)
        s.status = ScenarioStatus.PENDING
        self.assertEqual(await o.start_scenario(), sid)
        await asyncio.gather(*o.scenario_tasks.values())
        o._collect_finished_tasks()
        self.assertEqual(o.scenario_tasks, {})
        s.status = ScenarioStatus.FINISHED
        await o._initialize_scenario()
        self.assertIsNone(await o.start_scenario())
        observer = NS(update=AsyncMock(side_effect=RuntimeError('metrics down')))
        o.set_observer(observer)
        await o._notify_observer()
        event = asyncio.Event()
        event.set()
        await o.run(event)
        o.docker_service.clear_resources.assert_awaited_once()
        await o.shutdown()


class DockerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.service = SdkDockerService()
        self.container = NS(id='id', start=AsyncMock(), stop=AsyncMock(), delete=AsyncMock(),
                            show=AsyncMock(return_value={'State': {'Running': True, 'Status': 'running'}}))
        self.network = NS(connect=AsyncMock(), delete=AsyncMock())
        self.service.client = NS(containers=NS(create=AsyncMock(return_value=self.container),
            get=AsyncMock(return_value=self.container), list=AsyncMock(return_value=[self.container])),
            networks=NS(create=AsyncMock(return_value=self.network), get=AsyncMock(return_value=self.network),
                        list=AsyncMock(return_value=[{'Id': 'net'}])), close=AsyncMock())

    async def test_create_start_stop_and_cleanup(self):
        s = self.service
        self.assertEqual(await s.create_container(ContainerConfig(image='image', name='name'), {'scenario_id':'s'}), 'id')
        await s.create_network('net')
        await s.connect_to_observability_network('id')
        self.assertTrue(await s.start_container('id'))
        self.assertEqual(await s.get_list_containers(), [self.container])
        await s.stop_container('id')
        await s.clear_resources()
        await s.clear_scenario_resources('s')
        self.assertEqual(self.container.delete.await_count, 3)
        await s.close_client()
        self.assertIsNone(s.client)

    async def test_missing_and_failed_containers(self):
        s = self.service
        s.client.containers.get.side_effect = RuntimeError('missing')
        self.assertIsNone(await s.get_container('missing'))
        self.assertFalse(await s.start_container('missing'))
        await s.stop_container('missing')
        s.client.containers.create.side_effect = RuntimeError('create failed')
        self.assertIsNone(await s.create_container(ContainerConfig(image='x')))

    async def test_stats_aggregation(self):
        self.container.show.return_value = {'Name':'/broker', 'Config':{'Labels':{'scenario_id':'s','component':'broker'}}}
        self.container.stats = AsyncMock(return_value=[{'cpu_stats':{'cpu_usage':{'total_usage':2_000_000_000}},
            'memory_stats':{'usage':123}, 'networks':{'eth0':{'rx_bytes':5,'tx_bytes':7}},
            'blkio_stats':{'io_service_bytes_recursive':[{'op':'Write','value':11}]}}])
        stats = await self.service.get_benchmark_container_stats()
        self.assertEqual(stats[0]['cpu_seconds'], 2)
        self.assertEqual(stats[0]['disk_written_bytes'], 11)
        self.assertEqual(stats[0]['network_received_bytes'], 5)

    async def test_build_pull_and_file_transfer(self):
        async def logs():
            yield {'stream':'done'}
        s = self.service
        s.client.images = NS(pull=MagicMock(side_effect=lambda *a, **kw: logs()), build=MagicMock(side_effect=lambda **kw: logs()))
        with tempfile.TemporaryDirectory() as directory:
            Path(directory, 'Dockerfile').write_text('FROM scratch')
            self.assertEqual(await s.get_image('tag', 'Dockerfile', Path(directory)), 'tag')
        self.assertEqual(await s.get_image('tag'), 'tag')
        self.assertIsNone(await s.get_image(None))
        self.container.get_archive = AsyncMock(return_value=io.BytesIO(b'data'))
        self.container.put_archive = AsyncMock()
        self.container.kill = AsyncMock()
        await s.wait_for_container_file('id','/tmp/done')
        await s.write_container_file('id','/tmp/start','123')
        await s.signal_container('id','SIGUSR1')
        self.container.put_archive.assert_awaited_once()


class ResultsTests(unittest.IsolatedAsyncioTestCase):
    async def test_collect_aggregates_and_handles_missing_samples(self):
        c = PrometheusScenarioResultsCollector('http://fake')
        c._query = AsyncMock(side_effect=[100, 0, 1024, 90, 2, 10, 1, .01, .2, .1, .15, .3, .4])
        result = await c.collect('s')
        self.assertEqual(result['reliability']['lost_messages'], 12)
        self.assertEqual(result['latency_seconds']['average'], .1)
        self.assertEqual(result['latency_seconds']['p99'], .2)
        c._query = AsyncMock(return_value=None)
        self.assertIsNone((await c.collect('s'))['latency_seconds']['average'])
        self.assertFalse(await c.wait_for_final_metrics('s', 2, .001))
        c._query.return_value = 2
        self.assertTrue(await c.wait_for_final_metrics('s', 2, 1))
        self.assertEqual(await c.received_at_drain('s'), 2)

    async def test_http_responses(self):
        c = PrometheusScenarioResultsCollector('http://fake')
        for value, expected in [('12',12), ('NaN',None), ('invalid',None)]:
            response = MagicMock()
            response.json.return_value = {'data':{'result':[{'value':[0,value]}]}}
            with patch('src.results.service.requests.get', return_value=response):
                self.assertEqual(await c._query('query'), expected)
        response.json.return_value = {'data':{'result':[]}}
        with patch('src.results.service.requests.get', return_value=response):
            self.assertIsNone(await c._query('query'))
