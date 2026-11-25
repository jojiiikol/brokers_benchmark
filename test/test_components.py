import argparse
import asyncio
import importlib
import logging
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import AsyncMock, MagicMock, patch

from src.components.common.message_payload import build_message, decode_message
from src.components.common.phase_control import wait_for_release
from src.components.common.producer_metrics import ProducerMetrics, wait_for_result_collection
from src.components.common.load_profile import LoadScheduler, LoadType
from src.components.common.consumer_latency import add_consumer_latency_arguments
from src.docker_configurator import ContainerConfig, build_docker_payload
from src.docker_configurator.schema import Mount, HealthcheckConfig


def kafka_module(role):
    # Unit tests replace the transport, including optional native codecs.
    fake = NS(AIOKafkaProducer=MagicMock(), AIOKafkaConsumer=MagicMock())
    errors = NS(KafkaConnectionError=type('KafkaConnectionError', (Exception,), {}))
    with patch.dict('sys.modules', {'aiokafka': fake, 'aiokafka.errors': errors}):
        return importlib.import_module('src.components.'+role+'.kafka.app')


class ComponentTests(unittest.IsolatedAsyncioTestCase):
    async def test_producer_entrypoints_wire_arguments_and_close(self):
        for broker, name in [('rabbit_mq','Producer'),('kafka','KafkaProducer'),('nats','NatsProducer')]:
            m = kafka_module('producer') if broker == 'kafka' else importlib.import_module('src.components.producer.'+broker+'.app')
            client = NS(connect=AsyncMock(), _connect=AsyncMock(), declare_topology=AsyncMock(),
                        run_benchmark=AsyncMock(), close=AsyncMock())
            with patch.object(m,name,return_value=client), patch.object(m,'start_metrics_server'), \
                 patch.object(m,'setup_logging'), patch.object(m,'wait_for_result_collection',AsyncMock()), \
                 patch('sys.argv',['app','--broker-host','host','--duration-seconds','1','--rps','10','--payload_size','1']):
                await m.main()
            client.run_benchmark.assert_awaited_once()
            client.close.assert_awaited_once()

    async def test_consumer_entrypoints_wire_arguments(self):
        for broker, name, extra in [('kafka','KafkaConsumer',['--group-id','g']),
                                    ('nats','NatsConsumer',['--consumer-name','c','--consumer-mode','pull']),
                                    ('rabbit_mq','Consumer',[])]:
            m = kafka_module('consumer') if broker == 'kafka' else importlib.import_module('src.components.consumer.'+broker+'.app')
            client = NS(connect=AsyncMock(),_connect=AsyncMock(),declare_queue=AsyncMock(),
                        consume=AsyncMock(),close=AsyncMock())
            with patch.object(m,name,return_value=client), patch.object(m,'start_metrics_server'), \
                 patch.object(m,'setup_logging'), patch('sys.argv',['app','--broker-host','host',*extra]):
                await m.main()
            client.consume.assert_awaited_once()

    async def test_observer_exports_resources_and_status_transitions(self):
        from src.observability.prometheus import PrometheusOrchestratorObserver, metrics_enabled
        from src.scenario_service.service import ScenarioStatus
        docker = NS(get_benchmark_container_stats=AsyncMock(return_value=[dict(
            scenario_id='s',component='broker',container_name='b',cpu_seconds=1,memory_bytes=2,
            network_received_bytes=3,network_transmitted_bytes=4,disk_written_bytes=5)]))
        observer = PrometheusOrchestratorObserver(docker)
        scenario = NS(id='s',status=ScenarioStatus.RUNNING)
        await observer.update([scenario],1)
        scenario.status = ScenarioStatus.FINISHED
        await observer.update([scenario],0)
        self.assertEqual(observer._previous_statuses['s'],'FINISHED')
        with patch.dict('os.environ',{'BENCHMARK_METRICS_ENABLED':'false'}):
            self.assertFalse(metrics_enabled())

    async def test_producer_publish_profiles_and_close(self):
        for broker, class_name in [('rabbit_mq','Producer'), ('kafka','KafkaProducer'), ('nats','NatsProducer')]:
            module = kafka_module('producer') if broker == 'kafka' else importlib.import_module('src.components.producer.'+broker+'.app')
            for guarantee in ['at_most_once','at_least_once']:
                kwargs = dict(load_scheduler=LoadScheduler(LoadType.CONSTANT,.001,rps=100), guarantee_type=guarantee)
                if broker != 'rabbit_mq':
                    kwargs.update(broker_host='host',broker_port='123',rps=100,payload_size=1)
                producer = getattr(module,class_name)(**kwargs)
                if broker == 'rabbit_mq':
                    producer._connection = NS(close=AsyncMock())
                    producer._channel = MagicMock()
                    producer._exchange = NS(publish=AsyncMock())
                    producer.MESSAGE_SIZE = 1
                elif broker == 'kafka':
                    producer._producer = NS(send=AsyncMock(),send_and_wait=AsyncMock(),stop=AsyncMock())
                else:
                    producer.nc = NS(publish=AsyncMock(),drain=AsyncMock())
                    producer.js = NS(publish=AsyncMock())
                await producer.run_benchmark(0,.001)
                self.assertGreater(producer._sequence,0)
                body = producer._create_payload('measurement',123)
                self.assertEqual(decode_message(body).sent_at,123)
                if broker == 'rabbit_mq':
                    self.assertGreater(producer._exchange.publish.await_count,0)
                elif broker == 'kafka':
                    call = producer._producer.send_and_wait if guarantee == 'at_least_once' else producer._producer.send
                    self.assertGreater(call.await_count,0)
                else:
                    call = producer.js.publish if guarantee == 'at_least_once' else producer.nc.publish
                    self.assertGreater(call.await_count,0)
                await producer.close()

    async def test_producer_connections_and_rabbit_topologies(self):
        from src.components.producer.rabbit_mq import app as rabbit
        for mode in ['work_queue','pub_sub']:
            p = rabbit.Producer(interaction_mode=mode)
            channel = NS(declare_exchange=AsyncMock(),declare_queue=AsyncMock(),default_exchange='default')
            connection = NS(channel=AsyncMock(return_value=channel))
            with patch.object(rabbit.aio_pika,'connect_robust',AsyncMock(return_value=connection)):
                await p._connect()
                await p._connect()
                await p.declare_topology()
            if mode == 'work_queue':
                self.assertEqual(p._exchange,'default')
            else:
                channel.declare_exchange.assert_awaited_once()
        kafka = kafka_module('producer')
        p = kafka.KafkaProducer('host')
        client = NS(start=AsyncMock())
        with patch.object(kafka,'AIOKafkaProducer',return_value=client):
            await p.connect()
            await p.connect()
        self.assertIs(p._producer,client)
        from src.components.producer.nats import app as nats
        p = nats.NatsProducer('host','4222',1,1,LoadScheduler(LoadType.CONSTANT,1,rps=1))
        js = NS(stream_info=AsyncMock())
        nc = NS(jetstream=lambda:js)
        with patch.object(nats.nats,'connect',AsyncMock(return_value=nc)):
            await p.connect()
        js.stream_info.assert_awaited_once()

    async def test_consumers_count_duplicate_and_sample_latency(self):
        from src.components.consumer.rabbit_mq.app import Consumer
        from src.components.consumer.nats.app import NatsConsumer
        for c in [Consumer(latency_sample_every=1,metrics_window_size=2,metrics_log_every_windows=1),
                  NatsConsumer('host','4222','workers','pull','work_queue',latency_sample_every=1,metrics_window_size=2,metrics_log_every_windows=1)]:
            c.prometheus_metrics = MagicMock()
            observe = getattr(c,'_observe_message',None) or c._observe
            body = build_message(size_bytes=32,phase='measurement',sent_at=time.monotonic_ns(),message_id=1)
            observe(body)
            observe(body)
            observe(b'bad')
            self.assertEqual(c.measurement_messages_total,2)
            c.prometheus_metrics.duplicate.assert_called_once_with('measurement')
            self.assertEqual(c.metrics.count,0)
            c._log_metrics(final=True)

    async def test_kafka_consumer_loop_and_commit(self):
        m = kafka_module('consumer')
        c = m.KafkaConsumer('host','9092','group',guarantee_type='at_least_once',latency_sample_every=1,metrics_window_size=1,metrics_log_every_windows=1)
        body = build_message(size_bytes=32,phase='measurement',sent_at=time.monotonic_ns(),message_id=1)
        async def messages():
            for value in [b'bad',body,body]:
                yield NS(value=value)
        client = MagicMock()
        client.__aiter__.side_effect = messages
        client.start = AsyncMock()
        client.commit = AsyncMock()
        client.stop = AsyncMock()
        with patch.object(m,'AIOKafkaConsumer',return_value=client):
            await c.connect()
            await c.connect()
        await c.consume()
        self.assertEqual(c.measurement_messages_total,2)
        self.assertEqual(client.commit.await_count,2)
        await c.close()
        self.assertIsNone(c._consumer)

    async def test_phase_files_and_metrics_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory,'release')
            path.write_text(str(time.time()-1))
            await wait_for_release(str(path))
            path.write_text('bad')
            with self.assertRaises(RuntimeError):
                await wait_for_release(str(path))
        metrics = ProducerMetrics(logging.getLogger('test'), sink=MagicMock())
        metrics.begin_phase('measurement')
        with self.assertRaises(RuntimeError):
            await metrics.track(1,AsyncMock(side_effect=RuntimeError('publish failed')))
        metrics._sink.failed.assert_called_once_with('measurement')
        await wait_for_result_collection(None,0)
        parser = argparse.ArgumentParser()
        add_consumer_latency_arguments(parser)
        self.assertGreater(parser.parse_args([]).latency_sample_every,0)


class DockerPayloadTests(unittest.TestCase):
    def test_complete_payload_and_validation(self):
        cfg = ContainerConfig(image='image',name='valid',hostname='host',cmd=['run'],entrypoint=['python'],
            env=['KEY=value'],labels={'x':'y'},working_dir='/app',user='1000',dns=['1.1.1.1'],
            network_mode='bridge',stop_timeout=2,ports=[dict(host_port=1234,container_port=80)],
            mounts=[dict(source='/tmp',target='/data',read_only=True)],healthcheck=dict(test='true'),
            resources=dict(cpu_shares=10,memory=10*1024**2,memory_swap=-1,nano_cpus=1000000,pids_limit=5))
        payload,name,_ = build_docker_payload(cfg)
        self.assertEqual(name,'valid')
        self.assertEqual(payload['HostConfig']['MemorySwap'],-1)
        self.assertEqual(payload['Healthcheck']['Interval'],30_000_000_000)
        self.assertEqual(payload['Env'],['KEY=value'])
        for kwargs in [dict(name='invalid!'),dict(env=['bad']),dict(network_mode='host',ports=[dict(host_port=1,container_port=1)]),
                       dict(auto_remove=True,restart_policy=dict(name='always')),
                       dict(ports=[dict(host_port=1,container_port=1)]*2)]:
            with self.assertRaises(ValueError):
                ContainerConfig(image='image',**kwargs)
        for kwargs in [dict(target='/x'),dict(source='/tmp',target='relative')]:
            with self.assertRaises(ValueError):
                Mount(**kwargs)
        self.assertEqual(HealthcheckConfig(test=['CMD','true']).test,['CMD','true'])
        with self.assertRaises(ValueError):
            HealthcheckConfig(test=['bad'])
