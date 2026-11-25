import asyncio
import random
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from src.components.common.message_ids import MessageIdTracker
from src.components.common.message_payload import build_message
from src.components.consumer.kafka.app import KafkaConsumer
from src.components.consumer.nats.pull import StreamingPullReader
from src.components.common.prometheus_metrics import ProducerPrometheusMetrics, ConsumerPrometheusMetrics
from src.components.producer.nats.app import NatsProducer
from src.components.common.load_profile import LoadScheduler, LoadType, publish_with_profile
from src.results.service import PrometheusScenarioResultsCollector
from src.api.models import ScenarioCreateRequest
from src.scenario_service.dto.interaction_mode import InteractionMode
from src.docker_service.docker_sdk import SdkDockerService


class MessageIdTests(unittest.TestCase):
    def test_buffered_counters_flush_exactly_once_per_phase(self):
        producer = ProducerPrometheusMetrics('buffered-counter-test')
        consumer = ConsumerPrometheusMetrics('buffered-counter-test')
        for phase, count in [('warmup',7),('measurement',13)]:
            sent_before = producer._accepted[phase]._value.get()
            received_before = consumer._received[phase]._value.get()
            for _ in range(count):
                producer.accepted(phase,16384)
                consumer.received(phase)
            producer.flush();consumer.flush()
            self.assertEqual(producer._accepted[phase]._value.get()-sent_before,count)
            self.assertEqual(consumer._received[phase]._value.get()-received_before,count)
            producer.flush();consumer.flush()
            self.assertEqual(producer._accepted[phase]._value.get()-sent_before,count)

    def test_gaps_out_of_order_and_duplicate_sequences_are_exact(self):
        tracker = MessageIdTracker()
        expected = set()
        rng = random.Random(42)
        ids = [(p << 48) | n for p in range(1,4) for n in range(1,2000)]
        rng.shuffle(ids)
        for message_id in ids + ids[::2]:
            self.assertEqual(tracker.seen(message_id), message_id in expected)
            expected.add(message_id)
        self.assertEqual(len(tracker.ranges),3)
        self.assertTrue(all(pair == ([1],[1999]) for pair in tracker.ranges.values()))

    def test_missing_messages_are_not_invented(self):
        tracker = MessageIdTracker()
        for n in [1,3,7,8,10]:
            self.assertFalse(tracker.seen(n))
        self.assertFalse(tracker.seen(2))
        self.assertTrue(tracker.seen(3))
        self.assertFalse(tracker.seen(9))
        self.assertTrue(tracker.seen(8))

    def test_contiguous_history_has_constant_storage(self):
        tracker = MessageIdTracker()
        for n in range(100000):
            self.assertFalse(tracker.seen((1 << 48) | n))
        self.assertEqual(tracker.ranges, {1:([0],[99999])})


class KafkaBatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_high_rps_is_not_limited_to_one_message_per_timer_tick(self):
        now = 0
        published = 0
        real_sleep = asyncio.sleep

        async def sleep(delay):
            nonlocal now
            # Simulate a timer that wakes only on millisecond boundaries.
            now += max(.001, round(delay * 1000 + .5) / 1000)
            await real_sleep(0)

        async def publish(_step):
            nonlocal published
            published += 1

        with patch('src.components.common.load_profile.monotonic', side_effect=lambda: now), \
                patch('src.components.common.load_profile.asyncio.sleep', side_effect=sleep):
            await publish_with_profile(
                LoadScheduler(LoadType.CONSTANT, .1, rps=10000), .1,
                publish, lambda _step: None, max_in_flight=64)
        # The last incomplete timer tick is deliberately not replayed after
        # the deadline, so allow its small boundary error.
        self.assertGreaterEqual(published, 950)
        self.assertLessEqual(published, 1000)

    async def test_cleanup_removes_owned_anonymous_volumes(self):
        service = SdkDockerService()
        service.client = SimpleNamespace(containers=object(),networks=object())
        container = SimpleNamespace(id='owned-container',delete=AsyncMock())
        service._get_resources_by_labels = AsyncMock(side_effect=[[container],[]])
        await service.clear_scenario_resources('owned-scenario')
        container.delete.assert_awaited_once_with(force=True,v=True)

    async def test_nats_publication_still_waits_for_puback(self):
        producer = NatsProducer('nats','4222',None,16,LoadScheduler(LoadType.MAX_THROUGHPUT,1),
                                guarantee_type='at_least_once')
        future = asyncio.get_running_loop().create_future()
        async def confirmed(*args):
            return await future
        producer.js = SimpleNamespace(publish=AsyncMock(side_effect=confirmed))
        task = asyncio.create_task(producer.publish('measurement'))
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        future.set_result(SimpleNamespace(stream='SCENARIO',seq=1))
        await task
        self.assertEqual(len(producer.js.publish.call_args.args[1]),16*1024)

    async def test_warmup_requires_all_producers_and_unique_deliveries(self):
        config = ScenarioCreateRequest.model_validate(dict(
            broker=dict(broker_type='kafka'), producer=dict(load_type='constant',rps=1,payload_size=1,count=2,
                                                           guarantee_type='at_least_once'),
            consumer=dict(count=1)))
        scenario = SimpleNamespace(id='warmup-test',config=config.to_domain())
        collector = PrometheusScenarioResultsCollector()
        async def value(query):
            if query.startswith('count('):return 2
            if 'warmup_messages' in query:return 10
            if 'duplicate' in query:return 0
            return 10
        collector._query = value
        with patch.dict('os.environ', {'BENCHMARK_METRICS_ENABLED':'true'}):
            self.assertTrue(await collector.wait_for_warmup(scenario,.1))
            scenario.config.interaction_mode = InteractionMode.PUB_SUB
            scenario.config.consumer_config.count=2
            self.assertFalse(await collector.wait_for_warmup(scenario,.001))

    async def test_minimal_guarantee_does_not_require_lossless_warmup(self):
        config = ScenarioCreateRequest.model_validate(dict(
            broker=dict(broker_type='nats_pull'),
            consumer=dict(count=1),
            producer=dict(load_type='constant',rps=1,payload_size=1,guarantee_type='at_most_once')))
        collector = PrometheusScenarioResultsCollector()
        collector._query = AsyncMock(return_value=0)
        scenario = SimpleNamespace(id='lossy-warmup',config=config.to_domain())
        with patch.dict('os.environ', {'BENCHMARK_METRICS_ENABLED':'true'}):
            self.assertIsNone(await collector.wait_for_warmup(scenario,1))
        collector._query.assert_not_awaited()

    async def test_commit_only_processed_batch_offsets_after_observation(self):
        consumer = KafkaConsumer('kafka','9092','workers',guarantee_type='at_least_once')
        committed = asyncio.Event()
        batch = [SimpleNamespace(value=build_message(size_bytes=64,phase='measurement',sent_at=1,message_id=i),offset=i)
                 for i in range(3)]
        fetched = False
        async def getmany(**kwargs):
            nonlocal fetched
            if fetched:
                await asyncio.Event().wait()
            fetched=True
            return {'partition-0':batch}
        async def commit(offsets):
            self.assertEqual(consumer.measurement_messages_total,3)
            self.assertEqual(offsets,{'partition-0':3})
            committed.set()
        client=SimpleNamespace(assignment=lambda:{'partition-0'},position=AsyncMock(return_value=0),
                               getmany=getmany,commit=AsyncMock(side_effect=commit))
        consumer._consumer=client
        with patch('src.components.consumer.kafka.app.Path.touch'):
            task=asyncio.create_task(consumer.consume())
            await asyncio.wait_for(committed.wait(),1)
            task.cancel();await asyncio.gather(task,return_exceptions=True)
        client.commit.assert_awaited_once()

    async def test_minimal_mode_does_not_add_manual_commits(self):
        consumer = KafkaConsumer('kafka','9092','workers',guarantee_type='at_most_once')
        consumer._consumer = SimpleNamespace(assignment=lambda:{'p'},position=AsyncMock(return_value=0),
                                             getmany=AsyncMock(return_value={}),commit=AsyncMock())
        with patch('src.components.consumer.kafka.app.Path.touch'):
            task=asyncio.create_task(consumer.consume())
            # Empty batches must yield to timers as well as populated batches.
            await asyncio.sleep(.01)
            task.cancel();await asyncio.gather(task,return_exceptions=True)
        consumer._consumer.commit.assert_not_awaited()


class NatsPullTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ready=asyncio.Event()
        self.sub=SimpleNamespace(unsubscribe=AsyncMock())
        self.connection=SimpleNamespace(new_inbox=lambda:'inbox.test',publish=AsyncMock(),flush=AsyncMock())
        async def subscribe(inbox,**kwargs):
            self.handler=kwargs['cb'];self.subscription_options=kwargs
            return self.sub
        self.connection.subscribe=subscribe
        self.js=SimpleNamespace(add_consumer=AsyncMock(return_value=SimpleNamespace(name='workers')))
        self.observed=[]
        self.reader=StreamingPullReader(self.connection,self.js,'SCENARIO',object(),self.observed.append)
        self.task=asyncio.create_task(self.reader.run(self.ready.set))
        await asyncio.wait_for(self.ready.wait(),1)

    async def asyncTearDown(self):
        self.task.cancel();await asyncio.gather(self.task,return_exceptions=True)

    async def test_processes_first_message_without_waiting_for_batch_to_fill(self):
        ack=AsyncMock()
        await self.handler(SimpleNamespace(subject=self.reader.reply,headers=None,data=b'first',ack=ack))
        self.assertEqual(self.observed,[b'first'])
        ack.assert_awaited_once()
        self.assertEqual(self.connection.publish.await_count,1)
        self.assertEqual(self.reader.remaining,127)

    async def test_expiration_and_byte_limit_replenish_without_leaking_requests(self):
        for status,description in [('408','Request Timeout'),('409','Batch Completed')]:
            await self.handler(SimpleNamespace(subject=self.reader.reply,headers={'Status':status,'Description':description}))
        self.assertEqual(self.connection.publish.await_count,3)
        self.assertEqual(self.subscription_options['pending_bytes_limit'],8*1024*1024)
        self.assertEqual(self.reader.remaining,self.reader.BATCH_SIZE)

    async def test_complete_window_requests_next_and_preserves_each_ack(self):
        ack=AsyncMock()
        old_reply = self.reader.reply
        for _ in range(self.reader.BATCH_SIZE):
            await self.handler(SimpleNamespace(subject=self.reader.reply,headers=None,data=b'x',ack=ack))
        self.assertEqual(ack.await_count,128)
        self.assertEqual(self.connection.publish.await_count,2)
        await self.handler(SimpleNamespace(subject=old_reply,headers={'Status':'409','Description':'Batch Completed'}))
        self.assertEqual(self.connection.publish.await_count,2)

    async def test_errors_propagate_instead_of_silently_hanging(self):
        await self.handler(SimpleNamespace(subject=self.reader.reply,headers={'Status':'503','Description':'Unavailable'}))
        with self.assertRaises(RuntimeError):
            await self.task
        self.sub.unsubscribe.assert_awaited_once()

    async def test_missing_terminal_status_restarts_expired_request(self):
        old_reply = self.reader.reply
        await asyncio.sleep(self.reader.REQUEST_TIMEOUT_SECONDS + .1)
        self.assertNotEqual(old_reply, self.reader.reply)
        self.assertEqual(self.connection.publish.await_count, 2)
        # An old reply arriving after recovery must not create a second pull.
        await self.handler(SimpleNamespace(subject=old_reply, headers={'Status':'408'}))
        self.assertEqual(self.connection.publish.await_count, 2)
