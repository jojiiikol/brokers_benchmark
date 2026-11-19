import asyncio
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import httpx
import numpy as np

from src.api.app import create_app
from src.experiments.models import ExperimentSpecification, build_plan
from src.experiments.service import ExperimentService
from src.experiments.service import analyze_directory
from src.experiments.statistics import analyze, cliffs_delta, compare, describe, effect_category, holm, rng_for
from src.experiments.storage import atomic_json, read_json
from src.orchestrator import Orchestrator
from src.scenario_service.service import ScenarioStatus


def specification(**overrides):
    data = dict(name="scaling", experiment_type="producer_scaling",
                brokers=["rabbit_mq", "kafka"], values=[1, 2], repetitions=2, seed=42,
                scenario=dict(producer=dict(count=1, load_type="constant", rps=1000,
                                            payload_size=1, guarantee_type="at_most_once"),
                              consumer=dict(count=1), warmup_seconds=0,
                              duration_seconds=5, drain_seconds=1))
    data.update(overrides)
    return ExperimentSpecification.model_validate(data)


class PlannerTests(unittest.TestCase):
    def test_full_matrix_and_deterministic_order(self):
        spec = specification(brokers=["rabbit_mq", "kafka", "nats_pull", "nats_push"],
                             values=[1,2,4,8], repetitions=4)
        plan = build_plan(spec)
        self.assertEqual(plan["total_runs"], 64)
        self.assertEqual(plan, build_plan(spec))
        self.assertEqual(len({r["run_id"] for r in plan["runs"]}), 64)
        self.assertTrue(all(c["scenario"]["producer"]["rps"] == 1000 for c in plan["configurations"]))
        for i in range(4):
            self.assertEqual({r["repetition"] for r in plan["runs"][i*16:(i+1)*16]}, {i+1})

    def test_every_type_modifies_its_own_field(self):
        for kind, role, key in [("producer_scaling", "producer", "count"),
                                ("consumer_scaling", "consumer", "count"),
                                ("message_size", "producer", "payload_size"),
                                ("pub_sub", "consumer", "count")]:
            data = specification().model_dump(mode="json")
            data["experiment_type"] = kind
            if kind == "pub_sub":
                data["scenario"]["interaction_mode"] = "pub_sub"
            plan = build_plan(ExperimentSpecification.model_validate(data))
            self.assertEqual([c["scenario"][role][key] for c in plan["configurations"][:2]], [1,2])

    def test_rejects_invalid_plan_before_resources(self):
        for overrides in (dict(values=[1,1]), dict(values=[0]), dict(values=[]),
                          dict(brokers=["kafka", "kafka"]), dict(experiment_type="guarantees"),
                          dict(experiment_type="consumer_scaling", values=[17])):
            with self.assertRaises(ValueError):
                specification(**overrides)

    def test_ramp_is_whole_run_not_cartesian_steps(self):
        data = specification().model_dump(mode="json")
        data.update(experiment_type="load_ramp", values=None,
                    ramp=dict(start_rps=100, end_rps=1000, step_duration_seconds=20))
        data["scenario"]["duration_seconds"] = 60
        plan = build_plan(ExperimentSpecification.model_validate(data))
        self.assertEqual(plan["total_runs"], 4)
        self.assertEqual(plan["configurations"][0]["scenario"]["producer"]["load_type"], "linear")
        data["scenario"]["duration_seconds"] = 61
        with self.assertRaises(ValueError):
            ExperimentSpecification.model_validate(data)


class StatisticsTests(unittest.TestCase):
    def test_quartiles_and_reproducible_bootstrap(self):
        a = describe([1,2,3,4], rng_for(42, "test"))
        self.assertEqual((a["median"], a["q1"], a["q3"], a["iqr"]), (2.5,1.75,3.25,1.5))
        self.assertEqual(a, describe([1,2,3,4], rng_for(42,"test")))
        self.assertIsNone(describe([1], rng_for(1,"one"))["mean_ci95"])

    def test_effect_sign_and_boundaries(self):
        self.assertEqual(cliffs_delta([3,4], [1,2]), 1)
        self.assertEqual(cliffs_delta([1,2], [3,4]), -1)
        self.assertEqual(cliffs_delta([1,2], [1,2]), 0)
        self.assertEqual([effect_category(x) for x in [.146,.147,.329,.33,.473,.474]],
                         ["negligible","small","small","medium","medium","large"])

    def test_mann_whitney_exact_ties_constants_and_small_n(self):
        exact = compare([1,2], [3,4], rng_for(1,"exact"), resamples=200)
        self.assertEqual(exact["method"], "exact")
        self.assertAlmostEqual(exact["p_value"], 1/3)
        tied = compare([1,2], [2,3], rng_for(1,"ties"), resamples=200)
        self.assertEqual(tied["method"], "asymptotic")
        constant = compare([1,1], [1,1], rng_for(1,"same"))
        self.assertEqual((constant["p_value"], constant["cliffs_delta"], constant["delta_ci95"]), (1,0,[0,0]))
        self.assertIsNone(compare([1], [2,3], rng_for(1,"small"))["p_value"])

    def test_holm_adjustment(self):
        np.testing.assert_allclose(holm([.01,.04,.03]), [.03,.06,.06])

    def test_groups_and_exclusions_not_message_samples(self):
        spec = specification(values=[1], repetitions=2)
        plan = build_plan(spec)
        records = []
        for run in plan["runs"]:
            artifact = dict(scenario=dict(status="FINISHED"), metrics=dict(
                latency_seconds=dict(average=1, samples=100000), quality=dict(final_metrics_collected=True)))
            records.append(dict(run, status="finished", artifact=artifact))
        records[0]["artifact"]["metrics"]["quality"]["final_metrics_collected"] = False
        summary, comparisons, _ = analyze(spec.model_dump(mode="json"), plan, records)
        counts = [g["metrics"]["latency_seconds.average"]["n"] for g in summary["groups"]]
        self.assertEqual(sorted(counts), [1,2])
        self.assertTrue(any(c.get("reason") for c in comparisons["comparisons"]))


class SchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def test_step_snapshots_are_differences_without_per_message_step_work(self):
        from src.components.common.phase_control import measurement_snapshot
        from src.components.common.prometheus_metrics import STEP_MESSAGES, STEP_BYTES, STEP_LATENCY_COUNT
        counters = iter([(200,204800), (600,614400)])
        latency = iter([{("count", ""):20,("sum", ""): .02,("bucket","+Inf"):20},
                        {("count", ""):60,("sum", ""): .06,("bucket","+Inf"):60}])
        with patch("src.components.common.phase_control.wait_for_release", return_value=None), \
             patch("src.components.common.phase_control.Path.read_text", return_value="0"), \
             patch("src.components.common.phase_control.time.time", return_value=0):
            await measurement_snapshot(20, lambda: next(counters), step_duration=10,
                                       latency=lambda: next(latency))
        self.assertEqual(STEP_MESSAGES.labels("0")._value.get(),200)
        self.assertEqual(STEP_MESSAGES.labels("1")._value.get(),400)
        self.assertEqual(STEP_BYTES.labels("1")._value.get(),409600)
        self.assertEqual(STEP_LATENCY_COUNT.labels("1")._value.get(),40)

    async def test_capacity_reserved_before_initialization_and_cancel_releases_it(self):
        orchestrator = Orchestrator(max_concurrent_scenarios=2)
        entered, release = asyncio.Event(), asyncio.Event()
        cleaned = []

        async def initialize(scenario):
            entered.set()
            await release.wait()
            scenario.status = ScenarioStatus.PENDING

        async def clean(identifier):
            cleaned.append(identifier)

        orchestrator.scenario_service.initialize_scenario = initialize
        orchestrator.docker_service.clear_scenario_resources = clean
        config = specification().scenario_request(specification().brokers[0],1).to_domain()
        ids = [await orchestrator.create_scenario(config) for _ in range(3)]
        self.assertEqual(await orchestrator.start_scenario(), ids[0])
        self.assertEqual(await orchestrator.start_scenario(), ids[1])
        self.assertIsNone(await orchestrator.start_scenario())
        await entered.wait()
        await orchestrator.stop_scenario(ids[0])
        self.assertIn(ids[0], cleaned)
        self.assertEqual(await orchestrator.start_scenario(), ids[2])
        await asyncio.sleep(0)
        await orchestrator.stop_scenario(ids[1])
        await orchestrator.stop_scenario(ids[2])


class SeriesTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.orchestrator = Orchestrator(max_concurrent_scenarios=2)
        self.service = ExperimentService(self.orchestrator, self.root / "experiments")
        self.cleaned = []
        self.run_count = 0

        async def initialize(scenario):
            scenario.status = ScenarioStatus.PENDING

        async def start(scenario):
            self.run_count += 1
            scenario.status = ScenarioStatus.RUNNING
            await asyncio.sleep(.01)
            artifact = dict(scenario=dict(status="FINISHED"), experiment=scenario.experiment,
                            metrics=dict(latency_seconds=dict(average=.01, p50=.01,p75=.01,p95=.02,p99=.02),
                                         rates=dict(producer_messages_per_second=1000,
                                                    consumer_messages_per_second=1000,
                                                    producer_payload_bytes_per_second=1024000),
                                         resources={role:dict(cpu_cores_average=.1,cpu_cores_maximum=.2,
                                                              memory_bytes_average=1024,memory_bytes_maximum=2048)
                                                    for role in ("broker","producer","consumer")},
                                         quality=dict(final_metrics_collected=True)))
            target = self.root / f"scenario-{scenario.id}.json"
            atomic_json(target, artifact)
            scenario.result_path = str(target)
            scenario.status = ScenarioStatus.FINISHED

        async def clean(identifier): self.cleaned.append(identifier)
        async def cancel(scenario):
            scenario.status = ScenarioStatus.CANCELLED
            await clean(scenario.id)
        self.orchestrator.scenario_service.initialize_scenario = initialize
        self.orchestrator.scenario_service.start_scenario = start
        self.orchestrator.scenario_service.cancel_scenario = cancel
        self.orchestrator.docker_service.clear_scenario_resources = clean

    async def asyncTearDown(self):
        await self.service.close()
        self.temp.cleanup()

    async def pump(self, state):
        async with asyncio.timeout(15):
            while state["status"] in ("planned", "running", "analyzing"):
                self.orchestrator._collect_finished_tasks()
                while self.orchestrator._check_run_available:
                    if await self.orchestrator.start_scenario() is None: break
                await asyncio.sleep(.01)

    async def test_series_persists_and_reanalyzes_offline(self):
        state = await self.service.create(specification(values=[1]))
        await self.pump(state)
        self.assertEqual(self.run_count, 4)
        self.assertEqual(state["status"], "finished")
        self.assertEqual(len(self.service.results(state["id"])["summary"]["groups"]), 2)
        self.assertTrue((self.root / "experiments" / state["id"] / "runs.csv").exists())
        repeated = await asyncio.to_thread(self.service.analyze, state["id"])
        self.assertEqual(repeated, self.service.results(state["id"]))
        await self.service.resume(state["id"])
        await self.pump(state)
        self.assertEqual(self.run_count, 4)

    async def test_failure_is_not_retried_automatically(self):
        async def fail(scenario):
            self.run_count += 1
            scenario.status = ScenarioStatus.ERROR
            scenario.error = "test failure"
        self.orchestrator.scenario_service.start_scenario = fail
        state = await self.service.create(specification(values=[1], repetitions=1))
        await self.pump(state)
        self.assertEqual(state["status"], "partial")
        self.assertEqual(self.run_count, 2)
        self.assertTrue(all(len(r["attempts"]) == 1 for r in state["runs"]))

    async def test_api_preview_rejects_unsupported_and_cli_uses_same_plan(self):
        app = create_app(self.orchestrator, manage_runtime=False, experiment_service=self.service)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/api/v1/experiments/plan", json=specification().model_dump(mode="json"))
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["total_runs"], 8)
            invalid = specification().model_dump(mode="json")
            invalid["experiment_type"] = "guarantees"
            self.assertEqual((await client.post("/api/v1/experiments",json=invalid)).status_code,400)
            self.assertEqual((await client.get("/api/v1/experiments/missing")).status_code,404)

    async def test_api_creates_series_and_returns_results(self):
        app = create_app(self.orchestrator, manage_runtime=False, experiment_service=self.service)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            response = await client.post("/api/v1/experiments", json=specification(values=[1]).model_dump(mode="json"))
            self.assertEqual(response.status_code, 202)
            identifier = response.json()["id"]
            self.assertEqual((await client.get(f"/api/v1/experiments/{identifier}/results")).status_code,409)
            await self.pump(self.service.get(identifier))
            self.assertEqual((await client.get(f"/api/v1/experiments/{identifier}/results")).status_code,200)
            self.assertEqual((await client.post(f"/api/v1/experiments/{identifier}/analyze")).status_code,200)
            cancelled = await client.post(f"/api/v1/experiments/{identifier}/cancel")
            self.assertEqual(cancelled.json()["status"], "finished")

    async def test_restart_marks_interrupted_and_preserves_completed(self):
        state = await self.service.create(specification(values=[1], repetitions=1))
        await asyncio.sleep(0)
        await self.service.close()
        original_ids = [r["attempts"][-1]["scenario_id"] for r in state["runs"]]
        replacement = ExperimentService(self.orchestrator, self.root / "experiments")
        replacement.initialize()
        recovered = replacement.get(state["id"])
        self.assertEqual(recovered["status"], "partial")
        self.assertTrue(all(r["status"] == "interrupted" for r in recovered["runs"]))
        await replacement.resume(state["id"])
        await self.pump(recovered)
        self.assertTrue(set(original_ids).issubset(self.cleaned))
        self.assertTrue(all(len(r["attempts"]) == 2 for r in recovered["runs"]))
        await replacement.close()

    async def test_recovers_saved_success_without_rerun_and_cleans_leftovers(self):
        state = await self.service.create(specification(values=[1], repetitions=1))
        await asyncio.sleep(0)
        await self.service.close()
        run = state["runs"][0]
        attempt = run["attempts"][-1]
        atomic_json(self.root / f"scenario-{attempt['scenario_id']}.json", dict(scenario=dict(status="FINISHED"), metrics={}))
        run["status"] = attempt["status"] = "running"
        atomic_json(self.root / "experiments" / state["id"] / "progress.json", state)
        replacement = ExperimentService(self.orchestrator, self.root / "experiments")
        replacement.initialize()
        recovered = replacement.get(state["id"])
        self.assertEqual(recovered["runs"][0]["status"], "finished")
        await replacement.resume(state["id"])
        await self.pump(recovered)
        self.assertEqual(self.run_count, 1)
        self.assertIn(attempt["scenario_id"], self.cleaned)
        self.assertEqual(len(recovered["runs"][0]["attempts"]), 1)
        await replacement.close()

    async def test_single_directory_owner_and_offline_active_guard(self):
        state = await self.service.create(specification(values=[1], repetitions=1))
        other = ExperimentService(self.orchestrator, self.root / "experiments")
        with self.assertRaises(RuntimeError):
            other.initialize()
        with self.assertRaises(ValueError):
            analyze_directory(self.root / "experiments" / state["id"])

    def test_cli_sends_plan_to_api(self):
        from src.cli import main
        path = self.root / "spec.json"
        atomic_json(path, specification().model_dump(mode="json"))
        with patch("httpx.Client") as client_type, patch("builtins.print"):
            client = client_type.return_value.__enter__.return_value
            client.post.return_value = httpx.Response(200, json={"total_runs":8},
                                                      request=httpx.Request("POST","http://test"))
            main(["experiments", "--api-url", "http://test", "plan", "--file", str(path)])
            self.assertEqual(client.post.call_args.args, ("http://test/api/v1/experiments/plan",))


if __name__ == "__main__":
    unittest.main()
