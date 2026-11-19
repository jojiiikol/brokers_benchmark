"""Durable experiment execution using the application's shared orchestrator."""

import asyncio
import csv
import datetime as dt
import fcntl
import io
import json
import logging
import os
from pathlib import Path
import uuid

from src.api.models import ScenarioCreateRequest
from src.experiments.models import ExperimentSpecification, build_plan
from src.experiments.storage import atomic_json, read_json

logger = logging.getLogger(__name__)
TERMINAL = {"FINISHED", "ERROR", "CANCELLED"}


class ExperimentNotFound(KeyError):
    pass


def now():
    return dt.datetime.now(dt.UTC).isoformat()


class ExperimentService:
    def __init__(self, orchestrator, directory: Path | None = None):
        self.orchestrator = orchestrator
        self.directory = directory or Path(os.getenv("BENCHMARK_RESULTS_DIR", "/app/results")) / "experiments"
        self.experiments = {}
        self.tasks = {}
        self._lock = asyncio.Lock()
        self._owner = None
        self.orchestrator.experiment_provider = lambda: self.experiments

    def initialize(self):
        if self._owner is not None:
            return
        self.directory.mkdir(parents=True, exist_ok=True)
        owner = (self.directory / ".owner.lock").open("a+")
        try:
            fcntl.flock(owner.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            owner.close()
            raise RuntimeError("Another process owns this experiment results directory")
        self._owner = owner
        for path in self.directory.glob("*/progress.json"):
            state = read_json(path)
            for run in state["runs"]:
                if run["status"] in ("queued", "running", "interrupted"):
                    if not self._recover_finished(state, run):
                        run["attempts"][-1]["status"] = run["status"] = "interrupted"
            if state["status"] in ("running", "analyzing"):
                state["status"] = "partial"
            self.experiments[state["id"]] = state
            self._save(state)

    def _recover_finished(self, state, run):
        if not run["attempts"]:
            return False
        folder = self.directory / state["id"]
        attempt = run["attempts"][-1]
        target = folder / "runs" / f"{attempt['attempt_id']}.json"
        source = target if target.exists() else self.directory.parent / f"scenario-{attempt['scenario_id']}.json"
        if not source.exists():
            return False
        artifact = read_json(source)
        if artifact.get("scenario", {}).get("status") != "FINISHED":
            return False
        atomic_json(target, artifact)
        attempt.update(status="finished", result=str(target.relative_to(folder)), cleanup_required=True)
        run["status"] = "finished"
        return True

    def _save(self, state):
        state["updated_at"] = now()
        atomic_json(self.directory / state["id"] / "progress.json", state)

    def get(self, experiment_id):
        try:
            return self.experiments[experiment_id]
        except KeyError as error:
            raise ExperimentNotFound(experiment_id) from error

    async def create(self, spec: ExperimentSpecification):
        async with self._lock:
            self.initialize()
            plan = build_plan(spec)
            identifier = str(uuid.uuid4())
            folder = self.directory / identifier
            atomic_json(folder / "specification.json", spec.model_dump(mode="json"))
            atomic_json(folder / "plan.json", plan)
            state = dict(id=identifier, name=spec.name, status="planned", created_at=now(),
                         max_concurrent_scenarios=self.orchestrator.max_concurrent_scenarios,
                         runs=[dict(r, status="planned", attempts=[]) for r in plan["runs"]])
            self.experiments[identifier] = state
            self._save(state)
            self._launch(state)
            return state

    def _launch(self, state):
        state["status"] = "running"
        self._save(state)
        self.tasks[state["id"]] = asyncio.create_task(self._drive(state), name=f"experiment-{state['id']}")

    async def resume(self, identifier, retry_failed=False):
        async with self._lock:
            state = self.get(identifier)
            if identifier in self.tasks and not self.tasks[identifier].done():
                raise ValueError("Experiment is already active")
            statuses = {"error"} if retry_failed else {"planned", "interrupted", "cancelled"}
            for run in state["runs"]:
                retry_invalid = retry_failed and bool(run.get("invalid_metrics"))
                if run["status"] in statuses:
                    self._recover_finished(state, run)
                for attempt in run["attempts"]:
                    if attempt.get("cleanup_required"):
                        await self.orchestrator.docker_service.clear_scenario_resources(attempt["scenario_id"])
                        attempt["cleanup_required"] = False
                if run["status"] in statuses or retry_invalid:
                    for attempt in run["attempts"]:
                        if attempt["status"] in ("interrupted", "cancelled"):
                            await self.orchestrator.docker_service.clear_scenario_resources(attempt["scenario_id"])
                    run["status"] = "planned"
                    run["invalid_metrics"] = []
            self._launch(state)
            return state

    async def cancel(self, identifier):
        async with self._lock:
            state = self.get(identifier)
            if state["status"] == "finished":
                return state
            task = self.tasks.get(identifier)
            if task and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            for run in state["runs"]:
                for attempt in run["attempts"]:
                    if attempt.get("cleanup_required"):
                        await self.orchestrator.docker_service.clear_scenario_resources(attempt["scenario_id"])
                        attempt["cleanup_required"] = False
                if run["status"] in ("planned", "queued", "running", "interrupted"):
                    run["status"] = "cancelled"
            state["status"] = "cancelled"
            self._save(state)
            return state

    async def _drive(self, state):
        folder = self.directory / state["id"]
        plan = read_json(folder / "plan.json")
        configurations = {c["configuration_id"]: c for c in plan["configurations"]}
        try:
            for run in state["runs"]:
                if run["status"] != "planned":
                    continue
                attempt = dict(attempt_id=str(uuid.uuid4()), scenario_id=str(uuid.uuid4()),
                               status="queued", queued_at=now(),
                               max_concurrent_scenarios=self.orchestrator.max_concurrent_scenarios)
                run["attempts"].append(attempt)
                run["status"] = "queued"
                self._save(state)  # Persist ownership BEFORE submitting anything to Docker.
                metadata = dict(experiment_id=state["id"], configuration_id=run["configuration_id"],
                                repetition=run["repetition"], attempt_id=attempt["attempt_id"])
                request = ScenarioCreateRequest.model_validate(configurations[run["configuration_id"]]["scenario"])
                await self.orchestrator.create_scenario(request.to_domain(), metadata=metadata,
                                                        scenario_id=attempt["scenario_id"])
            while any(r["status"] in ("queued", "running") for r in state["runs"]):
                changed = False
                for run in state["runs"]:
                    if run["status"] not in ("queued", "running"):
                        continue
                    attempt = run["attempts"][-1]
                    scenario = self.orchestrator.get_scenario(attempt["scenario_id"])
                    if scenario is None:
                        raise RuntimeError("Submitted scenario disappeared from orchestrator")
                    reserved_at = getattr(scenario, "reserved_at", None)
                    if reserved_at and run["status"] == "queued":
                        run["status"] = attempt["status"] = "running"
                        attempt["resource_started_at"] = reserved_at
                        changed = True
                    if scenario.status.value not in TERMINAL or scenario.id in self.orchestrator.scenario_tasks:
                        continue
                    status = "finished" if scenario.status.value == "FINISHED" else "error"
                    run["status"] = attempt["status"] = status
                    attempt.update(error=scenario.error, ended_at=now(),
                                   resource_started_at=reserved_at,
                                   resource_finished_at=getattr(scenario, "released_at", now()),
                                   overlapping_scenario_ids=getattr(scenario, "overlapping_scenario_ids", []))
                    if scenario.result_path and Path(scenario.result_path).exists():
                        target = folder / "runs" / f"{attempt['attempt_id']}.json"
                        atomic_json(target, read_json(Path(scenario.result_path)))
                        attempt["result"] = str(target.relative_to(folder))
                    else:
                        run["status"] = attempt["status"] = "error"
                        attempt["error"] = "scenario_result_missing"
                    changed = True
                if changed:
                    self._save(state)
                await asyncio.sleep(.2)
            state["status"] = "analyzing"
            self._save(state)
            analysis = asyncio.create_task(asyncio.to_thread(self.analyze, state["id"]))
            try:
                report = await asyncio.shield(analysis)
            except asyncio.CancelledError:
                # A worker thread cannot be cancelled; let it finish before
                # releasing directory ownership or allowing a resume.
                await analysis
                raise
            complete = (all(r["status"] == "finished" for r in state["runs"])
                        and report["summary"]["valid_run_count"] == len(state["runs"])
                        and not report["summary"]["incomplete_groups"])
            invalid = {}
            for group in report["summary"]["groups"]:
                for metric, description in group["metrics"].items():
                    for excluded in description["excluded"]:
                        invalid.setdefault(excluded["run_id"], []).append(metric)
            for run in state["runs"]:
                run["invalid_metrics"] = invalid.get(run["run_id"], [])
            state["status"] = "finished" if complete else "partial"
            self._save(state)
        except asyncio.CancelledError:
            for run in state["runs"]:
                if run["status"] in ("queued", "running"):
                    attempt = run["attempts"][-1]
                    await self.orchestrator.stop_scenario(attempt["scenario_id"])
                    attempt["status"] = run["status"] = "interrupted"
            state["status"] = "partial"
            self._save(state)
            raise
        except Exception as error:
            logger.exception("Experiment %s failed", state["id"])
            # No orphaned workload may survive a manager failure.
            for run in state["runs"]:
                if run["status"] in ("queued", "running"):
                    await self.orchestrator.stop_scenario(run["attempts"][-1]["scenario_id"])
                    run["attempts"][-1]["status"] = run["status"] = "interrupted"
            state.update(status="error", error=str(error))
            self._save(state)

    def analyze(self, identifier):
        return analyze_directory(self.directory / identifier, allow_active=True)

    async def reanalyze(self, identifier):
        async with self._lock:
            if self.get(identifier)["status"] in ("running", "analyzing"):
                raise ValueError("Cannot reanalyze an active experiment")
            return await asyncio.to_thread(self.analyze, identifier)

    def results(self, identifier):
        self.get(identifier)
        folder = self.directory / identifier
        if not (folder / "summary.json").exists():
            raise ValueError("Experiment results are not ready")
        return dict(summary=read_json(folder / "summary.json"), comparisons=read_json(folder / "comparisons.json"))

    async def close(self):
        for task in self.tasks.values():
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        if self._owner is not None:
            self._owner.close()
            self._owner = None


def analyze_directory(folder: Path, *, allow_active=False):
    """Serialize derived-file writes, including offline CLI invocations."""
    state = read_json(folder / "progress.json")
    if not allow_active and state["status"] in ("running", "analyzing"):
        raise ValueError("Cannot reanalyze an active experiment")
    with (folder / ".analysis.lock").open("a+") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        return _analyze_directory(folder)


def _analyze_directory(folder: Path):
    """Rebuild all derived artifacts using files only, with no running services."""
    from src.experiments.statistics import analyze
    state = read_json(folder / "progress.json")
    records = []
    for run in state["runs"]:
        attempt = run["attempts"][-1] if run["attempts"] else {}
        record = dict(run_id=run["run_id"], configuration_id=run["configuration_id"],
                      repetition=run["repetition"], status=run["status"],
                      error=attempt.get("error"), scenario_id=attempt.get("scenario_id"))
        if attempt.get("result"):
            record["artifact"] = read_json(folder / attempt["result"])
        records.append(record)
    summary, comparisons, rows = analyze(read_json(folder / "specification.json"),
                                         read_json(folder / "plan.json"), records)
    summary.update(experiment_id=folder.name, max_concurrent_scenarios=state["max_concurrent_scenarios"])
    summary["execution"] = state["runs"]
    atomic_json(folder / "summary.json", summary)
    atomic_json(folder / "comparisons.json", comparisons)
    fields = sorted(set().union(*(r["metrics"] for r in rows)))
    output = io.StringIO()
    metadata_fields = ["run_id", "configuration_id", "repetition", "status", "broker", "value",
                       "scenario_id", "invalid_reason", "exclusions"]
    writer = csv.DictWriter(output, fieldnames=metadata_fields + fields)
    writer.writeheader()
    configurations = {c["configuration_id"]: c for c in read_json(folder / "plan.json")["configurations"]}
    for row in rows:
        config = configurations[row["configuration_id"]]
        writer.writerow({**{k: row.get(k) for k in metadata_fields}, **row["metrics"],
                         "broker": config["broker"], "value": config["value"],
                         "exclusions": json.dumps(row["exclusions"], ensure_ascii=False)})
    # CSV is derived; JSON remains the durable source of truth.
    temporary = folder / ".runs.csv.tmp"
    temporary.write_text(output.getvalue(), encoding="utf-8")
    os.replace(temporary, folder / "runs.csv")
    return dict(summary=summary, comparisons=comparisons)

