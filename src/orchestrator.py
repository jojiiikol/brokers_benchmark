import asyncio
import logging
import os
import datetime
from typing import Protocol

from src.docker_service.docker_sdk import SdkDockerService
from src.scenario_service.dto.scenario_config import ScenarioConfig
from src.scenario_service.service import ScenarioService, Scenario, ScenarioStatus

logger = logging.getLogger(__name__)


class ScenarioObserver(Protocol):
    """Optional observer of scenario state, independent from a metrics backend."""

    async def update(self, scenarios: list, active_task_count: int) -> None: ...


class Orchestrator:
    def __init__(self, observer: ScenarioObserver | None = None,
                 max_concurrent_scenarios: int | None = None):
        limit = (int(os.getenv("MAX_CONCURRENT_SCENARIOS", "1"))
                 if max_concurrent_scenarios is None else max_concurrent_scenarios)
        if type(limit) is not int or limit < 1:
            raise ValueError("max_concurrent_scenarios must be a positive integer")
        self.max_concurrent_scenarios = limit
        self.docker_service = SdkDockerService()
        self.scenario_service = ScenarioService(self.docker_service)
        self.scenario_storage: list = []
        self.scenario_tasks: dict[str, asyncio.Task] = {}
        self._shutdown_complete = False
        self._observer = observer
        self.experiment_provider = None

        self.create_image_lock = asyncio.Lock()
        self._scenario_operation_lock = asyncio.Lock()

    def set_observer(self, observer: ScenarioObserver | None) -> None:
        """Attach or detach an optional scenario observer at runtime."""
        self._observer = observer

    @property
    def scenario_running_count(self) -> int:
        running_scenarios = list(
            filter(
                lambda scenario: scenario.status in {
                    ScenarioStatus.STARTING,
                    ScenarioStatus.WARMING_UP,
                    ScenarioStatus.RUNNING,
                    ScenarioStatus.DRAINING,
                    ScenarioStatus.STOPPING,
                },
                self.scenario_storage
            )
        )
        return len(running_scenarios)

    def _get_older_scenario(self, status: ScenarioStatus) -> Scenario:
        running_scenarios = list(
            filter(
                lambda scenario: scenario.status == status,
                self.scenario_storage
            )
        )
        older_scenario = min(running_scenarios, key=lambda scenario: scenario.created_at, default=None)
        return older_scenario


    @property
    def _check_run_available(self) -> bool:
        return len(self.scenario_tasks) < self.max_concurrent_scenarios

    def get_info(self):
        full_text = []
        for key, scenario in enumerate(self.scenario_storage):
            full_text.append(
                f"Number {key} | id: {scenario.id} | status: {scenario.status}"
            )
        return "\n".join(full_text)

    async def create_scenario(
            self,
            scenario_config: ScenarioConfig,
            *, metadata: dict | None = None, scenario_id: str | None = None,
    ) -> str:
        scenario = await self.scenario_service.create_scenario(scenario_config, scenario_id=scenario_id)
        scenario.experiment = metadata
        self.scenario_storage.append(scenario)
        await self._notify_observer()
        return scenario.id

    def get_scenario(self, scenario_id: str) -> Scenario | None:
        return next(
            (
                scenario
                for scenario in self.scenario_storage
                if scenario.id == scenario_id
            ),
            None,
        )

    def list_scenarios(self) -> list[Scenario]:
        return list(self.scenario_storage)

    async def start_scenario(self):
        async with self._scenario_operation_lock:
            if not self._check_run_available:
                return

            older_scenario = self._get_older_scenario(ScenarioStatus.CREATED)
            if older_scenario is None:
                older_scenario = self._get_older_scenario(ScenarioStatus.PENDING)
            if older_scenario is None:
                return

            task = asyncio.create_task(
                self._run_reserved_scenario(older_scenario),
                name=f"scenario-{older_scenario.id}",
            )
            self.scenario_tasks[older_scenario.id] = task
            older_scenario.reserved_at = datetime.datetime.now(datetime.UTC).isoformat()
            older_scenario.overlapping_scenario_ids = [sid for sid in self.scenario_tasks if sid != older_scenario.id]
            for sid in older_scenario.overlapping_scenario_ids:
                other = self.get_scenario(sid)
                if other is not None:
                    other.overlapping_scenario_ids = getattr(other, "overlapping_scenario_ids", []) + [older_scenario.id]
            older_scenario.status = ScenarioStatus.INITIALIZING
            return older_scenario.id

    async def _run_reserved_scenario(self, scenario):
        try:
            async with self.create_image_lock:
                await self.scenario_service.initialize_scenario(scenario)
            if scenario.status is not ScenarioStatus.ERROR:
                await self.scenario_service.start_scenario(scenario)
        except asyncio.CancelledError:
            if scenario.status is not ScenarioStatus.CANCELLED:
                await self.docker_service.clear_scenario_resources(scenario.id)
                scenario.status = ScenarioStatus.CANCELLED
            raise
        except Exception as error:
            logger.exception("Scenario worker failed: %s", scenario.id)
            scenario.error = str(error)
            scenario.status = ScenarioStatus.ERROR
            await self.docker_service.clear_scenario_resources(scenario.id)
        finally:
            scenario.released_at = datetime.datetime.now(datetime.UTC).isoformat()

    async def stop_scenario(self, scenario_id: str) -> Scenario | None:
        """Stop one scenario and clean only the Docker resources it owns."""
        async with self._scenario_operation_lock:
            scenario = self.get_scenario(scenario_id)
            if scenario is None:
                return None

            task = self.scenario_tasks.pop(scenario_id, None)
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            elif scenario.status not in {
                ScenarioStatus.CANCELLED,
                ScenarioStatus.ERROR,
                ScenarioStatus.FINISHED,
            }:
                await self.scenario_service.cancel_scenario(scenario)

            await self._notify_observer()
            return scenario


    async def _process(self, stop_when_complete: bool = False) -> None:
        while True:
            self._collect_finished_tasks()
            await self._notify_observer()
            logger.info(self.get_info())
            while self._check_run_available:
                if await self.start_scenario() is None:
                    break

            if stop_when_complete and self._all_scenarios_complete:
                logger.info("All submitted benchmark scenarios completed")
                return

            await asyncio.sleep(3)

    @property
    def _all_scenarios_complete(self) -> bool:
        return bool(self.scenario_storage) and not self.scenario_tasks and all(
            scenario.status in {
                ScenarioStatus.CANCELLED,
                ScenarioStatus.ERROR,
                ScenarioStatus.FINISHED,
            }
            for scenario in self.scenario_storage
        )

    async def _notify_observer(self) -> None:
        if self._observer is None:
            return
        try:
            await self._observer.update(
                self.scenario_storage,
                active_task_count=len(self.scenario_tasks),
            )
            if hasattr(self._observer, "update_experiments"):
                self._observer.update_experiments(
                    list(self.experiment_provider().values()) if self.experiment_provider else [],
                    self.max_concurrent_scenarios,
                )
        except Exception:
            # Observability must never prevent a benchmark from running.
            logger.exception("Scenario observer update failed")

    def _collect_finished_tasks(self) -> None:
        for scenario_id, task in list(self.scenario_tasks.items()):
            if not task.done():
                continue
            self.scenario_tasks.pop(scenario_id)
            try:
                task.result()
            except asyncio.CancelledError:
                logger.info("Scenario %s was cancelled", scenario_id)
            except Exception:
                logger.exception("Scenario %s task failed", scenario_id)

    async def shutdown(self, clear_all_resources: bool = True) -> None:
        """Stop active tasks and remove global or orchestrator-owned resources."""
        if self._shutdown_complete:
            return

        logger.info("Stopping %s active scenario task(s)", len(self.scenario_tasks))
        tasks = list(self.scenario_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self.scenario_tasks.clear()
        await self._notify_observer()

        if clear_all_resources:
            await self.docker_service.clear_resources()
        else:
            await asyncio.gather(*(
                self.docker_service.clear_scenario_resources(scenario.id)
                for scenario in self.scenario_storage
            ))
        self._shutdown_complete = True
        logger.info("Dynamic benchmark resources were cleaned up")

    async def run(
            self,
            stop_event: asyncio.Event | None = None,
            stop_when_complete: bool = False,
            clear_all_resources_on_shutdown: bool = True,
    ) -> None:
        task = asyncio.create_task(
            self._process(stop_when_complete=stop_when_complete),
            name="orchestrator-process",
        )
        stop_task = (
            asyncio.create_task(stop_event.wait(), name="orchestrator-stop-wait")
            if stop_event is not None
            else None
        )
        try:
            if stop_task is None:
                await task
                return

            done, _ = await asyncio.wait(
                (task, stop_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if stop_task in done:
                logger.info("Shutdown signal received")
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                await self.shutdown(
                    clear_all_resources=clear_all_resources_on_shutdown
                )
                return

            await task
        except asyncio.CancelledError:
            logger.info("Process cancelled")
            raise
        except Exception:
            logger.exception("Process failed")
            raise
        finally:
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            if stop_task is not None and not stop_task.done():
                stop_task.cancel()
                await asyncio.gather(stop_task, return_exceptions=True)
