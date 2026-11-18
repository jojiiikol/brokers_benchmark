import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from src import setup_logging
from src.api.models import ScenarioCreateRequest, ScenarioResponse
from src.observability.prometheus import (
    PrometheusOrchestratorObserver,
    metrics_enabled,
    start_prometheus_server,
)
from src.orchestrator import Orchestrator


def create_app(
    orchestrator: Orchestrator | None = None,
    *,
    manage_runtime: bool = True,
) -> FastAPI:
    benchmark_orchestrator = orchestrator or Orchestrator()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if not manage_runtime:
            yield
            return

        setup_logging()
        if metrics_enabled():
            start_prometheus_server()
            benchmark_orchestrator.set_observer(
                PrometheusOrchestratorObserver(
                    benchmark_orchestrator.docker_service
                )
            )

        stop_event = asyncio.Event()
        await benchmark_orchestrator.docker_service.open_client()
        process_task = asyncio.create_task(
            benchmark_orchestrator.run(stop_event),
            name="benchmark-orchestrator",
        )
        try:
            yield
        finally:
            stop_event.set()
            await asyncio.gather(process_task, return_exceptions=True)
            await benchmark_orchestrator.docker_service.close_client()

    app = FastAPI(
        title="Broker Benchmark API",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.orchestrator = benchmark_orchestrator

    @app.exception_handler(RequestValidationError)
    async def request_validation_error(
        _request, error: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": jsonable_encoder(error.errors())},
        )

    @app.get("/health", tags=["system"])
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/api/v1/scenarios",
        response_model=ScenarioResponse,
        status_code=status.HTTP_202_ACCEPTED,
        tags=["scenarios"],
    )
    async def create_scenario(payload: ScenarioCreateRequest) -> ScenarioResponse:
        try:
            scenario_id = await benchmark_orchestrator.create_scenario(
                payload.to_domain()
            )
        except ValueError as error:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=str(error),
            ) from error
        scenario = benchmark_orchestrator.get_scenario(scenario_id)
        if scenario is None:  # pragma: no cover - defensive invariant
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Created scenario is missing from orchestrator storage",
            )
        return ScenarioResponse.from_domain(scenario)

    @app.get(
        "/api/v1/scenarios",
        response_model=list[ScenarioResponse],
        tags=["scenarios"],
    )
    async def list_scenarios() -> list[ScenarioResponse]:
        return [
            ScenarioResponse.from_domain(scenario)
            for scenario in benchmark_orchestrator.list_scenarios()
        ]

    @app.get(
        "/api/v1/scenarios/{scenario_id}",
        response_model=ScenarioResponse,
        tags=["scenarios"],
    )
    async def get_scenario(scenario_id: str) -> ScenarioResponse:
        scenario = benchmark_orchestrator.get_scenario(scenario_id)
        if scenario is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Scenario not found",
            )
        return ScenarioResponse.from_domain(scenario)

    @app.post(
        "/api/v1/scenarios/{scenario_id}/stop",
        response_model=ScenarioResponse,
        tags=["scenarios"],
    )
    async def stop_scenario(scenario_id: str) -> ScenarioResponse:
        scenario = await benchmark_orchestrator.stop_scenario(scenario_id)
        if scenario is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Scenario not found",
            )
        return ScenarioResponse.from_domain(scenario)

    return app


app = create_app()
