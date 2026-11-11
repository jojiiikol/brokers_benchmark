"""Broker-independent deterministic pacing for benchmark producers."""

import argparse
import asyncio
import enum
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass


class LoadType(str, enum.Enum):
    CONSTANT = "constant"
    LINEAR = "linear"


@dataclass(frozen=True)
class LoadStep:
    index: int
    target_rps: int


class LoadScheduler:
    """Resolve a target rate from elapsed phase time.

    A linear profile is discretized into equal-duration steps. The first and
    last steps always use the requested start and end RPS, respectively.
    """

    def __init__(
            self,
            load_type: LoadType,
            measurement_duration_seconds: float,
            rps: int | None = None,
            start_rps: int | None = None,
            end_rps: int | None = None,
            step_duration_seconds: float | None = None,
    ) -> None:
        if measurement_duration_seconds <= 0:
            raise ValueError("measurement duration must be greater than zero")
        self.load_type = load_type
        self.measurement_duration_seconds = measurement_duration_seconds

        if load_type is LoadType.CONSTANT:
            if rps is None or rps <= 0:
                raise ValueError("constant load requires a positive rps")
            self._steps = (LoadStep(index=0, target_rps=rps),)
            self.step_duration_seconds = measurement_duration_seconds
            return

        if (
            start_rps is None
            or end_rps is None
            or step_duration_seconds is None
            or start_rps <= 0
            or end_rps <= start_rps
            or step_duration_seconds <= 0
        ):
            raise ValueError(
                "linear load requires positive start_rps, end_rps and "
                "step_duration_seconds, with end_rps greater than start_rps"
            )

        step_count_float, remainder = divmod(
            measurement_duration_seconds,
            step_duration_seconds,
        )
        if not math.isclose(remainder, 0.0, abs_tol=1e-9):
            raise ValueError(
                "measurement duration must be divisible by linear step duration"
            )
        step_count = int(step_count_float)
        if step_count < 2:
            raise ValueError("linear load requires at least two load steps")

        self.step_duration_seconds = step_duration_seconds
        self._steps = tuple(
            LoadStep(
                index=index,
                target_rps=round(
                    start_rps + (end_rps - start_rps) * index / (step_count - 1)
                ),
            )
            for index in range(step_count)
        )

    def step_at(self, elapsed_seconds: float) -> LoadStep:
        if self.load_type is LoadType.CONSTANT:
            return self._steps[0]
        index = min(
            max(int(elapsed_seconds / self.step_duration_seconds), 0),
            len(self._steps) - 1,
        )
        return self._steps[index]


def add_load_profile_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--load-type",
        choices=tuple(load_type.value for load_type in LoadType),
        default=LoadType.CONSTANT.value,
    )
    parser.add_argument("--rps", type=int, help="RPS for constant load")
    parser.add_argument("--start-rps", type=int, help="Initial RPS for linear load")
    parser.add_argument("--end-rps", type=int, help="Final RPS for linear load")
    parser.add_argument(
        "--step-duration-seconds",
        type=float,
        help="Duration of one linear-load step",
    )
    parser.add_argument(
        "--max-in-flight",
        type=int,
        default=100,
        help="Maximum concurrent local publish operations",
    )


def scheduler_from_arguments(
        args: argparse.Namespace,
        measurement_duration_seconds: float,
) -> LoadScheduler:
    load_type = LoadType(args.load_type)
    if load_type is LoadType.CONSTANT:
        return LoadScheduler(
            load_type=load_type,
            measurement_duration_seconds=measurement_duration_seconds,
            rps=args.rps,
        )
    return LoadScheduler(
        load_type=load_type,
        measurement_duration_seconds=measurement_duration_seconds,
        start_rps=args.start_rps,
        end_rps=args.end_rps,
        step_duration_seconds=args.step_duration_seconds,
    )


async def publish_with_profile(
        scheduler: LoadScheduler,
        duration_seconds: float,
        publish: Callable[[LoadStep], Awaitable[None]],
        on_step: Callable[[LoadStep], None],
        max_in_flight: int = 100,
) -> None:
    """Call ``publish`` at the current target rate for one phase.

    The deadline is authoritative. When a broker is slower than the requested
    rate, the producer never sends a compensating burst afterwards. Publish
    operations run concurrently, but the bounded window prevents an unlimited
    number of tasks and payloads from accumulating in producer memory.
    """
    if max_in_flight <= 0:
        raise ValueError("max_in_flight must be greater than zero")

    loop = asyncio.get_running_loop()
    started_at = loop.time()
    deadline = started_at + duration_seconds
    next_publish_at = started_at
    current_step_index: int | None = None

    in_flight: set[asyncio.Task[None]] = set()
    try:
        while True:
            now = loop.time()
            if now >= deadline:
                return

            step = scheduler.step_at(now - started_at)
            if step.index != current_step_index:
                current_step_index = step.index
                next_publish_at = now
                on_step(step)

            delay = next_publish_at - now
            if delay > 0:
                await asyncio.sleep(min(delay, deadline - now))
                continue

            in_flight.add(asyncio.create_task(publish(step)))
            next_publish_at = max(next_publish_at + 1 / step.target_rps, loop.time())

            if len(in_flight) >= max_in_flight:
                completed, _ = await asyncio.wait(
                    in_flight,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                in_flight.difference_update(completed)
                # Propagate a publish error instead of silently treating a
                # failed local hand-off as a sent message.
                await asyncio.gather(*completed)
    finally:
        if in_flight:
            await asyncio.gather(*in_flight)
