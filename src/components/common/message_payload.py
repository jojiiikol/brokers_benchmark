"""Fixed-size binary benchmark messages shared by every broker adapter."""

import struct
from dataclasses import dataclass


_MAGIC = b"BMB1"
_HEADER = struct.Struct("!4sB3xQ")
_PHASE_TO_CODE = {"warmup": 1, "measurement": 2}
_CODE_TO_PHASE = {code: phase for phase, code in _PHASE_TO_CODE.items()}


@dataclass(frozen=True)
class BenchmarkMessage:
    phase: str
    sent_at: int


class InvalidBenchmarkMessage(ValueError):
    """Raised when a body does not use the benchmark binary protocol."""


def build_message(
        *,
        size_bytes: int,
        phase: str,
        sent_at: int,
) -> bytes:
    """Build a body with an exact byte size and a constant-size header."""
    if size_bytes < _HEADER.size:
        raise ValueError(
            f"message size must be at least {_HEADER.size} bytes to fit its header"
        )
    try:
        phase_code = _PHASE_TO_CODE[phase]
    except KeyError as error:
        raise ValueError(f"Unsupported benchmark phase: {phase}") from error
    if not 0 <= sent_at <= 2**64 - 1:
        raise ValueError("sent_at must fit into an unsigned 64-bit integer")
    header = _HEADER.pack(_MAGIC, phase_code, sent_at)
    return header + (b"a" * (size_bytes - len(header)))


def decode_message(body: bytes) -> BenchmarkMessage:
    """Read a message header into the convenient object used outside hot paths."""
    phase, sent_at = decode_message_fields(body)
    return BenchmarkMessage(phase=phase, sent_at=sent_at)


def decode_message_fields(body: bytes) -> tuple[str, int]:
    """Read only metadata needed by consumers without allocating a dataclass."""
    if len(body) < _HEADER.size:
        raise InvalidBenchmarkMessage("message is shorter than the benchmark header")
    magic, phase_code, sent_at = _HEADER.unpack_from(body)
    if magic != _MAGIC:
        raise InvalidBenchmarkMessage("message does not use the benchmark protocol")
    try:
        phase = _CODE_TO_PHASE[phase_code]
    except KeyError as error:
        raise InvalidBenchmarkMessage(f"unknown benchmark phase code: {phase_code}") from error
    return phase, sent_at
