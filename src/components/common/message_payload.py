"""Fixed-size binary benchmark messages shared by every broker adapter."""

import struct
from dataclasses import dataclass
from functools import lru_cache


_MAGIC = b"BMB1"
_HEADER = struct.Struct("!4sB3xQQ")
_PHASE_TO_CODE = {"warmup": 1, "measurement": 2}
_CODE_TO_PHASE = {code: phase for phase, code in _PHASE_TO_CODE.items()}


@dataclass(frozen=True)
class BenchmarkMessage:
    phase: str
    sent_at: int
    message_id: int


class InvalidBenchmarkMessage(ValueError):
    """Raised when a body does not use the benchmark binary protocol."""


@lru_cache(maxsize=8)
def _message_padding(size_bytes: int) -> bytes:
    return b"a" * (size_bytes - _HEADER.size)


def build_message(
        *,
        size_bytes: int,
        phase: str,
        sent_at: int,
        message_id: int = 0,
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
    if not 0 <= sent_at <= 2**64 - 1 or not 0 <= message_id <= 2**64 - 1:
        raise ValueError("sent_at and message_id must fit into unsigned 64-bit integers")
    header = _HEADER.pack(_MAGIC, phase_code, sent_at, message_id)
    return header + _message_padding(size_bytes)


def decode_message(body: bytes) -> BenchmarkMessage:
    """Read a message header into the convenient object used outside hot paths."""
    phase, sent_at, message_id = decode_message_metadata(body)
    return BenchmarkMessage(phase=phase, sent_at=sent_at, message_id=message_id)


def decode_message_fields(body: bytes) -> tuple[str, int]:
    """Read only metadata needed by consumers without allocating a dataclass."""
    phase, sent_at, _ = decode_message_metadata(body)
    return phase, sent_at


def decode_message_metadata(body: bytes) -> tuple[str, int, int]:
    """Read phase, timestamp and unique message identifier from a body."""
    if len(body) < _HEADER.size:
        raise InvalidBenchmarkMessage("message is shorter than the benchmark header")
    magic, phase_code, sent_at, message_id = _HEADER.unpack_from(body)
    if magic != _MAGIC:
        raise InvalidBenchmarkMessage("message does not use the benchmark protocol")
    try:
        phase = _CODE_TO_PHASE[phase_code]
    except KeyError as error:
        raise InvalidBenchmarkMessage(f"unknown benchmark phase code: {phase_code}") from error
    return phase, sent_at, message_id
