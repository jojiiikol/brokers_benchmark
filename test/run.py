"""Run isolated unit tests and enforce coverage for every module in src."""

import subprocess
import sys
from pathlib import Path


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    commands = [
        ['run', '-m', 'unittest', 'discover', '-s', 'test', '-v'],
        ['report'],
        ['html'],
        ['xml'],
    ]
    status = 0
    for command in commands:
        result = subprocess.run(
            [sys.executable, '-m', 'coverage', *command], cwd=root,
        )
        status = status or result.returncode
        if command[0] == 'run' and result.returncode:
            break
    return status


if __name__ == '__main__':
    raise SystemExit(main())
