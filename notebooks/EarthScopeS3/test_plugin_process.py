import os
from pathlib import Path
import signal
import subprocess
import sys


def test_plugin_in_separate_worker_process():
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).with_name("plugin_process_probe.py"))],
        start_new_session=True,
    )
    try:
        assert process.wait(timeout=45) == 0
    finally:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=5)
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()
