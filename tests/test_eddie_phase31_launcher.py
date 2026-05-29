import shutil
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
LAUNCHER = ROOT / "scripts" / "eddie_phase31_launch.sh"


def test_phase31_launcher_contains_production_contract():
    text = LAUNCHER.read_text()

    assert ". \"${PROJECT_ROOT}/config/eddie_env.sh\"" in text
    assert "export CPTOOLS2_SCRATCH_ROOT=\"${RUN_ROOT}\"" in text
    assert "NXF_HOME" in (ROOT / "config" / "eddie_env.sh").read_text()
    assert "tmux has-session" in text
    assert "tmux new-session -d" in text
    assert "qstat -u" in text
    assert "ps -fu" in text
    assert "driver_status.json" in text
    assert "status.txt" in text
    assert "--nextflow-diagnostics" in text
    assert "--continue-on-batch-failure" in text
    assert "qsub" not in text


def test_phase31_launcher_bash_syntax():
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash is not available")
    if "system32\\bash" in bash.lower().replace("/", "\\"):
        pytest.skip("Windows WSL bash shim cannot access the sandbox path")

    result = subprocess.run(
        [bash, "-n", str(LAUNCHER)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    assert result.returncode == 0, result.stderr
