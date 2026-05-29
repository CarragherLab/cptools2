import os
import re
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

import yaml

from scripts import fuse_diagnostic_runner

ROOT = Path(__file__).resolve().parents[1]
EDDIE_CONFIG = ROOT / "nextflow" / "conf" / "eddie.config"
LEGACY_SGE_GENERATOR = ROOT / "cptools2" / "generate_scripts.py"
DIAG_SCRIPT = ROOT / "scripts" / "fuse_diagnostic_runner.py"


def _env(tmp_path):
    source = tmp_path / "staged-source"
    source.mkdir()
    return {
        "CPTOOLS2_PROJECT_ROOT": "/project/root",
        "CPTOOLS2_SCRATCH_ROOT": str(tmp_path / "scratch"),
        "CPTOOLS2_CONTAINER_DIR": "/project/root/containers",
        "CPTOOLS2_DIAG_ROOT": str(tmp_path / "diag"),
        "CPTOOLS2_DIAG_PLATE_ID": "plate-001",
        "CPTOOLS2_DIAG_STAGED_SOURCE": str(source),
    }


def _bash_path(path):
    resolved = Path(path).resolve()
    if os.name != "nt":
        return str(resolved)
    drive = resolved.drive.rstrip(":").lower()
    rest = resolved.as_posix()[2:]
    return f"/mnt/{drive}{rest}"


def test_diagnostic_config_is_already_staged_and_chunk_size_is_configurable(tmp_path):
    paths = fuse_diagnostic_runner.generate_diagnostic_run(
        "scratch",
        chunk_size=7,
        max_chunks=3,
        environ=_env(tmp_path),
        now=datetime(2026, 5, 12, 10, 30, 0),
        create_staged_link=False,
    )

    config = yaml.safe_load(paths.config_path.read_text())

    assert config["stage_data"] is False
    assert config["chunk"] == 7
    assert config["max_chunks"] == 3
    assert config["input_dir"] == str(paths.staged_root)
    assert paths.staged_root == paths.run_dir / "staged-root"
    assert config["plates"] == ["plate-001"]
    assert config["container_path"] == "/project/root/containers"
    assert config["illum_pipeline_calculate"].startswith("${CPTOOLS2_PROJECT_ROOT}")
    assert config["feature_extraction"]["weights"].startswith("${CPTOOLS2_MODEL_DIR}")


def test_diagnostic_config_stages_are_environment_configurable(tmp_path):
    env = _env(tmp_path)
    env["CPTOOLS2_DIAG_STAGES"] = "segment, extract"

    paths = fuse_diagnostic_runner.generate_diagnostic_run(
        "mig-extract",
        chunk_size=5,
        environ=env,
        now=datetime(2026, 5, 13, 9, 15, 0),
        create_staged_link=False,
    )

    config = yaml.safe_load(paths.config_path.read_text())

    assert config["stages"] == ["segment", "extract"]


def test_diagnostic_metadata_records_container_runtime_mode(tmp_path):
    env = _env(tmp_path)
    env["CPTOOLS2_CONTAINER_RUNTIME_MODE"] = "unsquash"

    paths = fuse_diagnostic_runner.generate_diagnostic_run(
        "unsquash",
        chunk_size=5,
        environ=env,
        now=datetime(2026, 5, 14, 9, 15, 0),
        create_staged_link=False,
    )

    metadata = paths.metadata_path.read_text()

    assert "container_runtime_mode=unsquash" in metadata


def test_launcher_writes_status_when_failure_happens_before_pipeline(tmp_path):
    bash = shutil.which("bash")
    if bash is None:
        raise AssertionError("bash is required to validate the generated launcher")

    launch_path = tmp_path / "launch.sh"
    status_path = tmp_path / "logs" / "status.txt"
    log_path = tmp_path / "logs" / "launcher.log"
    launch_path.write_text(fuse_diagnostic_runner.build_launcher(), newline="\n")

    result = subprocess.run(
        [
            bash,
            _bash_path(launch_path),
            _bash_path(tmp_path / "missing-project"),
            _bash_path(tmp_path / "config.yaml"),
            _bash_path(status_path),
            _bash_path(log_path),
            "scratch",
            _bash_path(tmp_path / "containers"),
        ],
        check=False,
        timeout=10,
    )

    assert result.returncode == 11
    assert status_path.read_text().strip() == "11"
    assert log_path.exists()


def test_launcher_runs_nextflow_from_run_dir_with_isolated_nxf_home():
    text = fuse_diagnostic_runner.build_launcher()

    assert 'run_dir="$(dirname "$config_path")"' in text
    assert 'export NXF_HOME="$run_dir/.nxf_home"' in text
    assert 'cd "$run_dir" || exit 16' in text


def test_diagnostic_artifacts_have_no_forbidden_hard_coded_paths(tmp_path):
    paths = fuse_diagnostic_runner.generate_diagnostic_run(
        "node-local",
        chunk_size=3,
        environ=_env(tmp_path),
        now=datetime(2026, 5, 12, 10, 30, 0),
        create_staged_link=False,
    )
    generated_text = "\n".join(
        [
            DIAG_SCRIPT.read_text(),
            paths.config_path.read_text(),
            paths.launch_path.read_text(),
            paths.metadata_path.read_text(),
        ]
    )
    generated_text = generated_text.replace(str(tmp_path), "<tmp>")
    escaped_tmp_path = str(tmp_path).replace("\\", "\\\\")
    generated_text = generated_text.replace(escaped_tmp_path, "<tmp>")

    forbidden = [
        "mharvey" + "2",
        "Chandran" + "Labs",
        "Sarah" + "-screen",
        "sarah" + "-screen",
        "/Users/" + "mharvey" + "2",
        "/" + "exports" + "/" + "cmvm"
        "/eddie/smgphs/groups/" + "Chandran" + "Labs",
    ]
    for token in forbidden:
        assert token not in generated_text


def test_eddie_diagnostic_resources_use_h_rss_not_h_vmem():
    texts = [DIAG_SCRIPT.read_text(), EDDIE_CONFIG.read_text()]

    for text in texts:
        assert "h_vmem" not in text

    sge_directive_sources = list((ROOT / "scripts").glob("*.sh")) + [
        ROOT / "cptools2" / "dockerfiles" / "build_containers.sh",
    ]
    for script_path in sge_directive_sources:
        text = script_path.read_text()
        if "#$" in text:
            assert "h_rss" in text
            assert "h_vmem" not in text


def test_staging_queue_has_no_multi_slot_request():
    eddie_text = EDDIE_CONFIG.read_text()
    staging_block = re.search(
        r"withLabel: 'staging' \{(?P<body>.*?)\n    \}",
        eddie_text,
        flags=re.DOTALL,
    )
    assert staging_block is not None
    assert "queue = 'staging'" in staging_block.group("body")
    assert "penv = null" in staging_block.group("body")
    assert "sharedmem" not in staging_block.group("body")

    legacy_text = LEGACY_SGE_GENERATOR.read_text()
    staging_func = re.search(
        r"def _create_staging_script\(.*?def _create_analysis_script",
        legacy_text,
        flags=re.DOTALL,
    )
    assert staging_func is not None
    assert "#$ -q staging" in staging_func.group(0)
    assert "sharedmem" not in staging_func.group(0)
    assert "-pe" not in staging_func.group(0)
