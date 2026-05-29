import shutil
from pathlib import Path

from scripts import eddie_deepprofiler_matrix
from scripts import launch_deepprofiler_route


def test_matrix_contains_the_expected_route_names():
    matrix = eddie_deepprofiler_matrix.build_matrix(max_chunks=5, chunk=24)
    names = {entry["name"] for entry in matrix}

    assert names == {
        "dp-serialized",
        "dp-concurrency2",
        "dp-hostlock-concurrency4",
        "dp-growth-concurrency2",
        "dp-growth-concurrency5",
        "dp-growth-concurrency8",
        "dp-growth-hostlock-concurrency4",
    }


def test_matrix_entries_use_generic_gpu_scheduling_and_no_memory_limit():
    matrix = eddie_deepprofiler_matrix.build_matrix(max_chunks=5, chunk=24)

    for entry in matrix:
        env = entry["env"]
        assert env["CPTOOLS2_GPU_QUEUE"] == "gpu"
        assert env["CPTOOLS2_GPU_RESOURCE"] == "-l gpu=1"
        assert env["CPTOOLS2_FEATURE_GPU_QUEUE"] == "gpu"
        assert env["CPTOOLS2_FEATURE_GPU_RESOURCE"] == "-l gpu=1"
        assert env["CPTOOLS2_SEGMENT_GPU_QUEUE"] == "gpu"
        assert env["CPTOOLS2_SEGMENT_GPU_RESOURCE"] == "-l gpu=1"
        assert env["CPTOOLS2_SEGMENT_MAX_FORKS"] == "5"
        assert env["CPTOOLS2_DEEPPROFILER_LOCK_DIR"] == (
            "${CPTOOLS2_SCRATCH_ROOT}/locks/deepprofiler"
        )
        assert "CPTOOLS2_DEEPPROFILER_GPU_MEMORY_LIMIT_MB" not in env


def test_render_remote_command_exports_env_and_avoids_nextflow_run():
    entry = eddie_deepprofiler_matrix.build_matrix(max_chunks=5, chunk=24)[0]
    command = eddie_deepprofiler_matrix.render_remote_command(
        entry,
        project_root="/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2",
        scratch_root="/exports/eddie/scratch/mharvey2/cptools2-ai-update",
        config_path="config/loop440-eddie-smoke.yaml",
    )

    assert "export CPTOOLS2_FEATURE_MAX_FORKS=" in command
    assert "export CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH=" in command
    assert "export CPTOOLS2_DEEPPROFILER_HOST_LOCK=" in command
    assert "source config/eddie_env.sh" in command
    assert "python3 -m cptools2.__main__ pipeline" in command
    assert "route_config.yml" in command
    assert "nextflow run" not in command
    assert "CPTOOLS2_DEEPPROFILER_GPU_MEMORY_LIMIT_MB" not in command


def test_render_remote_command_sets_roots_before_sourcing_then_route_overrides():
    entry = eddie_deepprofiler_matrix.build_matrix(max_chunks=5, chunk=24)[0]
    command = eddie_deepprofiler_matrix.render_remote_command(
        entry,
        project_root="/project",
        scratch_root="/scratch",
        config_path="config/loop440-eddie-smoke.yaml",
    )
    lines = command.splitlines()

    project_index = lines.index("export CPTOOLS2_PROJECT_ROOT=/project")
    scratch_index = lines.index("export CPTOOLS2_SCRATCH_ROOT=/scratch")
    source_index = lines.index("source config/eddie_env.sh")
    route_override_index = lines.index("export CPTOOLS2_FEATURE_MAX_FORKS=1")

    assert project_index < source_index
    assert scratch_index < source_index
    assert source_index < route_override_index


def test_write_route_config_sets_route_fields():
    workspace = Path.cwd() / "tmp-codex" / "deepprofiler-matrix-runner"
    shutil.rmtree(workspace, ignore_errors=True)
    workspace.mkdir(parents=True, exist_ok=True)
    try:
        base = workspace / "base.yml"
        base.write_text(
            "input_dir: /input\n"
            "output_dir: /old-output\n"
            "commands location: /old-commands\n"
            "plates:\n"
            "  - tiny-plate-001\n"
            "chunk: 96\n"
            "max_chunks: 1\n"
        )
        entry = eddie_deepprofiler_matrix.build_matrix(max_chunks=5, chunk=24)[0]
        route_dir = workspace / "route"

        config_path = eddie_deepprofiler_matrix.write_route_config(
            entry,
            base_config=base,
            route_dir=route_dir,
        )

        assert route_dir.exists()
        text = config_path.read_text()
        assert "chunk: 24" in text
        assert "max_chunks: 5" in text
        assert f"output_dir: {route_dir.as_posix()}/outputs" in text
        assert f"commands location: {route_dir.as_posix()}/commands" in text
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_write_route_config_omits_max_chunks_when_none():
    workspace = Path.cwd() / "tmp-codex" / "deepprofiler-full-plate-config"
    shutil.rmtree(workspace, ignore_errors=True)
    workspace.mkdir(parents=True, exist_ok=True)
    try:
        base = workspace / "base.yml"
        base.write_text(
            "input_dir: /input\n"
            "output_dir: /old-output\n"
            "plates:\n"
            "  - sarah-representative\n"
            "chunk: 96\n"
            "max_chunks: 5\n"
        )
        entry = {
            **eddie_deepprofiler_matrix.build_matrix(max_chunks=None, chunk=24)[0],
            "name": "fullplate-growth-forks8",
        }
        route_dir = workspace / "route"

        config_path = eddie_deepprofiler_matrix.write_route_config(
            entry,
            base_config=base,
            route_dir=route_dir,
        )

        text = config_path.read_text()
        assert "chunk: 24" in text
        assert "max_chunks:" not in text
        assert f"output_dir: {route_dir.as_posix()}/outputs" in text
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


def test_render_remote_command_uses_custom_route_root():
    entry = eddie_deepprofiler_matrix.build_matrix(
        max_chunks=None,
        chunk=24,
        route_root="diagnostics/deepprofiler-full-plate-scalability",
    )[0]
    command = eddie_deepprofiler_matrix.render_remote_command(
        entry,
        project_root="/project",
        scratch_root="/scratch",
        config_path="/scratch/base.yml",
    )

    assert "/scratch/diagnostics/deepprofiler-full-plate-scalability/" in command
    assert "python3 -m cptools2.__main__ pipeline" in command


def test_launch_script_can_resume_pipeline_command(tmp_path):
    route = {
        "name": "dp-growth-concurrency8",
        "run_root": str(tmp_path / "run"),
        "remote_command": "python3 -m cptools2.__main__ pipeline route_config.yml",
    }

    launch_path = launch_deepprofiler_route.write_launch_script(route, resume=True)

    assert "--resume" in launch_path.read_text()


def test_main_writes_matrix_json_and_route_configs():
    workspace = Path.cwd() / "tmp-codex" / "deepprofiler-matrix-main"
    shutil.rmtree(workspace, ignore_errors=True)
    workspace.mkdir(parents=True, exist_ok=True)
    try:
        base = workspace / "base.yml"
        base.write_text(
            "input_dir: /input\n"
            "output_dir: /old-output\n"
            "plates:\n"
            "  - tiny-plate-001\n"
            "chunk: 96\n"
            "max_chunks: 1\n"
        )
        out = workspace / "matrix.json"
        scratch_root = workspace / "scratch"

        exit_code = eddie_deepprofiler_matrix.main(
            [
                "--chunk",
                "24",
                "--max-chunks",
                "5",
                "--project-root",
                str(workspace / "repo"),
                "--scratch-root",
                str(scratch_root),
                "--config-path",
                str(base),
                "--out",
                str(out),
            ]
        )

        assert exit_code == 0
        assert out.exists()
        data = out.read_text()
        assert data.count("\"name\"") == 7
        assert "\"run_root\"" in data
        assert "\"route_config\"" in data
        assert "\"output_root\"" in data
        assert "\"remote_command\"" in data

        for route in (
            "dp-serialized",
            "dp-concurrency2",
            "dp-hostlock-concurrency4",
            "dp-growth-concurrency2",
            "dp-growth-concurrency5",
            "dp-growth-concurrency8",
            "dp-growth-hostlock-concurrency4",
        ):
            route_dir = (
                scratch_root / "diagnostics" / "deepprofiler-scalability" / route
            )
            assert (route_dir / "route_config.yml").exists()
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
