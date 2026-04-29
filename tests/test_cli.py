import json
import os
from types import SimpleNamespace

import pytest

# Import the CLI module directly for unit testing
from cptools2 import __main__ as cli_module
from cptools2 import batch as batch_module
from cptools2.__main__ import build_parser, cmd_generate, cmd_pipeline, cmd_prepare

CURRENT_PATH = os.path.dirname(__file__)
PIPELINE_CONFIG = os.path.join(CURRENT_PATH, "pipeline_config.yaml")
TEST_CONFIG = os.path.join(CURRENT_PATH, "test_config.yaml")


class TestBuildParser:
    """Test that the argparse parser is built correctly."""

    def test_parser_has_subcommands(self):
        parser = build_parser()
        # parser should not raise when given valid subcommands
        args = parser.parse_args(["pipeline", PIPELINE_CONFIG, "--dry-run"])
        assert args.command == "pipeline"
        assert args.dry_run is True

    def test_pipeline_subcommand_defaults(self):
        parser = build_parser()
        args = parser.parse_args(["pipeline", PIPELINE_CONFIG])
        assert args.command == "pipeline"
        assert args.config == PIPELINE_CONFIG
        assert args.dry_run is False
        assert args.resume is False
        assert args.stages is None

    def test_pipeline_with_stages(self):
        parser = build_parser()
        args = parser.parse_args(
            ["pipeline", PIPELINE_CONFIG, "--stages", "illum", "segment"]
        )
        assert args.stages == ["illum", "segment"]

    def test_pipeline_with_resume(self):
        parser = build_parser()
        args = parser.parse_args(["pipeline", PIPELINE_CONFIG, "--resume"])
        assert args.resume is True

    def test_prepare_subcommand(self):
        parser = build_parser()
        args = parser.parse_args(["prepare", PIPELINE_CONFIG])
        assert args.command == "prepare"
        assert args.config == PIPELINE_CONFIG

    def test_prepare_with_stages(self):
        parser = build_parser()
        args = parser.parse_args(
            ["prepare", PIPELINE_CONFIG, "--stages", "extract"]
        )
        assert args.stages == ["extract"]

    def test_join_subcommand(self):
        parser = build_parser()
        args = parser.parse_args(
            ["join", "--location", "/some/path", "--patterns", "Image.csv", "Cells.csv"]
        )
        assert args.command == "join"
        assert args.location == "/some/path"
        assert args.patterns == ["Image.csv", "Cells.csv"]

    def test_generate_subcommand_exists(self):
        parser = build_parser()
        args = parser.parse_args(["generate", "config.yml"])
        assert args.command == "generate"

    def test_version_flag(self):
        parser = build_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--version"])
        assert exc_info.value.code == 0


class TestCmdGenerate:
    """Test that 'generate' prints deprecation and exits."""

    def test_generate_exits_with_error(self):
        parser = build_parser()
        args = parser.parse_args(["generate", "config.yml"])
        with pytest.raises(SystemExit) as exc_info:
            cmd_generate(args)
        assert exc_info.value.code == 1


class TestCmdPipelineDryRun:
    """Test pipeline --dry-run generates params.json."""

    def test_dry_run_creates_params_json(self, tmp_path):
        """pipeline --dry-run should generate params.json without invoking nextflow."""
        # Create a minimal config pointing location to tmp_path
        config_content = (
            "experiment: /path/to/experiment\n"
            "chunk: 46\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "commands location: /home/user\n"
            "channels:\n"
            "  - DAPI\n"
            "  - GFP\n"
            "stages:\n"
            "  - illum\n"
            "  - segment\n"
        ).format(str(tmp_path))
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(config_content)

        parser = build_parser()
        args = parser.parse_args(
            ["pipeline", str(config_file), "--dry-run"]
        )
        cmd_pipeline(args)

        params_path = tmp_path / "params.json"
        assert params_path.exists()
        with open(params_path) as f:
            params = json.load(f)
        assert isinstance(params, dict)
        assert params["channels"] == ["DAPI", "GFP"]
        # illum alias should be expanded
        assert "illum_calculate" in params["stages"]
        assert "illum_apply" in params["stages"]
        assert "segmentation" in params["stages"]

    def test_dry_run_with_stages_override(self, tmp_path):
        """--stages flag should override stages from config."""
        config_content = (
            "experiment: /path/to/experiment\n"
            "chunk: 46\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "commands location: /home/user\n"
            "stages:\n"
            "  - illum\n"
            "  - segment\n"
            "  - extract\n"
        ).format(str(tmp_path))
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(config_content)

        parser = build_parser()
        args = parser.parse_args(
            ["pipeline", str(config_file), "--dry-run", "--stages", "illum"]
        )
        cmd_pipeline(args)

        params_path = tmp_path / "params.json"
        with open(params_path) as f:
            params = json.load(f)
        # Only illum stages should be present (overridden)
        assert params["stages"] == ["illum_calculate", "illum_apply"]

    def test_dry_run_creates_missing_output_directory(self, tmp_path):
        """pipeline --dry-run should create the configured output dir for params.json."""
        output_dir = tmp_path / "fresh-output"
        config_content = (
            "experiment: /path/to/experiment\n"
            "chunk: 46\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "commands location: /home/user\n"
        ).format(str(output_dir))
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(config_content)

        parser = build_parser()
        args = parser.parse_args(["pipeline", str(config_file), "--dry-run"])
        cmd_pipeline(args)

        params_path = output_dir / "params.json"
        assert output_dir.exists()
        assert params_path.exists()

    def test_dry_run_writes_batch_specific_params(self, tmp_path, monkeypatch):
        """Each scratch-safe plate batch gets its own params file."""
        input_dir = tmp_path / "screen"
        (input_dir / "plate-a").mkdir(parents=True)
        (input_dir / "plate-b").mkdir()
        (input_dir / "plate-a" / "image.tif").write_text("a")
        (input_dir / "plate-b" / "image.tif").write_text("b")
        output_dir = tmp_path / "out"
        config_content = (
            "experiment: {}\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "commands location: /home/user\n"
            "chunk: 96\n"
        ).format(str(input_dir), str(output_dir))
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(config_content)
        monkeypatch.setattr(
            batch_module,
            "create_batches",
            lambda plate_sizes, available: [
                {
                    "batch_id": 1,
                    "plates": ["plate-a"],
                    "total_size_gb": 0.1,
                    "plate_count": 1,
                },
                {
                    "batch_id": 2,
                    "plates": ["plate-b"],
                    "total_size_gb": 0.1,
                    "plate_count": 1,
                },
            ],
        )

        parser = build_parser()
        args = parser.parse_args(["pipeline", str(config_file), "--dry-run"])
        cmd_pipeline(args)

        with open(output_dir / "params.batch_1.json") as f:
            batch_1 = json.load(f)
        with open(output_dir / "params.batch_2.json") as f:
            batch_2 = json.load(f)
        assert batch_1["plates"] == ["plate-a"]
        assert batch_2["plates"] == ["plate-b"]
        assert batch_1["batch_id"] == 1
        assert batch_2["batch_id"] == 2

    def test_dry_run_uses_configured_plate_sizes_for_inaccessible_input(
        self, tmp_path, monkeypatch
    ):
        """DataStore staging can use measured plate sizes when input is not mounted."""
        output_dir = tmp_path / "out"
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(
            "input_dir: /exports/igmm/datastore/ImageXpress2020/imagexpress/Sarah-screen\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "plates:\n"
            "  - 3723-D-100\n"
            "plate_sizes_gb:\n"
            "  3723-D-100: 103\n"
            "stage_data: true\n".format(str(output_dir))
        )
        monkeypatch.setattr(
            batch_module,
            "get_scratch_quota",
            lambda config_quota=None: batch_module.ScratchQuota(
                500 * 1024**3, 0, 500 * 1024**3
            ),
        )

        parser = build_parser()
        args = parser.parse_args(["pipeline", str(config_file), "--dry-run"])
        cmd_pipeline(args)

        with open(output_dir / "params.batch_1.json") as f:
            batch_1 = json.load(f)
        assert batch_1["plates"] == ["3723-D-100"]
        assert batch_1["batch_plate_count"] == 1
        assert batch_1["batch_total_size_gb"] == pytest.approx(133.9)

    def test_datastore_dry_run_requires_plate_sizes_when_input_inaccessible(
        self, tmp_path
    ):
        """Do not silently bypass scratch batching for inaccessible DataStore inputs."""
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(
            "input_dir: /exports/igmm/datastore/ImageXpress2020/imagexpress/Sarah-screen\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "plates:\n"
            "  - 3723-D-100\n"
            "stage_data: true\n".format(str(tmp_path / "out"))
        )

        parser = build_parser()
        args = parser.parse_args(["pipeline", str(config_file), "--dry-run"])
        with pytest.raises(RuntimeError, match="plate_sizes_gb"):
            cmd_pipeline(args)


class TestCmdPrepare:
    """Test the prepare subcommand."""

    def test_prepare_creates_params_json(self, tmp_path):
        config_content = (
            "experiment: /path/to/experiment\n"
            "chunk: 46\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "commands location: /home/user\n"
            "stages:\n"
            "  - extract\n"
        ).format(str(tmp_path))
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(config_content)

        parser = build_parser()
        args = parser.parse_args(["prepare", str(config_file)])
        cmd_prepare(args)

        params_path = tmp_path / "params.json"
        assert params_path.exists()
        with open(params_path) as f:
            params = json.load(f)
        assert params["stages"] == ["feature_extract"]


class TestCmdPipelineNoNextflow:
    """Test pipeline without --dry-run when nextflow is not found."""

    def test_pipeline_exits_when_nextflow_missing(self, tmp_path):
        """Without nextflow on PATH, pipeline should exit with error."""
        config_content = (
            "experiment: /path/to/experiment\n"
            "chunk: 46\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "commands location: /home/user\n"
        ).format(str(tmp_path))
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(config_content)

        parser = build_parser()
        args = parser.parse_args(["pipeline", str(config_file)])
        # nextflow is not on PATH in test environment
        with pytest.raises(SystemExit) as exc_info:
            cmd_pipeline(args)
        assert exc_info.value.code == 1

    def test_pipeline_invokes_nextflow_with_scratch_work_dir(
        self, tmp_path, monkeypatch
    ):
        """Nextflow work state should live under the configured output location."""
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(
            "experiment: /path/to/experiment\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "commands location: /home/user\n".format(str(tmp_path))
        )
        captured = []
        monkeypatch.setattr(
            batch_module,
            "get_scratch_quota",
            lambda config_quota=None: batch_module.ScratchQuota(
                500 * 1024**3, 0, 500 * 1024**3
            ),
        )
        monkeypatch.setattr(cli_module, "_find_nextflow", lambda: True)
        monkeypatch.setattr(
            cli_module.subprocess,
            "run",
            lambda cmd: captured.append(cmd) or SimpleNamespace(returncode=0),
        )

        parser = build_parser()
        args = parser.parse_args(["pipeline", str(config_file)])
        cmd_pipeline(args)

        assert captured
        assert "-work-dir" in captured[0]
        assert captured[0][captured[0].index("-work-dir") + 1] == str(
            tmp_path / "work"
        )


class TestConfigFileValidation:
    """Test config file validation."""

    def test_missing_config_raises(self):
        from cptools2.__main__ import _check_config_file

        with pytest.raises(FileNotFoundError):
            _check_config_file("/nonexistent/path/config.yaml")


def test_cmd_join_calls_join_plate_files(tmp_path):
    """cmd_join calls file_tools.join_plate_files with correct args."""
    from unittest.mock import patch
    from cptools2.__main__ import cmd_join
    import argparse
    args = argparse.Namespace(location=str(tmp_path), patterns=["Image.csv"])
    with patch("cptools2.file_tools.join_plate_files") as mock_join:
        cmd_join(args)
        mock_join.assert_called_once()
        call_kwargs = mock_join.call_args
        assert call_kwargs[1]["plate_store"] is None or call_kwargs[0][0] is None
        assert "Image.csv" in (call_kwargs[1].get("patterns") or call_kwargs[0][2])


def test_cmd_pipeline_stages_expanded_in_params(tmp_path):
    """--stages illum produces expanded names in params.json."""
    import json
    # Create minimal config
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "pipeline: ./tests/example_pipeline.cppipe\n"
        "location: {}\n"
        "commands location: /tmp\n"
        "chunk: 10\n"
        "stages:\n"
        "  - illum\n".format(str(tmp_path))
    )
    from cptools2.__main__ import _prepare_config
    config, params_path = _prepare_config(str(config_file))
    with open(params_path) as f:
        params = json.load(f)
    assert "illum_calculate" in params["stages"]
    assert "illum_apply" in params["stages"]
