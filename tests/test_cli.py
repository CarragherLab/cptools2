import csv
import json
import os
import shutil
import sys
from contextlib import contextmanager
from types import ModuleType, SimpleNamespace
from pathlib import Path
from uuid import uuid4

import pytest

# Import the CLI module directly for unit testing
from cptools2 import __main__ as cli_module
from cptools2 import batch as batch_module
from cptools2.__main__ import (
    build_parser,
    cmd_export_features,
    cmd_generate,
    cmd_pipeline,
    cmd_prepare,
)

CURRENT_PATH = os.path.dirname(__file__)
PIPELINE_CONFIG = os.path.join(CURRENT_PATH, "pipeline_config.yaml")
TEST_CONFIG = os.path.join(CURRENT_PATH, "test_config.yaml")
TEST_ARTIFACT_ROOT = Path(CURRENT_PATH).parent / ".tmp-tests"


@contextmanager
def _repo_tmp_dir(prefix):
    path = TEST_ARTIFACT_ROOT / f"{prefix}-{uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _write_basic_pipeline_config(path, output_dir, extra=""):
    path.write_text(
        (
            "experiment: /path/to/experiment\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "commands location: /home/user\n"
            "{}"
        ).format(str(output_dir), extra)
    )


def _write_stage_out_verification(output_dir, batch_name, status):
    evidence_dir = output_dir / "stage_out_evidence" / batch_name / "plate-a"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    (evidence_dir / "verification.json").write_text(
        json.dumps(
            {
                "verification_status": status,
                "durable_evidence_dir": str(evidence_dir),
            }
        )
    )
    return evidence_dir


def _two_batch_scratch_batches():
    return [
        {
            "batch_id": 1,
            "plates": ["plate-a"],
            "total_size_gb": 1,
            "plate_count": 1,
        },
        {
            "batch_id": 2,
            "plates": ["plate-b"],
            "total_size_gb": 1,
            "plate_count": 1,
        },
    ]


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
        assert args.cleanup_policy == "verified"
        assert args.clean_work is False
        assert args.continue_on_batch_failure is False
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

    def test_pipeline_with_nextflow_diagnostics(self):
        parser = build_parser()
        args = parser.parse_args(
            ["pipeline", PIPELINE_CONFIG, "--nextflow-diagnostics", "minimal"]
        )
        assert args.nextflow_diagnostics == "minimal"

    def test_pipeline_with_cleanup_policy(self):
        parser = build_parser()
        args = parser.parse_args(
            ["pipeline", PIPELINE_CONFIG, "--cleanup-policy", "keep"]
        )
        assert args.cleanup_policy == "keep"

    def test_pipeline_with_clean_work(self):
        parser = build_parser()
        args = parser.parse_args(["pipeline", PIPELINE_CONFIG, "--clean-work"])
        assert args.clean_work is True
        assert args.cleanup_policy == "verified"

    def test_pipeline_with_continue_on_batch_failure(self):
        parser = build_parser()
        args = parser.parse_args(
            ["pipeline", PIPELINE_CONFIG, "--continue-on-batch-failure"]
        )
        assert args.continue_on_batch_failure is True

    def test_prepare_subcommand(self):
        parser = build_parser()
        args = parser.parse_args(["prepare", PIPELINE_CONFIG])
        assert args.command == "prepare"
        assert args.config == PIPELINE_CONFIG

    def test_prepare_with_stages(self):
        parser = build_parser()
        args = parser.parse_args(["prepare", PIPELINE_CONFIG, "--stages", "extract"])
        assert args.stages == ["extract"]

    def test_join_subcommand(self):
        parser = build_parser()
        args = parser.parse_args(
            ["join", "--location", "/some/path", "--patterns", "Image.csv", "Cells.csv"]
        )
        assert args.command == "join"
        assert args.location == "/some/path"
        assert args.patterns == ["Image.csv", "Cells.csv"]

    def test_deepprofiler_package_subcommand(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "deepprofiler-package",
                "--chunk-manifest",
                "chunk.csv",
                "--locations-dir",
                "locations",
                "--output-root",
                "dp_project/inputs",
                "--config-path",
                "config.json",
            ]
        )
        assert args.command == "deepprofiler-package"
        assert args.chunk_manifest == "chunk.csv"
        assert args.locations_dir == "locations"
        assert args.output_root == "dp_project/inputs"
        assert args.config_path == "config.json"

    def test_export_features_subcommand_defaults_to_csv(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "export-features",
                "/tmp/features",
                "--extractor",
                "deepprofiler",
                "--output-dir",
                "/tmp/export",
            ]
        )
        assert args.command == "export-features"
        assert args.features_dir == "/tmp/features"
        assert args.extractor == "deepprofiler"
        assert args.output_dir == "/tmp/export"
        assert args.formats == ["csv"]
        assert args.nan_object_fail_fraction == 0.05

    def test_export_features_subcommand_accepts_optional_parquet(self):
        parser = build_parser()
        args = parser.parse_args(
            [
                "export-features",
                "/tmp/features",
                "--extractor",
                "deepprofiler",
                "--output-dir",
                "/tmp/export",
                "--formats",
                "csv",
                "parquet",
                "--nan-object-fail-fraction",
                "0.2",
            ]
        )
        assert args.formats == ["csv", "parquet"]
        assert args.nan_object_fail_fraction == 0.2

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
        args = parser.parse_args(["pipeline", str(config_file), "--dry-run"])
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
        """pipeline --dry-run should create the configured output dir."""
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
            lambda plate_sizes, available, **kwargs: [
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
        assert batch_1["batch_name"] == "batch_001"
        assert batch_2["batch_name"] == "batch_002"
        assert batch_1["batch_work_dir"].endswith(os.path.join("work", "batch_001"))
        assert batch_2["batch_work_dir"].endswith(os.path.join("work", "batch_002"))
        assert batch_1["output_dir"] == str(output_dir)
        assert batch_2["output_dir"] == str(output_dir)

    def test_export_features_dispatches_to_deepprofiler_exporter(
        self, monkeypatch, capsys, tmp_path
    ):
        """export-features should call the intended feature export API."""
        fake_package = ModuleType("cptools2.feature_export")
        fake_package.__path__ = []
        fake_contract = ModuleType("cptools2.feature_export.contract")
        fake_deepprofiler = ModuleType("cptools2.feature_export.deepprofiler")
        captured = {}

        class FeatureExportRequest:
            def __init__(
                self,
                extractor,
                features_dir,
                output_dir,
                formats,
                nan_object_fail_fraction,
                plate_id=None,
                run_label=None,
            ):
                self.extractor = extractor
                self.features_dir = features_dir
                self.output_dir = output_dir
                self.formats = formats
                self.nan_object_fail_fraction = nan_object_fail_fraction
                self.plate_id = plate_id
                self.run_label = run_label

        def normalise_formats(values):
            return tuple(str(value).lower() for value in values)

        def export_deepprofiler(request):
            captured["request"] = request
            return SimpleNamespace(
                extractor=request.extractor,
                manifest_rows=10,
                site_rows=11,
                cell_rows=12,
                quality_rows=13,
            )

        fake_contract.FeatureExportRequest = FeatureExportRequest
        fake_contract.normalise_formats = normalise_formats
        fake_deepprofiler.export_deepprofiler = export_deepprofiler

        monkeypatch.setitem(sys.modules, "cptools2.feature_export", fake_package)
        monkeypatch.setitem(
            sys.modules, "cptools2.feature_export.contract", fake_contract
        )
        monkeypatch.setitem(
            sys.modules, "cptools2.feature_export.deepprofiler", fake_deepprofiler
        )

        args = build_parser().parse_args(
            [
                "export-features",
                str(tmp_path / "features"),
                "--extractor",
                "deepprofiler",
                "--output-dir",
                str(tmp_path / "export"),
                "--formats",
                "csv",
                "parquet",
                "--nan-object-fail-fraction",
                "0.2",
                "--plate-id",
                "plate-001",
                "--run-label",
                "loop510",
            ]
        )

        cmd_export_features(args)

        request = captured["request"]
        assert request.features_dir == tmp_path / "features"
        assert request.output_dir == tmp_path / "export"
        assert request.formats == ("csv", "parquet")
        assert request.nan_object_fail_fraction == 0.2
        assert request.plate_id == "plate-001"
        assert request.run_label == "loop510"
        assert (
            "Exported deepprofiler: manifest=10 sites=11 cells=12 quality=13"
            in capsys.readouterr().out
        )

    def test_dry_run_reports_batch_provenance_paths(
        self, tmp_path, monkeypatch, capsys
    ):
        """Dry-run output should show batch work, trace, and params paths."""
        input_dir = tmp_path / "screen"
        (input_dir / "plate-a").mkdir(parents=True)
        (input_dir / "plate-a" / "image.tif").write_text("a")
        output_dir = tmp_path / "out"
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(
            "experiment: {}\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "commands location: /home/user\n".format(str(input_dir), str(output_dir))
        )
        monkeypatch.setattr(
            batch_module,
            "create_batches",
            lambda plate_sizes, available, **kwargs: [
                {
                    "batch_id": 1,
                    "plates": ["plate-a"],
                    "total_size_gb": 0.1,
                    "plate_count": 1,
                }
            ],
        )

        parser = build_parser()
        args = parser.parse_args(["pipeline", str(config_file), "--dry-run"])
        cmd_pipeline(args)

        captured = capsys.readouterr().out.replace("\\", "/")
        assert "batch_001" in captured
        assert "work/batch_001" in captured
        assert "traces/trace.batch_001.txt" in captured
        assert "traces/report.batch_001.html" in captured
        assert "traces/timeline.batch_001.html" in captured
        assert "params.batch_1.json" in captured
        assert str(output_dir).replace("\\", "/") in captured

    def test_dry_run_passes_scratch_model_overrides_to_create_batches(
        self, tmp_path, monkeypatch
    ):
        """Config sizing overrides should be forwarded to batch creation."""
        output_dir = tmp_path / "out"
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(
            "experiment: /path/to/experiment\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "commands location: /home/user\n"
            "plates:\n"
            "  - plate-a\n"
            "plate_sizes_gb:\n"
            "  plate-a: 100\n"
            "scratch_utilisation_fraction: 0.6\n"
            "scratch_work_factor: 1.8\n".format(str(output_dir))
        )

        monkeypatch.setattr(
            batch_module,
            "get_scratch_quota",
            lambda config_quota=None: batch_module.ScratchQuota(
                500 * 1024**3, 0, 500 * 1024**3
            ),
        )
        monkeypatch.setattr(cli_module, "_find_nextflow", lambda: True)

        captured = {}

        def fake_create_batches(
            plate_sizes,
            available,
            utilisation_fraction=0.75,
            work_factor=1.3,
        ):
            captured["plate_sizes"] = plate_sizes
            captured["available"] = available
            captured["utilisation_fraction"] = utilisation_fraction
            captured["work_factor"] = work_factor
            return [
                {
                    "batch_id": 1,
                    "plates": ["plate-a"],
                    "total_size_gb": 0.1,
                    "plate_count": 1,
                }
            ]

        monkeypatch.setattr(batch_module, "create_batches", fake_create_batches)

        parser = build_parser()
        args = parser.parse_args(["pipeline", str(config_file), "--dry-run"])
        cmd_pipeline(args)

        assert captured["plate_sizes"] == {"plate-a": 100 * 1024**3}
        assert captured["available"] == 500 * 1024**3
        assert captured["utilisation_fraction"] == 0.6
        assert captured["work_factor"] == 1.8

    def test_dry_run_reports_scratch_model_values(self, tmp_path, capsys):
        """Dry-run output should include the scratch sizing model values."""
        output_dir = tmp_path / "out"
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(
            "experiment: /path/to/experiment\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "commands location: /home/user\n"
            "plates:\n"
            "  - plate-a\n"
            "plate_sizes_gb:\n"
            "  plate-a: 100\n"
            "scratch_utilisation_fraction: 0.6\n"
            "scratch_work_factor: 1.8\n".format(str(output_dir))
        )

        parser = build_parser()
        args = parser.parse_args(["pipeline", str(config_file), "--dry-run"])
        cmd_pipeline(args)

        captured = capsys.readouterr().out.replace("\\", "/")
        assert "scratch_utilisation_fraction" in captured
        assert "0.6" in captured
        assert "scratch_work_factor" in captured
        assert "1.8" in captured

    def test_invalid_scratch_model_values_are_rejected(self, tmp_path):
        """Invalid scratch sizing values should fail before batch creation."""
        output_dir = tmp_path / "out"
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(
            "experiment: /path/to/experiment\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "commands location: /home/user\n"
            "plates:\n"
            "  - plate-a\n"
            "plate_sizes_gb:\n"
            "  plate-a: 100\n"
            "scratch_utilisation_fraction: 0\n"
            "scratch_work_factor: 1.8\n".format(str(output_dir))
        )

        parser = build_parser()
        args = parser.parse_args(["pipeline", str(config_file), "--dry-run"])
        with pytest.raises(ValueError, match="scratch_utilisation_fraction"):
            cmd_pipeline(args)

    def test_nextflow_command_builder_uses_batch_provenance_paths(self, tmp_path):
        """The Nextflow command helper should isolate each batch's work and traces."""
        from cptools2.__main__ import _build_nextflow_command

        batch_info = {
            "batch_id": 1,
            "batch_name": "batch_001",
            "batch_work_dir": str(tmp_path / "work" / "batch_001"),
            "trace_path": str(tmp_path / "traces" / "trace.batch_001.txt"),
            "report_path": str(tmp_path / "traces" / "report.batch_001.html"),
            "timeline_path": str(tmp_path / "traces" / "timeline.batch_001.html"),
        }
        command = _build_nextflow_command(
            nf_main="/path/to/main.nf",
            batch_params_path=str(tmp_path / "params.batch_1.json"),
            batch_info=batch_info,
            resume=True,
        )

        assert command[:4] == ["nextflow", "run", "/path/to/main.nf", "-params-file"]
        assert command[4] == str(tmp_path / "params.batch_1.json")
        assert "-work-dir" in command
        assert command[command.index("-work-dir") + 1] == batch_info["batch_work_dir"]
        assert command[command.index("-with-trace") + 1] == batch_info["trace_path"]
        assert command[command.index("-with-report") + 1] == batch_info["report_path"]
        assert (
            command[command.index("-with-timeline") + 1] == batch_info["timeline_path"]
        )
        assert command[-1] == "-resume"

    def test_nextflow_command_builder_full_diagnostics(self):
        """Full diagnostics should enable trace, report, and timeline observers."""
        from cptools2.__main__ import _build_nextflow_command

        with _repo_tmp_dir("builder-full") as tmp_path:
            batch_info = {
                "batch_id": 1,
                "batch_name": "batch_001",
                "batch_work_dir": str(tmp_path / "work" / "batch_001"),
                "trace_path": str(tmp_path / "traces" / "trace.batch_001.txt"),
                "report_path": str(tmp_path / "traces" / "report.batch_001.html"),
                "timeline_path": str(tmp_path / "traces" / "timeline.batch_001.html"),
            }

            command = _build_nextflow_command(
                nf_main="/path/to/main.nf",
                batch_params_path=str(tmp_path / "params.batch_1.json"),
                batch_info=batch_info,
                diagnostics_mode="full",
            )

            assert "-with-trace" in command
            assert "-with-report" in command
            assert "-with-timeline" in command

    def test_nextflow_command_builder_minimal_diagnostics(self):
        """Minimal diagnostics should keep trace but suppress report and timeline."""
        from cptools2.__main__ import _build_nextflow_command

        with _repo_tmp_dir("builder-minimal") as tmp_path:
            batch_info = {
                "batch_id": 1,
                "batch_name": "batch_001",
                "batch_work_dir": str(tmp_path / "work" / "batch_001"),
                "trace_path": str(tmp_path / "traces" / "trace.batch_001.txt"),
                "report_path": str(tmp_path / "traces" / "report.batch_001.html"),
                "timeline_path": str(tmp_path / "traces" / "timeline.batch_001.html"),
            }

            command = _build_nextflow_command(
                nf_main="/path/to/main.nf",
                batch_params_path=str(tmp_path / "params.batch_1.json"),
                batch_info=batch_info,
                diagnostics_mode="minimal",
            )

            assert "-with-trace" in command
            assert "-with-report" not in command
            assert "-with-timeline" not in command

    def test_nextflow_command_builder_off_diagnostics(self):
        """Off diagnostics should omit all optional Nextflow observer flags."""
        from cptools2.__main__ import _build_nextflow_command

        with _repo_tmp_dir("builder-off") as tmp_path:
            batch_info = {
                "batch_id": 1,
                "batch_name": "batch_001",
                "batch_work_dir": str(tmp_path / "work" / "batch_001"),
                "trace_path": str(tmp_path / "traces" / "trace.batch_001.txt"),
                "report_path": str(tmp_path / "traces" / "report.batch_001.html"),
                "timeline_path": str(tmp_path / "traces" / "timeline.batch_001.html"),
            }

            command = _build_nextflow_command(
                nf_main="/path/to/main.nf",
                batch_params_path=str(tmp_path / "params.batch_1.json"),
                batch_info=batch_info,
                diagnostics_mode="off",
            )

            assert "-with-trace" not in command
            assert "-with-report" not in command
            assert "-with-timeline" not in command

    def test_dry_run_writes_batch_params_diagnostics_from_config(self):
        """Config-driven diagnostics mode should flow into batch params."""
        with _repo_tmp_dir("diag-config") as tmp_path:
            output_dir = tmp_path / "out"
            config_file = tmp_path / "test_config.yaml"
            config_file.write_text(
                "experiment: /path/to/experiment\n"
                "pipeline: tests/example_pipeline.cppipe\n"
                "location: {}\n"
                "commands location: /home/user\n"
                "nextflow_diagnostics: minimal\n".format(str(output_dir))
            )

            parser = build_parser()
            args = parser.parse_args(["pipeline", str(config_file), "--dry-run"])
            cmd_pipeline(args)

            with open(output_dir / "params.batch_1.json") as f:
                batch_1 = json.load(f)

            assert batch_1["enable_trace"] is True
            assert batch_1["enable_report"] is False
            assert batch_1["enable_timeline"] is False

    def test_dry_run_writes_batch_params_diagnostics_from_cli(self):
        """CLI diagnostics mode should override the config default."""
        with _repo_tmp_dir("diag-cli") as tmp_path:
            output_dir = tmp_path / "out"
            config_file = tmp_path / "test_config.yaml"
            config_file.write_text(
                "experiment: /path/to/experiment\n"
                "pipeline: tests/example_pipeline.cppipe\n"
                "location: {}\n"
                "commands location: /home/user\n".format(str(output_dir))
            )

            parser = build_parser()
            args = parser.parse_args(
                [
                    "pipeline",
                    str(config_file),
                    "--dry-run",
                    "--nextflow-diagnostics",
                    "off",
                ]
            )
            cmd_pipeline(args)

            with open(output_dir / "params.batch_1.json") as f:
                batch_1 = json.load(f)

            assert batch_1["enable_trace"] is False
            assert batch_1["enable_report"] is False
            assert batch_1["enable_timeline"] is False

    def test_dry_run_uses_configured_plate_sizes_for_inaccessible_input(
        self, tmp_path, monkeypatch
    ):
        """DataStore staging can use measured plate sizes when input is not mounted."""
        output_dir = tmp_path / "out"
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(
            "input_dir: /exports/<college>/datastore/<project>/imagexpress/<screen>\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "plates:\n"
            "  - example-plate-001\n"
            "plate_sizes_gb:\n"
            "  example-plate-001: 103\n"
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
        assert batch_1["plates"] == ["example-plate-001"]
        assert batch_1["batch_plate_count"] == 1
        assert batch_1["batch_total_size_gb"] == pytest.approx(133.9)

    def test_datastore_dry_run_requires_plate_sizes_when_input_inaccessible(
        self, tmp_path
    ):
        """Do not silently bypass scratch batching for inaccessible DataStore inputs."""
        config_file = tmp_path / "test_config.yaml"
        config_file.write_text(
            "input_dir: /exports/<college>/datastore/<project>/imagexpress/<screen>\n"
            "pipeline: tests/example_pipeline.cppipe\n"
            "location: {}\n"
            "plates:\n"
            "  - example-plate-001\n"
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
        args = parser.parse_args(
            ["pipeline", str(config_file), "--cleanup-policy", "keep"]
        )
        cmd_pipeline(args)

        assert captured
        assert "-work-dir" in captured[0]
        assert captured[0][captured[0].index("-work-dir") + 1] == str(
            tmp_path / "work" / "batch_001"
        )

    def test_pipeline_does_not_clean_failed_batches(self, monkeypatch):
        """Cleanup must not run when a batch exits non-zero."""
        with _repo_tmp_dir("clean-failed") as tmp_path:
            config_file = tmp_path / "test_config.yaml"
            config_file.write_text(
                "experiment: /path/to/experiment\n"
                "pipeline: tests/example_pipeline.cppipe\n"
                "location: {}\n"
                "commands location: /home/user\n".format(str(tmp_path))
            )
            calls = []
            monkeypatch.setattr(
                batch_module,
                "get_scratch_quota",
                lambda config_quota=None: batch_module.ScratchQuota(
                    500 * 1024**3, 0, 500 * 1024**3
                ),
            )
            monkeypatch.setattr(cli_module, "_find_nextflow", lambda: True)

            def fake_run(cmd):
                calls.append(cmd)
                return SimpleNamespace(returncode=1)

            monkeypatch.setattr(cli_module.subprocess, "run", fake_run)

            parser = build_parser()
            args = parser.parse_args(["pipeline", str(config_file), "--clean-work"])
            with pytest.raises(SystemExit) as exc_info:
                cmd_pipeline(args)
            assert exc_info.value.code == 1
            assert len(calls) == 1
            assert calls[0][0:2] == ["nextflow", "run"]
            assert not any(cmd[1] == "clean" for cmd in calls)

    def test_pipeline_cleans_work_after_successful_batch(self, monkeypatch):
        """Successful batches should trigger nextflow clean on the batch work dir."""
        with _repo_tmp_dir("clean-success") as tmp_path:
            output_dir = tmp_path / "out"
            config_file = tmp_path / "test_config.yaml"
            _write_basic_pipeline_config(config_file, output_dir)
            work_dir = output_dir / "work" / "batch_001"
            _write_stage_out_verification(output_dir, "batch_001", "verified")
            calls = []
            monkeypatch.setattr(
                batch_module,
                "get_scratch_quota",
                lambda config_quota=None: batch_module.ScratchQuota(
                    500 * 1024**3, 0, 500 * 1024**3
                ),
            )
            monkeypatch.setattr(cli_module, "_find_nextflow", lambda: True)

            def fake_run(cmd):
                calls.append(cmd)
                return SimpleNamespace(returncode=0)

            monkeypatch.setattr(cli_module.subprocess, "run", fake_run)

            parser = build_parser()
            args = parser.parse_args(["pipeline", str(config_file), "--clean-work"])
            cmd_pipeline(args)

            assert len(calls) == 2
            assert calls[0][0:2] == ["nextflow", "run"]
            assert calls[1][0:2] == ["nextflow", "clean"]
            assert calls[1][calls[1].index("-work-dir") + 1] == str(work_dir)

    def test_pipeline_does_not_clean_unverified_batches(self, monkeypatch):
        """Unverified stage-out evidence should stop cleanup."""
        with _repo_tmp_dir("clean-unverified") as tmp_path:
            output_dir = tmp_path / "out"
            config_file = tmp_path / "test_config.yaml"
            _write_basic_pipeline_config(config_file, output_dir)
            _write_stage_out_verification(output_dir, "batch_001", "unverified")
            calls = []
            monkeypatch.setattr(
                batch_module,
                "get_scratch_quota",
                lambda config_quota=None: batch_module.ScratchQuota(
                    500 * 1024**3, 0, 500 * 1024**3
                ),
            )
            monkeypatch.setattr(cli_module, "_find_nextflow", lambda: True)

            def fake_run(cmd):
                calls.append(cmd)
                return SimpleNamespace(returncode=0)

            monkeypatch.setattr(cli_module.subprocess, "run", fake_run)

            parser = build_parser()
            args = parser.parse_args(
                ["pipeline", str(config_file), "--cleanup-policy", "verified"]
            )
            with pytest.raises(SystemExit) as exc_info:
                cmd_pipeline(args)
            assert exc_info.value.code == 1
            assert len(calls) == 1
            assert calls[0][0:2] == ["nextflow", "run"]
            assert not any(cmd[0:2] == ["nextflow", "clean"] for cmd in calls)

    def test_pipeline_reads_nested_work_local_stage_out_verification(self):
        """Work-local stage_out_evidence fallback should find nested verification JSON."""
        from cptools2.__main__ import _read_stage_out_verification

        with _repo_tmp_dir("nested-verification") as tmp_path:
            batch_work_dir = tmp_path / "work" / "batch_001"
            evidence_dir = (
                batch_work_dir / "stage_out_evidence" / "batch_001" / "plate-a"
            )
            evidence_dir.mkdir(parents=True, exist_ok=True)
            verification_path = evidence_dir / "verification.json"
            verification_path.write_text(
                json.dumps(
                    {
                        "verification_status": "verified",
                        "durable_evidence_dir": str(evidence_dir),
                    }
                )
            )

            status, message, evidence_paths = _read_stage_out_verification(
                batch_work_dir
            )

            assert status == "verified"
            assert verification_path in evidence_paths
            assert "verified" in message

    def test_pipeline_cleans_work_without_verification_after_successful_batch(
        self, monkeypatch
    ):
        """Legacy clean-work should still clean a successful batch without evidence."""
        with _repo_tmp_dir("clean-no-verification") as tmp_path:
            config_file = tmp_path / "test_config.yaml"
            config_file.write_text(
                "experiment: /path/to/experiment\n"
                "pipeline: tests/example_pipeline.cppipe\n"
                "location: {}\n"
                "commands location: /home/user\n".format(str(tmp_path))
            )
            calls = []
            monkeypatch.setattr(
                batch_module,
                "get_scratch_quota",
                lambda config_quota=None: batch_module.ScratchQuota(
                    500 * 1024**3, 0, 500 * 1024**3
                ),
            )
            monkeypatch.setattr(cli_module, "_find_nextflow", lambda: True)

            def fake_run(cmd):
                calls.append(cmd)
                return SimpleNamespace(returncode=0)

            monkeypatch.setattr(cli_module.subprocess, "run", fake_run)

            parser = build_parser()
            args = parser.parse_args(["pipeline", str(config_file), "--clean-work"])
            cmd_pipeline(args)

            assert len(calls) == 2
            assert calls[0][0:2] == ["nextflow", "run"]
            assert calls[1][0:2] == ["nextflow", "clean"]
            assert calls[1][calls[1].index("-work-dir") + 1] == str(
                tmp_path / "work" / "batch_001"
            )

    def test_pipeline_verified_cleanup_requires_evidence(
        self, monkeypatch
    ):
        """Default verified cleanup should remain evidence-gated."""
        with _repo_tmp_dir("verified-needs-evidence") as tmp_path:
            config_file = tmp_path / "test_config.yaml"
            config_file.write_text(
                "experiment: /path/to/experiment\n"
                "pipeline: tests/example_pipeline.cppipe\n"
                "location: {}\n"
                "commands location: /home/user\n".format(str(tmp_path))
            )
            calls = []
            monkeypatch.setattr(
                batch_module,
                "get_scratch_quota",
                lambda config_quota=None: batch_module.ScratchQuota(
                    500 * 1024**3, 0, 500 * 1024**3
                ),
            )
            monkeypatch.setattr(cli_module, "_find_nextflow", lambda: True)

            def fake_run(cmd):
                calls.append(cmd)
                return SimpleNamespace(returncode=0)

            monkeypatch.setattr(cli_module.subprocess, "run", fake_run)

            parser = build_parser()
            args = parser.parse_args(["pipeline", str(config_file)])
            with pytest.raises(SystemExit) as exc_info:
                cmd_pipeline(args)
            assert exc_info.value.code == 1
            assert len(calls) == 1
            assert calls[0][0:2] == ["nextflow", "run"]
            assert not any(cmd[0:2] == ["nextflow", "clean"] for cmd in calls)

    def test_pipeline_continue_on_nextflow_failure_runs_later_batches(
        self, monkeypatch
    ):
        """A failed Nextflow batch should not block later batches when requested."""
        with _repo_tmp_dir("continue-nextflow-failure") as tmp_path:
            output_dir = tmp_path / "out"
            config_file = tmp_path / "test_config.yaml"
            _write_basic_pipeline_config(config_file, output_dir)
            _write_stage_out_verification(output_dir, "batch_002", "verified")
            monkeypatch.setattr(
                cli_module,
                "_build_scratch_batches",
                lambda *args, **kwargs: _two_batch_scratch_batches(),
            )
            monkeypatch.setattr(
                batch_module,
                "get_scratch_quota",
                lambda config_quota=None: batch_module.ScratchQuota(
                    500 * 1024**3, 0, 500 * 1024**3
                ),
            )
            monkeypatch.setattr(cli_module, "_find_nextflow", lambda: True)
            return_codes = iter([7, 0, 0])
            calls = []

            def fake_run(cmd):
                calls.append(cmd)
                return SimpleNamespace(returncode=next(return_codes))

            monkeypatch.setattr(cli_module.subprocess, "run", fake_run)

            parser = build_parser()
            args = parser.parse_args(
                [
                    "pipeline",
                    str(config_file),
                    "--continue-on-batch-failure",
                ]
            )
            with pytest.raises(SystemExit) as exc_info:
                cmd_pipeline(args)

            assert exc_info.value.code == 7
            assert [cmd[0:2] for cmd in calls] == [
                ["nextflow", "run"],
                ["nextflow", "run"],
                ["nextflow", "clean"],
            ]
            with open(output_dir / "batch_status.csv", newline="") as f:
                rows = list(csv.DictReader(f))
            assert [row["batch_name"] for row in rows] == ["batch_001", "batch_002"]
            assert rows[0]["nextflow_status"] == "failed"
            assert rows[1]["nextflow_status"] == "success"
            report = (output_dir / "run_report.md").read_text()
            assert "Problem batch records: 1" in report
            assert "batch_001" in report

    def test_pipeline_continue_on_unverified_stage_out_runs_later_batches(
        self, monkeypatch
    ):
        """Unverified stage-out should be recorded while later batches continue."""
        with _repo_tmp_dir("continue-unverified") as tmp_path:
            output_dir = tmp_path / "out"
            config_file = tmp_path / "test_config.yaml"
            _write_basic_pipeline_config(config_file, output_dir)
            _write_stage_out_verification(output_dir, "batch_001", "unverified")
            _write_stage_out_verification(output_dir, "batch_002", "verified")
            monkeypatch.setattr(
                cli_module,
                "_build_scratch_batches",
                lambda *args, **kwargs: _two_batch_scratch_batches(),
            )
            monkeypatch.setattr(
                batch_module,
                "get_scratch_quota",
                lambda config_quota=None: batch_module.ScratchQuota(
                    500 * 1024**3, 0, 500 * 1024**3
                ),
            )
            monkeypatch.setattr(cli_module, "_find_nextflow", lambda: True)
            calls = []
            monkeypatch.setattr(
                cli_module.subprocess,
                "run",
                lambda cmd: calls.append(cmd) or SimpleNamespace(returncode=0),
            )

            parser = build_parser()
            args = parser.parse_args(
                [
                    "pipeline",
                    str(config_file),
                    "--continue-on-batch-failure",
                ]
            )
            with pytest.raises(SystemExit) as exc_info:
                cmd_pipeline(args)

            assert exc_info.value.code == 1
            assert [cmd[0:2] for cmd in calls] == [
                ["nextflow", "run"],
                ["nextflow", "run"],
                ["nextflow", "clean"],
            ]
            with open(output_dir / "batch_status.csv", newline="") as f:
                rows = list(csv.DictReader(f))
            assert rows[0]["stage_out_status"] == "unverified"
            assert rows[0]["cleanup_status"] == "not_eligible"
            assert rows[1]["stage_out_status"] == "verified"
            assert rows[1]["cleanup_status"] == "cleaned"

    def test_pipeline_continue_on_cleanup_failure_runs_later_batches(
        self, monkeypatch
    ):
        """Cleanup failure should preserve that batch and continue when requested."""
        with _repo_tmp_dir("continue-cleanup-failure") as tmp_path:
            output_dir = tmp_path / "out"
            config_file = tmp_path / "test_config.yaml"
            _write_basic_pipeline_config(config_file, output_dir)
            _write_stage_out_verification(output_dir, "batch_001", "verified")
            _write_stage_out_verification(output_dir, "batch_002", "verified")
            monkeypatch.setattr(
                cli_module,
                "_build_scratch_batches",
                lambda *args, **kwargs: _two_batch_scratch_batches(),
            )
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
                lambda cmd: SimpleNamespace(returncode=0),
            )
            cleanup_calls = []

            def fake_cleanup(batch_work_dir, work_root):
                cleanup_calls.append(batch_work_dir)
                if len(cleanup_calls) == 1:
                    raise RuntimeError("cleanup refused")

            monkeypatch.setattr(cli_module, "_cleanup_batch_work_dir", fake_cleanup)

            parser = build_parser()
            args = parser.parse_args(
                [
                    "pipeline",
                    str(config_file),
                    "--continue-on-batch-failure",
                ]
            )
            with pytest.raises(SystemExit) as exc_info:
                cmd_pipeline(args)

            assert exc_info.value.code == 1
            assert len(cleanup_calls) == 2
            with open(output_dir / "batch_status.csv", newline="") as f:
                rows = list(csv.DictReader(f))
            assert rows[0]["cleanup_status"] == "failed"
            assert rows[1]["cleanup_status"] == "cleaned"
            assert "cleanup refused" in (output_dir / "run_report.md").read_text()

    def test_pipeline_stops_when_batch_no_longer_fits_scratch(
        self, tmp_path, monkeypatch
    ):
        """Each batch should be rechecked against current scratch before launch."""
        output_dir = tmp_path / "out"
        config_file = tmp_path / "test_config.yaml"
        _write_basic_pipeline_config(
            config_file,
            output_dir,
            extra=(
                "plates:\n"
                "  - plate-a\n"
                "plate_sizes_gb:\n"
                "  plate-a: 10\n"
            ),
        )
        quotas = iter(
            [
                batch_module.ScratchQuota(100, 0, 100),
                batch_module.ScratchQuota(100, 99, 1),
            ]
        )
        monkeypatch.setattr(
            batch_module,
            "get_scratch_quota",
            lambda config_quota=None: next(quotas),
        )
        monkeypatch.setattr(
            cli_module,
            "_build_scratch_batches",
            lambda *args, **kwargs: [
                {
                    "batch_id": 1,
                    "plates": ["plate-a"],
                    "total_size_gb": 5,
                    "plate_count": 1,
                }
            ],
        )
        monkeypatch.setattr(cli_module, "_find_nextflow", lambda: True)
        run_calls = []
        monkeypatch.setattr(
            cli_module.subprocess,
            "run",
            lambda cmd: run_calls.append(cmd) or SimpleNamespace(returncode=0),
        )

        parser = build_parser()
        args = parser.parse_args(["pipeline", str(config_file)])
        with pytest.raises(SystemExit) as exc_info:
            cmd_pipeline(args)
        assert exc_info.value.code == 1
        assert not run_calls

        ledger_path = output_dir / "batch_status.csv"
        with open(ledger_path, newline="") as f:
            rows = list(csv.DictReader(f))
        assert rows[-1]["nextflow_status"] == "not_started"
        assert rows[-1]["cleanup_status"] == "not_requested"
        assert "stopping before submission" in rows[-1]["message"]

    def test_pipeline_writes_batch_status_ledger(self, tmp_path, monkeypatch):
        """Successful cleanup should record a complete batch ledger row."""
        output_dir = tmp_path / "out"
        config_file = tmp_path / "test_config.yaml"
        _write_basic_pipeline_config(config_file, output_dir)
        work_dir = output_dir / "work" / "batch_001"
        evidence_dir = _write_stage_out_verification(
            output_dir, "batch_001", "verified"
        )
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
            lambda cmd: SimpleNamespace(returncode=0),
        )

        parser = build_parser()
        args = parser.parse_args(
            ["pipeline", str(config_file), "--cleanup-policy", "verified"]
        )
        cmd_pipeline(args)

        ledger_path = output_dir / "batch_status.csv"
        with open(ledger_path, newline="") as f:
            rows = list(csv.DictReader(f))
        assert len(rows) == 1
        row = rows[0]
        assert row["batch_id"] == "1"
        assert row["batch_name"] == "batch_001"
        assert row["nextflow_status"] == "success"
        assert row["stage_out_status"] == "verified"
        assert row["cleanup_policy"] == "verified"
        assert row["cleanup_status"] == "cleaned"
        assert row["work_dir"] == str(work_dir)
        assert str(evidence_dir) in row["message"]
        assert row["params_path"].endswith("params.batch_1.json")
        assert row["trace_path"].endswith("trace.batch_001.txt")
        assert row["attempt"] == "1"
        assert row["started_at"]
        assert row["finished_at"]
        assert row["updated_at"]

    def test_cleanup_rejects_targets_outside_work_root(self, tmp_path):
        """Cleanup helper must refuse paths outside the configured work root."""
        from cptools2.__main__ import _cleanup_batch_work_dir

        work_root = tmp_path / "work"
        target = tmp_path / "other" / "batch_001"

        with pytest.raises(ValueError, match="outside"):
            _cleanup_batch_work_dir(target, work_root)

    def test_cleanup_rejects_non_batch_targets(self, tmp_path):
        """Cleanup helper must refuse work dirs without a batch_### component."""
        from cptools2.__main__ import _cleanup_batch_work_dir

        work_root = tmp_path / "work"
        target = work_root / "scratch"

        with pytest.raises(ValueError, match="batch_###"):
            _cleanup_batch_work_dir(target, work_root)

    def test_cleanup_falls_back_to_direct_deletion_for_valid_batch_dir(
        self, tmp_path, monkeypatch
    ):
        """Cleanup fallback is restricted to the exact batch dir."""
        from cptools2.__main__ import _cleanup_batch_work_dir

        work_root = tmp_path / "work"
        target = work_root / "batch_001"
        target.mkdir(parents=True)
        calls = []

        def fake_run(cmd):
            calls.append(cmd)
            return SimpleNamespace(returncode=1)

        monkeypatch.setattr(cli_module.subprocess, "run", fake_run)
        monkeypatch.setattr(
            cli_module.shutil, "rmtree", lambda path: calls.append(path)
        )

        _cleanup_batch_work_dir(target, work_root)

        assert calls[0][0:2] == ["nextflow", "clean"]
        assert calls[0][calls[0].index("-work-dir") + 1] == str(target)
        assert calls[1] == str(target)


class TestConfigFileValidation:
    """Test config file validation."""

    def test_missing_config_raises(self):
        from cptools2.__main__ import _check_config_file

        with pytest.raises(FileNotFoundError):
            _check_config_file("/nonexistent/path/config.yaml")

    def test_clean_work_uses_success_policy(self):
        from cptools2.__main__ import _resolve_cleanup_policy

        args = SimpleNamespace(clean_work=True, cleanup_policy="verified")
        assert _resolve_cleanup_policy(args) == "success"


def test_cmd_join_calls_join_plate_files(tmp_path):
    """cmd_join calls file_tools.join_plate_files with correct args."""
    import argparse
    from unittest.mock import patch

    from cptools2.__main__ import cmd_join

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
