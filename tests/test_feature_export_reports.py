import csv
from pathlib import Path
from uuid import uuid4

from cptools2.feature_export.reports import (
    write_feature_export_summary,
    write_plate_export_report,
)


def _make_output_dir() -> Path:
    output_dir = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    output_dir.mkdir(parents=True, exist_ok=False)
    return output_dir


def _read_rows(path: Path):
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_write_plate_export_report_writes_success_and_failure_artifacts():
    output_dir = _make_output_dir()

    success_dir = output_dir / "plate-success"
    success_status_path, success_readme_path = write_plate_export_report(
        output_dir=success_dir,
        plate_id="plate-success",
        export_status="exported",
        failure_stage="",
        failure_reason="",
        tables_present=True,
        manifest_path="export_input_manifest.csv",
    )

    success_rows = _read_rows(success_status_path)
    success_readme = success_readme_path.read_text(encoding="utf-8")

    assert success_status_path == success_dir / "feature_export_status.csv"
    assert success_readme_path == success_dir / "README.md"
    assert success_rows == [
        {
            "plate_id": "plate-success",
            "export_status": "exported",
            "failure_stage": "",
            "failure_reason": "",
            "tables_present": "true",
            "manifest_path": "export_input_manifest.csv",
        }
    ]
    assert "Validated export tables are available" in success_readme
    assert "tables/" in success_readme
    assert "export_input_manifest.csv" in success_readme

    failure_dir = output_dir / "plate-failed"
    failure_status_path, failure_readme_path = write_plate_export_report(
        output_dir=failure_dir,
        plate_id="plate-failed",
        export_status="failed",
        failure_stage="plate_identity_validation",
        failure_reason=(
            "Requested plate_id plate-failed does not match exported plate(s): "
            "plate-002"
        ),
        tables_present=False,
        manifest_path="export_input_manifest.csv",
    )

    failure_rows = _read_rows(failure_status_path)
    failure_readme = failure_readme_path.read_text(encoding="utf-8")

    assert failure_status_path == failure_dir / "feature_export_status.csv"
    assert failure_readme_path == failure_dir / "README.md"
    assert failure_rows == [
        {
            "plate_id": "plate-failed",
            "export_status": "failed",
            "failure_stage": "plate_identity_validation",
            "failure_reason": (
                "Requested plate_id plate-failed does not match exported plate(s): "
                "plate-002"
            ),
            "tables_present": "false",
            "manifest_path": "export_input_manifest.csv",
        }
    ]
    assert "No measurement tables were published for this plate." in failure_readme
    assert "plate_identity_validation" in failure_readme
    assert "plate-002" in failure_readme
    assert "export_input_manifest.csv" in failure_readme
    assert not (failure_dir / "tables").exists()


def test_write_feature_export_summary_marks_failed_and_missing_status():
    output_dir = _make_output_dir()

    success_status_path, _ = write_plate_export_report(
        output_dir=output_dir / "plate-success",
        plate_id="plate-success",
        export_status="exported",
        failure_stage="",
        failure_reason="",
        tables_present=True,
        manifest_path="export_input_manifest.csv",
    )
    failed_status_path, _ = write_plate_export_report(
        output_dir=output_dir / "plate-failed",
        plate_id="plate-failed",
        export_status="failed",
        failure_stage="plate_identity_validation",
        failure_reason=(
            "Requested plate_id plate-failed does not match exported plate(s): "
            "plate-002"
        ),
        tables_present=False,
        manifest_path="export_input_manifest.csv",
    )

    summary_dir = output_dir / "feature_export_summary"
    summary_status_path, summary_readme_path = write_feature_export_summary(
        output_dir=summary_dir,
        expected_plate_ids=[
            "plate-success",
            "plate-failed",
            "plate-missing",
        ],
        observed_status_paths=[success_status_path, failed_status_path],
    )

    summary_rows = _read_rows(summary_status_path)
    summary_readme = summary_readme_path.read_text(encoding="utf-8")

    assert summary_status_path == summary_dir / "feature_export_summary.csv"
    assert summary_readme_path == summary_dir / "README.md"
    assert [row["plate_id"] for row in summary_rows] == [
        "plate-success",
        "plate-failed",
        "plate-missing",
    ]
    assert summary_rows[0]["export_status"] == "exported"
    assert summary_rows[1]["export_status"] == "failed"
    assert summary_rows[2]["export_status"] == "missing_status"
    assert summary_rows[2]["failure_stage"] == "missing_status"
    assert summary_rows[2]["tables_present"] == "false"
    assert summary_rows[2]["failure_reason"] == (
        "No export status artifact was produced for plate plate-missing"
    )
    assert "Successful plates: 1" in summary_readme
    assert "Failed plates: 1" in summary_readme
    assert "Missing status plates: 1" in summary_readme
    assert "plate-failed" in summary_readme
    assert "plate-missing" in summary_readme
