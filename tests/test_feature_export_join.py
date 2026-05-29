import csv
from pathlib import Path
from uuid import uuid4

import pytest

from cptools2.feature_export.join import stage_feature_payloads


def _make_payload(root: Path, name: str, content: str = "npz") -> Path:
    payload = root / name / "features"
    payload.mkdir(parents=True, exist_ok=False)
    (payload / "shared.npz").write_text(content, encoding="utf-8")
    return payload


def test_stage_feature_payloads_uses_chunk_scoped_targets_for_repeated_features_dirs():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    source_root = tmp_path / "source"
    export_input_dir = tmp_path / "export_input"
    payload_a = _make_payload(source_root, "chunk-a", content="a")
    payload_b = _make_payload(source_root, "chunk-b", content="b")

    staged = stage_feature_payloads(
        plate_id="plate-001",
        chunk_ids=["chunk-001", "chunk-002"],
        feature_dirs=[payload_a, payload_b],
        export_input_dir=export_input_dir,
    )

    assert staged == (
        export_input_dir / "chunk_chunk-001" / "features",
        export_input_dir / "chunk_chunk-002" / "features",
    )
    assert (staged[0] / "shared.npz").read_text(encoding="utf-8") == "a"
    assert (staged[1] / "shared.npz").read_text(encoding="utf-8") == "b"
    manifest = export_input_dir / "export_input_manifest.csv"
    assert manifest.is_file()
    with manifest.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows == [
        {
            "plate_id": "plate-001",
            "chunk_id": "chunk-001",
            "source_feature_dir": str(payload_a),
            "staged_feature_dir": str(staged[0]),
        },
        {
            "plate_id": "plate-001",
            "chunk_id": "chunk-002",
            "source_feature_dir": str(payload_b),
            "staged_feature_dir": str(staged[1]),
        },
    ]


def test_stage_feature_payloads_rejects_duplicate_chunk_ids_before_copy():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    source_root = tmp_path / "source"
    export_input_dir = tmp_path / "export_input"
    payload_a = _make_payload(source_root, "chunk-a", content="a")
    payload_b = _make_payload(source_root, "chunk-b", content="b")

    with pytest.raises(ValueError, match="Duplicate chunk_ids are not allowed"):
        stage_feature_payloads(
            plate_id="plate-001",
            chunk_ids=["chunk-001", "chunk-001"],
            feature_dirs=[payload_a, payload_b],
            export_input_dir=export_input_dir,
        )

    assert not export_input_dir.exists()


def test_stage_feature_payloads_rejects_missing_payload_directory():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    source_root = tmp_path / "source"
    export_input_dir = tmp_path / "export_input"
    payload_a = _make_payload(source_root, "chunk-a", content="a")
    missing_payload = source_root / "chunk-b" / "features"

    with pytest.raises(FileNotFoundError, match="Missing feature payload directory"):
        stage_feature_payloads(
            plate_id="plate-001",
            chunk_ids=["chunk-001", "chunk-002"],
            feature_dirs=[payload_a, missing_payload],
            export_input_dir=export_input_dir,
        )


def test_stage_feature_payloads_replaces_stale_chunk_payload_on_rerun():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    source_root = tmp_path / "source"
    export_input_dir = tmp_path / "export_input"
    payload = _make_payload(source_root, "chunk-a", content="fresh")

    stale_target = export_input_dir / "chunk_chunk-001" / "features"
    stale_target.mkdir(parents=True, exist_ok=True)
    (stale_target / "stale.txt").write_text("stale", encoding="utf-8")

    staged = stage_feature_payloads(
        plate_id="plate-001",
        chunk_ids=["chunk-001"],
        feature_dirs=[payload],
        export_input_dir=export_input_dir,
    )

    assert staged == (stale_target,)
    assert (stale_target / "shared.npz").read_text(encoding="utf-8") == "fresh"
    assert not (stale_target / "stale.txt").exists()


def test_stage_feature_payloads_rejects_empty_plate_id_before_staging():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    source_root = tmp_path / "source"
    export_input_dir = tmp_path / "export_input"
    payload = _make_payload(source_root, "chunk-a", content="fresh")

    with pytest.raises(ValueError, match="plate_id must not be empty"):
        stage_feature_payloads(
            plate_id="   ",
            chunk_ids=["chunk-001"],
            feature_dirs=[payload],
            export_input_dir=export_input_dir,
        )

    assert not export_input_dir.exists()
