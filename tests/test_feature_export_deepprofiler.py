import csv
from pathlib import Path
from uuid import uuid4

import numpy as np
import pytest

from cptools2.feature_export.contract import FeatureExportRequest
from cptools2.feature_export.deepprofiler import export_deepprofiler, inspect_npz


def test_inspect_npz_detects_cell_matrix_and_quality():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    path = tmp_path / "A01" / "1.npz"
    path.parent.mkdir()
    features = np.array([[1.0, 2.0], [3.0, np.nan]], dtype="float32")
    np.savez(
        path,
        features=features,
        metadata=np.array({"well": "A01"}, dtype=object),
        locations=np.array([[10.0, 20.0], [30.0, 40.0]], dtype="float32"),
    )

    schema = inspect_npz(path)

    assert schema.kind == "cell"
    assert schema.feature_key == "features"
    assert schema.n_rows == 2
    assert schema.n_features == 2
    assert schema.nan_count == 1
    assert schema.nan_object_count == 1
    assert schema.value_count == 4
    assert schema.has_nan is True
    assert schema.all_nan is False


def test_inspect_npz_detects_all_nan_matrix():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    path = tmp_path / "A01" / "2.npz"
    path.parent.mkdir()
    np.savez(
        path,
        features=np.full((2, 3), np.nan, dtype="float32"),
        metadata=np.array({"well": "A01"}, dtype=object),
    )

    schema = inspect_npz(path)

    assert schema.kind == "cell"
    assert schema.nan_count == 6
    assert schema.nan_object_count == 2
    assert schema.value_count == 6
    assert schema.all_nan is True


def test_inspect_npz_reports_missing_features():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    path = tmp_path / "A01" / "3.npz"
    path.parent.mkdir()
    np.savez(path, metadata=np.array({"well": "A01"}, dtype=object))

    schema = inspect_npz(path)

    assert schema.kind == "missing_features"
    assert schema.reason == "No supported feature array key found"


def test_inspect_npz_reports_unsupported_shape():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    path = tmp_path / "A01" / "4.npz"
    path.parent.mkdir()
    np.savez(
        path,
        features=np.zeros((1, 2, 3), dtype="float32"),
        metadata=np.array({"well": "A01"}, dtype=object),
    )

    schema = inspect_npz(path)

    assert schema.kind == "unsupported_schema"
    assert "Expected 1D or 2D feature array" in schema.reason


def test_export_deepprofiler_writes_manifest_sites_cells_and_quality():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    features_dir = tmp_path / "features"
    output_dir = tmp_path / "export"
    path = features_dir / "sarah-representative" / "A01" / "1.npz"
    path.parent.mkdir(parents=True)
    np.savez(
        path,
        features=np.array([[1.0, 2.0], [3.0, np.nan]], dtype="float32"),
        metadata=np.array({"well": "A01"}, dtype=object),
        locations=np.array([[10.0, 20.0], [30.0, 40.0]], dtype="float32"),
    )

    result = export_deepprofiler(
        FeatureExportRequest(
            extractor="deepprofiler",
            features_dir=features_dir,
            output_dir=output_dir,
            formats=("csv",),
        )
    )

    assert result.manifest_rows == 1
    assert result.site_rows == 1
    assert result.cell_rows == 2
    assert result.quality_rows == 1

    cell_rows = list(
        csv.DictReader((output_dir / "tables" / "deepprofiler_cells.csv").open())
    )
    assert cell_rows[0]["Metadata_Plate"] == "sarah-representative"
    assert cell_rows[0]["Metadata_Well"] == "A01"
    assert cell_rows[0]["Metadata_Site"] == "1"
    assert cell_rows[0]["Metadata_Object"] == "1"
    assert cell_rows[0]["Metadata_Center_X"] == "10.0"
    assert cell_rows[0]["Metadata_Center_Y"] == "20.0"
    assert cell_rows[0]["Metadata_Object_NaN_Count"] == "0"
    assert cell_rows[0]["Metadata_Object_QualityStatus"] == "pass"
    assert cell_rows[1]["Metadata_Object_NaN_Count"] == "1"
    assert cell_rows[1]["Metadata_Object_QualityStatus"] == "has_nan"
    assert "DeepProfiler_Feature_0001" in cell_rows[0]
    assert "well" not in cell_rows[0]

    quality_rows = list(
        csv.DictReader((output_dir / "tables" / "deepprofiler_quality.csv").open())
    )
    assert quality_rows[0]["Metadata_NaN_Count"] == "1"
    assert quality_rows[0]["Metadata_NaN_Object_Count"] == "1"
    assert quality_rows[0]["Metadata_NaN_Object_Fraction"] == "0.5"
    assert quality_rows[0]["Metadata_QualityStatus"] == "has_nan"
    assert quality_rows[0]["Metadata_QualityGate"] == "fail_site"
    assert (output_dir / "tables" / "deepprofiler_manifest.csv").exists()
    assert (
        output_dir / "tables" / "sarah-representative__deepprofiler_manifest.csv"
    ).exists()
    assert (
        output_dir / "tables" / "sarah-representative__deepprofiler_cells.csv"
    ).exists()


def test_export_deepprofiler_can_publish_run_label_aliases():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    features_dir = tmp_path / "features"
    output_dir = tmp_path / "export"
    path = features_dir / "plate-001" / "A01" / "1.npz"
    path.parent.mkdir(parents=True)
    np.savez(
        path,
        features=np.array([[1.0, 2.0], [3.0, 4.0]], dtype="float32"),
        locations=np.array([[10.0, 20.0], [30.0, 40.0]], dtype="float32"),
    )

    export_deepprofiler(
        FeatureExportRequest(
            extractor="deepprofiler",
            features_dir=features_dir,
            output_dir=output_dir,
            formats=("csv",),
            plate_id="plate-001",
            run_label="loop510",
        )
    )

    cell_rows = list(
        csv.DictReader((output_dir / "tables" / "deepprofiler_cells.csv").open())
    )
    assert cell_rows[0]["Metadata_Plate"] == "plate-001"
    assert (output_dir / "tables" / "deepprofiler_cells.csv").exists()
    assert (output_dir / "tables" / "plate-001__deepprofiler_cells.csv").exists()
    assert (
        output_dir / "tables" / "plate-001__loop510__deepprofiler_cells.csv"
    ).exists()


def test_export_deepprofiler_accepts_staged_chunk_scoped_input():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    features_dir = tmp_path / "staging"
    output_dir = tmp_path / "export"
    path = features_dir / "chunk_0001" / "features" / "plate-001" / "A01" / "1.npz"
    path.parent.mkdir(parents=True)
    np.savez(
        path,
        features=np.array([[1.0, 2.0], [3.0, 4.0]], dtype="float32"),
        locations=np.array([[10.0, 20.0], [30.0, 40.0]], dtype="float32"),
    )

    export_deepprofiler(
        FeatureExportRequest(
            extractor="deepprofiler",
            features_dir=features_dir,
            output_dir=output_dir,
            formats=("csv",),
            plate_id="plate-001",
            run_label="loop510",
        )
    )

    cell_rows = list(
        csv.DictReader((output_dir / "tables" / "deepprofiler_cells.csv").open())
    )
    assert cell_rows[0]["Metadata_Plate"] == "plate-001"
    assert (
        output_dir / "tables" / "plate-001__loop510__deepprofiler_cells.csv"
    ).exists()


def test_export_deepprofiler_rejects_requested_plate_mismatch():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    features_dir = tmp_path / "features"
    output_dir = tmp_path / "export"
    path = features_dir / "plate-B" / "A01" / "1.npz"
    path.parent.mkdir(parents=True)
    np.savez(path, features=np.array([1.0, 2.0], dtype="float32"))

    with pytest.raises(ValueError, match="Requested mismatch"):
        export_deepprofiler(
            FeatureExportRequest(
                extractor="deepprofiler",
                features_dir=features_dir,
                output_dir=output_dir,
                formats=("csv",),
                plate_id="plate-A",
            )
        )


def test_export_deepprofiler_rejects_mixed_observed_plates_with_request():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    features_dir = tmp_path / "features"
    output_dir = tmp_path / "export"
    plate_a = features_dir / "plate-A" / "A01" / "1.npz"
    plate_b = features_dir / "plate-B" / "A01" / "1.npz"
    plate_a.parent.mkdir(parents=True)
    plate_b.parent.mkdir(parents=True)
    np.savez(plate_a, features=np.array([1.0, 2.0], dtype="float32"))
    np.savez(plate_b, features=np.array([3.0, 4.0], dtype="float32"))

    with pytest.raises(ValueError, match="Expected one plate in DeepProfiler payloads"):
        export_deepprofiler(
            FeatureExportRequest(
                extractor="deepprofiler",
                features_dir=features_dir,
                output_dir=output_dir,
                formats=("csv",),
                plate_id="plate-A",
            )
        )


def test_export_deepprofiler_rejects_flattened_payload_layout():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    features_dir = tmp_path / "features"
    output_dir = tmp_path / "export"
    path = features_dir / "A01" / "1.npz"
    path.parent.mkdir(parents=True)
    np.savez(path, features=np.array([1.0, 2.0], dtype="float32"))

    with pytest.raises(
        ValueError,
        match=r"Expected DeepProfiler payload path features/<plate>/<well>/<site>\.npz",
    ):
        export_deepprofiler(
            FeatureExportRequest(
                extractor="deepprofiler",
                features_dir=features_dir,
                output_dir=output_dir,
                formats=("csv",),
            )
        )


def test_export_deepprofiler_parquet_only_writes_parquet_tables():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    features_dir = tmp_path / "features"
    output_dir = tmp_path / "export"
    path = features_dir / "plate-001" / "A01" / "1.npz"
    path.parent.mkdir(parents=True)
    np.savez(path, features=np.array([1.0, 2.0], dtype="float32"))

    result = export_deepprofiler(
        FeatureExportRequest(
            extractor="deepprofiler",
            features_dir=features_dir,
            output_dir=output_dir,
            formats=("parquet",),
        )
    )

    assert result.manifest_rows == 1
    assert (output_dir / "tables" / "deepprofiler_manifest.parquet").exists()
    assert not (output_dir / "tables" / "deepprofiler_manifest.csv").exists()
    assert (output_dir / "tables" / "deepprofiler_sites.parquet").exists()
    assert not (output_dir / "tables" / "deepprofiler_sites.csv").exists()
    assert (output_dir / "tables" / "deepprofiler_cells.parquet").exists()
    assert not (output_dir / "tables" / "deepprofiler_cells.csv").exists()
    assert (output_dir / "tables" / "deepprofiler_quality.parquet").exists()
    assert not (output_dir / "tables" / "deepprofiler_quality.csv").exists()


def test_export_deepprofiler_all_nan_file_is_reported():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    features_dir = tmp_path / "features"
    output_dir = tmp_path / "export"
    path = features_dir / "plate-001" / "B02" / "6.npz"
    path.parent.mkdir(parents=True)
    np.savez(
        path,
        features=np.full((2, 3), np.nan, dtype="float32"),
        metadata=np.array({"well": "B02"}, dtype=object),
    )

    export_deepprofiler(
        FeatureExportRequest(
            extractor="deepprofiler",
            features_dir=features_dir,
            output_dir=output_dir,
            formats=("csv",),
        )
    )

    quality_rows = list(
        csv.DictReader((output_dir / "tables" / "deepprofiler_quality.csv").open())
    )
    assert quality_rows[0]["Metadata_All_NaN"] == "true"
    assert quality_rows[0]["Metadata_QualityStatus"] == "all_nan"
    assert quality_rows[0]["Metadata_QualityGate"] == "fail_site"


def test_export_deepprofiler_small_nan_object_fraction_is_warning():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    features_dir = tmp_path / "features"
    output_dir = tmp_path / "export"
    path = features_dir / "plate-001" / "B02" / "6.npz"
    path.parent.mkdir(parents=True)
    features = np.ones((10, 3), dtype="float32")
    features[0, :] = np.nan
    np.savez(path, features=features, metadata=np.array({"well": "B02"}, dtype=object))

    export_deepprofiler(
        FeatureExportRequest(
            extractor="deepprofiler",
            features_dir=features_dir,
            output_dir=output_dir,
            formats=("csv",),
            nan_object_fail_fraction=0.2,
        )
    )

    quality_rows = list(
        csv.DictReader((output_dir / "tables" / "deepprofiler_quality.csv").open())
    )
    assert quality_rows[0]["Metadata_NaN_Object_Count"] == "1"
    assert quality_rows[0]["Metadata_NaN_Object_Fraction"] == "0.1"
    assert quality_rows[0]["Metadata_NaN_Object_Fail_Fraction"] == "0.2"
    assert quality_rows[0]["Metadata_QualityStatus"] == "has_nan"
    assert quality_rows[0]["Metadata_QualityGate"] == "warn"


def test_export_deepprofiler_empty_input_writes_header_only_tables():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    features_dir = tmp_path / "features"
    features_dir.mkdir()
    output_dir = tmp_path / "export"

    result = export_deepprofiler(
        FeatureExportRequest(
            extractor="deepprofiler",
            features_dir=features_dir,
            output_dir=output_dir,
            formats=("csv",),
        )
    )

    assert result.manifest_rows == 0
    assert result.site_rows == 0
    assert result.cell_rows == 0
    assert result.quality_rows == 0
    assert (output_dir / "tables" / "deepprofiler_cells.csv").exists()
