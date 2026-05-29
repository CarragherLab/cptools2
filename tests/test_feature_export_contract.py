from pathlib import Path
from uuid import uuid4

import pytest

from cptools2.feature_export.contract import (
    CELL_TABLE,
    DEFAULT_FORMATS,
    MANIFEST_TABLE,
    QUALITY_TABLE,
    REQUIRED_COMMON_METADATA,
    SITE_TABLE,
    FeatureExportRequest,
    normalise_formats,
    published_table_name,
)


def test_table_names_are_fixed():
    assert MANIFEST_TABLE == "deepprofiler_manifest"
    assert SITE_TABLE == "deepprofiler_sites"
    assert CELL_TABLE == "deepprofiler_cells"
    assert QUALITY_TABLE == "deepprofiler_quality"


def test_required_metadata_columns_use_metadata_prefix():
    assert REQUIRED_COMMON_METADATA == (
        "Metadata_Plate",
        "Metadata_Well",
        "Metadata_Site",
        "Metadata_ImageSet",
        "Metadata_Object",
        "Metadata_SourceFile",
        "Metadata_FeatureExtractor",
    )
    assert all(column.startswith("Metadata_") for column in REQUIRED_COMMON_METADATA)


def test_normalise_formats_defaults_to_csv_only():
    assert DEFAULT_FORMATS == ("csv",)
    assert normalise_formats(None) == ("csv",)


def test_normalise_formats_accepts_csv_and_parquet_once():
    assert normalise_formats(["CSV", "parquet", "csv"]) == ("csv", "parquet")


def test_normalise_formats_rejects_unknown_format():
    with pytest.raises(ValueError, match="Unsupported feature export format"):
        normalise_formats(["xlsx"])


def test_request_tables_dir_is_output_tables():
    output_dir = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    output_dir.mkdir(parents=True, exist_ok=False)
    request = FeatureExportRequest(
        extractor="deepprofiler",
        features_dir=output_dir / "features",
        output_dir=output_dir / "export",
        formats=("csv",),
    )

    assert request.tables_dir == output_dir / "export" / "tables"


def test_request_rejects_invalid_nan_object_threshold():
    with pytest.raises(ValueError, match="nan_object_fail_fraction"):
        FeatureExportRequest(
            extractor="deepprofiler",
            features_dir=Path("features"),
            output_dir=Path("export"),
            nan_object_fail_fraction=1.5,
        )


def test_published_table_name_includes_plate_and_optional_run_label():
    assert (
        published_table_name("plate-001", MANIFEST_TABLE)
        == "plate-001__deepprofiler_manifest.csv"
    )
    assert (
        published_table_name("plate-001", CELL_TABLE, run_label="loop510")
        == "plate-001__loop510__deepprofiler_cells.csv"
    )
