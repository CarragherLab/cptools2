from pathlib import Path
from uuid import uuid4

import polars as pl
import pytest

from cptools2.feature_export.writers import write_table


def test_write_table_writes_csv():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    rows = [
        {
            "Metadata_Plate": "plate-001",
            "Metadata_Well": "A01",
            "DeepProfiler_Feature_0000": 0.25,
        }
    ]

    written = write_table(
        rows=rows,
        output_stem=tmp_path / "deepprofiler_cells",
        formats=("csv",),
        columns=["Metadata_Plate", "Metadata_Well", "DeepProfiler_Feature_0000"],
    )

    assert written == (tmp_path / "deepprofiler_cells.csv",)
    df = pl.read_csv(written[0])
    assert df["Metadata_Well"].to_list() == ["A01"]
    assert df["DeepProfiler_Feature_0000"].to_list() == [0.25]


def test_write_table_writes_empty_csv_with_headers():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    written = write_table(
        rows=[],
        output_stem=tmp_path / "deepprofiler_cells",
        formats=("csv",),
        columns=["Metadata_Plate", "Metadata_Well"],
    )

    text = Path(written[0]).read_text(encoding="utf-8")
    assert text == "Metadata_Plate,Metadata_Well\n"


def test_write_table_csv_does_not_require_polars_dataframe(monkeypatch):
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)

    def fail_dataframe(*args, **kwargs):
        raise AssertionError("CSV writing should not build a Polars DataFrame")

    monkeypatch.setattr("cptools2.feature_export.writers.pl.DataFrame", fail_dataframe)

    written = write_table(
        rows=[{"Metadata_Plate": "plate-001", "Metadata_Well": "A01"}],
        output_stem=tmp_path / "deepprofiler_manifest",
        formats=("csv",),
        columns=["Metadata_Plate", "Metadata_Well"],
    )

    assert written == (tmp_path / "deepprofiler_manifest.csv",)
    assert written[0].read_text(encoding="utf-8") == (
        "Metadata_Plate,Metadata_Well\nplate-001,A01\n"
    )


def test_write_table_writes_optional_parquet():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    rows = [{"Metadata_Plate": "plate-001", "Metadata_Well": "A01"}]

    written = write_table(
        rows=rows,
        output_stem=tmp_path / "deepprofiler_manifest",
        formats=("csv", "parquet"),
        columns=["Metadata_Plate", "Metadata_Well"],
    )

    assert written == (
        tmp_path / "deepprofiler_manifest.csv",
        tmp_path / "deepprofiler_manifest.parquet",
    )
    assert pl.read_parquet(written[1]).height == 1


def test_write_table_rejects_empty_rows_without_columns():
    tmp_path = Path(__file__).resolve().parents[1] / ".test-output" / uuid4().hex
    tmp_path.mkdir(parents=True, exist_ok=False)
    with pytest.raises(ValueError, match="columns are required"):
        write_table(
            rows=[],
            output_stem=tmp_path / "empty",
            formats=("csv",),
        )
