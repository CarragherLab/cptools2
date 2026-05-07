import csv
import builtins
import importlib
import json
import os
import sys
from pathlib import Path

import polars as pl
import pytest

from cptools2 import nextflow_chunking


CURRENT_PATH = os.path.dirname(__file__)
PLATE_DIR = os.path.join(CURRENT_PATH, "example_dir", "test-plate-1")


def _write_deepprofiler_manifest(path, image_root):
    image_root = Path(image_root)
    image_root.mkdir(parents=True, exist_ok=True)
    rows = []
    for channel, suffix in [("1", "w1"), ("2", "w2")]:
        image_path = image_root / f"B02_s1_{suffix}.tif"
        image_path.write_bytes(b"fake-tiff")
        rows.append(
            {
                "plate": "tiny-plate-001",
                "well": "B02",
                "site": "1",
                "channel": channel,
                "image_path": str(image_path),
                "image_set_id": "tiny-plate-001_B02_s1",
                "source_plate_path": str(image_root),
                "chunk_id": "tiny-plate-001_chunk_0001",
                "chunk_number": "1",
            }
        )
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)


def _append_deepprofiler_manifest_site(path, image_root, well="B03", site="2"):
    image_root = Path(image_root)
    with open(path, "a", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "plate",
                "well",
                "site",
                "channel",
                "image_path",
                "image_set_id",
                "source_plate_path",
                "chunk_id",
                "chunk_number",
            ],
        )
        for channel, suffix in [("1", "w1"), ("2", "w2")]:
            image_path = image_root / f"{well}_s{site}_{suffix}.tif"
            image_path.write_bytes(b"fake-tiff")
            writer.writerow(
                {
                    "plate": "tiny-plate-001",
                    "well": well,
                    "site": site,
                    "channel": channel,
                    "image_path": str(image_path),
                    "image_set_id": f"tiny-plate-001_{well}_s{site}",
                    "source_plate_path": str(image_root),
                    "chunk_id": "tiny-plate-001_chunk_0001",
                    "chunk_number": "1",
                }
            )


def _write_cellpose_locations(path, rows=None):
    if rows is None:
        rows = [
            {
                "plate": "tiny-plate-001",
                "well": "B02",
                "site": "1",
                "image_path": "/scratch/tiny-plate-001/B02_s1_w1.tif",
                "image_set_id": "tiny-plate-001_B02_s1",
                "object_id": "1",
                "x": "12.5",
                "y": "31.25",
                "mask_path": "cellpose_masks/tiny-plate-001_B02_s1_cp_masks.tif",
            }
        ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                rows[0].keys()
                if rows
                else [
                    "plate",
                    "well",
                    "site",
                    "image_path",
                    "image_set_id",
                    "object_id",
                    "x",
                    "y",
                    "mask_path",
                ]
            ),
        )
        writer.writeheader()
        if rows:
            writer.writerows(rows)


def _write_deepprofiler_config(path):
    path.write_text(
        json.dumps(
            {
                "dataset": {
                    "metadata": {
                        "label_field": "Metadata_Compound",
                        "control_value": "DMSO",
                    },
                    "images": {
                        "channels": ["DNA", "RNA"],
                        "file_format": "tif",
                    },
                    "locations": {"mode": "single_cells"},
                }
            }
        )
    )


def test_nextflow_chunking_caps_polars_thread_pools():
    assert os.environ["POLARS_MAX_THREADS"] == "1"
    assert os.environ["RAYON_NUM_THREADS"] == "1"
    assert os.environ["OMP_NUM_THREADS"] == "1"
    assert os.environ["MKL_NUM_THREADS"] == "1"


def test_nextflow_chunking_import_does_not_require_parserix(monkeypatch):
    original_module = sys.modules.get("cptools2.nextflow_chunking")
    sys.modules.pop("cptools2.nextflow_chunking", None)
    real_import = builtins.__import__

    def guarded_import(name, *args, **kwargs):
        if name.startswith("parserix"):
            raise ModuleNotFoundError("No module named 'parserix'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    try:
        module = importlib.import_module("cptools2.nextflow_chunking")
        assert hasattr(module, "build_deepprofiler_input_package")
    finally:
        if original_module is not None:
            sys.modules["cptools2.nextflow_chunking"] = original_module


def test_build_image_set_index_preserves_complete_channel_groups(tmp_path):
    output_csv = tmp_path / "image_sets.csv"

    nextflow_chunking.build_image_set_index(
        "test-plate-1",
        PLATE_DIR,
        output_csv,
        expected_channels="1,2,3,4,5",
    )

    df = pl.read_csv(output_csv)
    assert {
        "plate",
        "well",
        "site",
        "channel",
        "image_path",
        "image_set_id",
        "source_plate_path",
    }.issubset(df.columns)
    groups = df.group_by("image_set_id").agg(pl.col("channel").unique())
    assert groups.height == 360
    assert all(sorted(row) == [1, 2, 3, 4, 5] for row in groups["channel"])


def test_build_image_set_index_fails_on_missing_required_channel(tmp_path):
    output_csv = tmp_path / "image_sets.csv"

    with pytest.raises(nextflow_chunking.ChunkingError, match="Incomplete"):
        nextflow_chunking.build_image_set_index(
            "test-plate-1",
            PLATE_DIR,
            output_csv,
            expected_channels="1,2,3,4,5,6",
        )


def test_split_image_set_index_writes_96_image_set_chunks(tmp_path):
    index_csv = tmp_path / "image_sets.csv"
    chunks_dir = tmp_path / "chunks"
    nextflow_chunking.build_image_set_index(
        "test-plate-1",
        PLATE_DIR,
        index_csv,
        expected_channels="1,2,3,4,5",
    )

    written = nextflow_chunking.split_image_set_index(
        index_csv,
        chunks_dir,
        chunk_size=96,
    )

    assert len(written) == 4
    first = pl.read_csv(written[0])
    last = pl.read_csv(written[-1])
    assert first["image_set_id"].n_unique() == 96
    assert last["image_set_id"].n_unique() == 72
    assert "chunk_id" in first.columns


def test_split_image_set_index_respects_max_chunks(tmp_path):
    index_csv = tmp_path / "image_sets.csv"
    chunks_dir = tmp_path / "chunks"
    nextflow_chunking.build_image_set_index(
        "test-plate-1",
        PLATE_DIR,
        index_csv,
        expected_channels="1,2,3,4,5",
    )

    written = nextflow_chunking.split_image_set_index(
        index_csv,
        chunks_dir,
        chunk_size=96,
        max_chunks=2,
    )

    assert len(written) == 2
    assert [os.path.basename(path) for path in written] == [
        "test-plate-1_chunk_0001.csv",
        "test-plate-1_chunk_0002.csv",
    ]


def test_build_deepprofiler_input_package_writes_complete_inputs_package(tmp_path):
    chunk_manifest = tmp_path / "tiny-plate-001_chunk_0001.csv"
    image_root = tmp_path
    locations_dir = tmp_path / "locations"
    output_root = tmp_path / "dp_project_inputs"
    config_path = tmp_path / "deepprofiler_config.json"

    _write_deepprofiler_manifest(chunk_manifest, image_root)
    _write_cellpose_locations(
        locations_dir / "tiny-plate-001_chunk_0001_locations.csv"
    )
    _write_deepprofiler_config(config_path)

    nextflow_chunking.build_deepprofiler_input_package(
        chunk_manifest=chunk_manifest,
        locations_dir=locations_dir,
        output_root=output_root,
        config_path=config_path,
    )

    image_path = output_root / "images" / "tiny-plate-001" / "B02_s1_w1.tif"
    assert image_path.exists()
    assert image_path.read_bytes() == b"fake-tiff"

    index_df = pl.read_csv(output_root / "metadata" / "index.csv")
    assert index_df.columns == [
        "Metadata_Plate",
        "Metadata_Well",
        "Metadata_Site",
        "Metadata_Compound",
        "Metadata_ImageSet",
        "DNA",
        "RNA",
    ]
    assert index_df.height == 1
    row = index_df.row(0, named=True)
    assert row["Metadata_Plate"] == "tiny-plate-001"
    assert row["Metadata_Well"] == "B02"
    assert str(row["Metadata_Site"]) == "1"
    assert row["Metadata_Compound"] == "DMSO"
    assert row["Metadata_ImageSet"] == "tiny-plate-001_B02_s1"
    assert row["DNA"] == "tiny-plate-001/B02_s1_w1.tif"
    assert row["RNA"] == "tiny-plate-001/B02_s1_w2.tif"

    nuclei_df = pl.read_csv(
        output_root / "locations" / "tiny-plate-001" / "B02-f1-Nuclei.csv"
    )
    assert nuclei_df.columns == [
        "Nuclei_Location_Center_X",
        "Nuclei_Location_Center_Y",
    ]
    assert nuclei_df.to_dicts() == [
        {
            "Nuclei_Location_Center_X": 12.5,
            "Nuclei_Location_Center_Y": 31.25,
        }
    ]

    copied_config = json.loads((output_root / "config" / "config.json").read_text())
    assert copied_config == json.loads(config_path.read_text())


def test_build_deepprofiler_input_package_fails_when_config_channel_is_missing(
    tmp_path,
):
    chunk_manifest = tmp_path / "tiny-plate-001_chunk_0001.csv"
    image_root = tmp_path
    locations_dir = tmp_path / "locations"
    output_root = tmp_path / "dp_project_inputs"
    config_path = tmp_path / "deepprofiler_config.json"

    _write_deepprofiler_manifest(chunk_manifest, image_root)
    _write_cellpose_locations(locations_dir / "tiny-plate-001_locations.csv")
    config = json.loads(
        json.dumps(
            {
                "dataset": {
                    "metadata": {
                        "label_field": "Metadata_Compound",
                        "control_value": "DMSO",
                    },
                    "images": {
                        "channels": ["DNA", "RNA", "ER"],
                        "file_format": "tif",
                    },
                }
            }
        )
    )
    config_path.write_text(json.dumps(config))

    with pytest.raises(nextflow_chunking.ChunkingError, match="Missing channel"):
        nextflow_chunking.build_deepprofiler_input_package(
            chunk_manifest=chunk_manifest,
            locations_dir=locations_dir,
            output_root=output_root,
            config_path=config_path,
        )


def test_build_deepprofiler_input_package_allows_zero_locations(tmp_path):
    chunk_manifest = tmp_path / "tiny-plate-001_chunk_0001.csv"
    image_root = tmp_path
    locations_dir = tmp_path / "locations"
    output_root = tmp_path / "dp_project_inputs"
    config_path = tmp_path / "deepprofiler_config.json"

    _write_deepprofiler_manifest(chunk_manifest, image_root)
    _write_deepprofiler_config(config_path)
    _write_cellpose_locations(
        locations_dir / "tiny-plate-001_chunk_0001_locations.csv",
        rows=[],
    )

    nextflow_chunking.build_deepprofiler_input_package(
        chunk_manifest=chunk_manifest,
        locations_dir=locations_dir,
        output_root=output_root,
        config_path=config_path,
    )

    nuclei_path = output_root / "locations" / "tiny-plate-001" / "B02-f1-Nuclei.csv"
    assert nuclei_path.exists()
    assert nuclei_path.read_text().strip() == (
        "Nuclei_Location_Center_X,Nuclei_Location_Center_Y"
    )


def test_build_deepprofiler_input_package_fails_when_locations_are_missing(
    tmp_path,
):
    chunk_manifest = tmp_path / "tiny-plate-001_chunk_0001.csv"
    image_root = tmp_path
    output_root = tmp_path / "dp_project_inputs"
    config_path = tmp_path / "deepprofiler_config.json"

    _write_deepprofiler_manifest(chunk_manifest, image_root)
    _write_deepprofiler_config(config_path)

    with pytest.raises(nextflow_chunking.ChunkingError, match="locations directory"):
        nextflow_chunking.build_deepprofiler_input_package(
            chunk_manifest=chunk_manifest,
            locations_dir=tmp_path / "missing-locations",
            output_root=output_root,
            config_path=config_path,
        )


def test_build_deepprofiler_input_package_writes_empty_file_for_site_without_cells(
    tmp_path,
):
    chunk_manifest = tmp_path / "tiny-plate-001_chunk_0001.csv"
    image_root = tmp_path
    locations_dir = tmp_path / "locations"
    output_root = tmp_path / "dp_project_inputs"
    config_path = tmp_path / "deepprofiler_config.json"

    _write_deepprofiler_manifest(chunk_manifest, image_root)
    _append_deepprofiler_manifest_site(chunk_manifest, image_root)
    _write_cellpose_locations(
        locations_dir / "tiny-plate-001_chunk_0001_locations.csv"
    )
    _write_deepprofiler_config(config_path)

    nextflow_chunking.build_deepprofiler_input_package(
        chunk_manifest=chunk_manifest,
        locations_dir=locations_dir,
        output_root=output_root,
        config_path=config_path,
    )

    populated = output_root / "locations" / "tiny-plate-001" / "B02-f1-Nuclei.csv"
    empty = output_root / "locations" / "tiny-plate-001" / "B03-f2-Nuclei.csv"
    assert pl.read_csv(populated).height == 1
    assert empty.read_text().strip() == (
        "Nuclei_Location_Center_X,Nuclei_Location_Center_Y"
    )


def test_build_deepprofiler_input_package_fails_without_chunk_location_csv(
    tmp_path,
):
    chunk_manifest = tmp_path / "tiny-plate-001_chunk_0001.csv"
    image_root = tmp_path
    locations_dir = tmp_path / "locations"
    output_root = tmp_path / "dp_project_inputs"
    config_path = tmp_path / "deepprofiler_config.json"

    _write_deepprofiler_manifest(chunk_manifest, image_root)
    _write_cellpose_locations(locations_dir / "other_chunk_locations.csv")
    _write_deepprofiler_config(config_path)

    with pytest.raises(nextflow_chunking.ChunkingError, match="for chunk"):
        nextflow_chunking.build_deepprofiler_input_package(
            chunk_manifest=chunk_manifest,
            locations_dir=locations_dir,
            output_root=output_root,
            config_path=config_path,
        )


def test_build_parser_registers_deepprofiler_package_subcommand():
    parser = nextflow_chunking.build_parser()
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


def test_parse_channel_falls_back_to_cellprofiler_channel_names():
    assert nextflow_chunking._parse_channel("val screen_B02_s1_DNA.tif") == 1
    assert nextflow_chunking._parse_channel("val screen_B02_s1_Mito.tif") == 5
