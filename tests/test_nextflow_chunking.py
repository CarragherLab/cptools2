import os

import polars as pl
import pytest

from cptools2 import nextflow_chunking


CURRENT_PATH = os.path.dirname(__file__)
PLATE_DIR = os.path.join(CURRENT_PATH, "example_dir", "test-plate-1")


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
