"""Helpers used by the Nextflow pipeline to index and chunk staged plates.

These functions do domain work for Nextflow processes. They do not submit jobs.
Nextflow owns orchestration; this module owns ImageXpress parsing and manifest
creation.
"""

import argparse
import os
from pathlib import Path

# Eddie login/local metadata tasks can hit process/thread limits if Polars/Rayon
# tries to create a full worker pool for small CSV writes.
os.environ.setdefault("POLARS_MAX_THREADS", "1")
os.environ.setdefault("RAYON_NUM_THREADS", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

import polars as pl
from parserix import parse as _parse

from cptools2 import filelist


DEFAULT_CHUNK_SIZE = 96


class ChunkingError(Exception):
    """Raised when a staged plate cannot be indexed or chunked safely."""


def _normalise_expected_channels(expected_channels):
    if expected_channels is None:
        return None
    if isinstance(expected_channels, str):
        expected_channels = [
            value.strip()
            for value in expected_channels.split(",")
            if value.strip()
        ]
    channels = []
    for channel in expected_channels:
        try:
            channels.append(int(channel))
        except (TypeError, ValueError):
            channels.append(str(channel))
    return sorted(channels, key=str)


def _parse_channel(filename):
    channel = _parse.img_channel(filename)
    try:
        return int(channel)
    except (TypeError, ValueError):
        return channel


def build_image_set_index(
    plate_id,
    plate_dir,
    output_csv,
    expected_channels=None,
    ext=".tif",
):
    """Index a staged plate into channel-level image-set rows.

    Parameters
    ----------
    plate_id : str
        Plate identifier from the workflow.
    plate_dir : str
        Staged plate directory visible to the current process.
    output_csv : str
        Destination CSV path.
    expected_channels : iterable or comma-separated str, optional
        Required channel ids. Missing channels fail before compute starts.
    ext : str
        Image extension to discover.
    """
    plate_dir = os.path.abspath(plate_dir)
    expected_channels = _normalise_expected_channels(expected_channels)
    _is_new_ix, image_paths = filelist.detect_plate_layout(
        plate_dir,
        ext=ext,
        clean=True,
        truncate=False,
    )

    rows = []
    for image_path in sorted(image_paths):
        filename = _parse.img_filename(image_path)
        well = _parse.img_well(filename)
        site = _parse.img_site(filename)
        channel = _parse_channel(filename)
        image_set_id = f"{plate_id}_{well}_s{site}"
        rows.append(
            {
                "plate": plate_id,
                "well": well,
                "site": str(site),
                "channel": channel,
                "image_path": os.path.abspath(image_path),
                "image_set_id": image_set_id,
                "source_plate_path": plate_dir,
            }
        )

    if not rows:
        raise ChunkingError(f"No images found for plate {plate_id}: {plate_dir}")

    df = pl.DataFrame(rows).sort(["well", "site", "channel", "image_path"])
    _validate_complete_image_sets(df, expected_channels)
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_csv)
    return output_csv


def _validate_complete_image_sets(df, expected_channels=None):
    groups = df.group_by("image_set_id", maintain_order=True).agg(
        pl.col("channel").unique().alias("channels")
    )
    if expected_channels is None:
        channel_counts = groups.with_columns(
            pl.col("channels").list.len().alias("n_channels")
        )["n_channels"].unique().to_list()
        if len(channel_counts) != 1:
            raise ChunkingError(
                "Image sets have inconsistent channel counts: "
                + ", ".join(str(value) for value in sorted(channel_counts))
            )
        return

    expected = set(expected_channels)
    failures = []
    for row in groups.iter_rows(named=True):
        observed = set(row["channels"])
        if observed != expected:
            failures.append(
                "{} expected {} observed {}".format(
                    row["image_set_id"],
                    sorted(expected, key=str),
                    sorted(observed, key=str),
                )
            )
    if failures:
        preview = "; ".join(failures[:5])
        if len(failures) > 5:
            preview += f"; ... {len(failures) - 5} more"
        raise ChunkingError(f"Incomplete image sets detected: {preview}")


def split_image_set_index(
    index_csv,
    output_dir,
    chunk_size=DEFAULT_CHUNK_SIZE,
    max_chunks=None,
):
    """Split an image-set index into per-chunk manifest CSVs."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")
    if max_chunks is not None and max_chunks <= 0:
        raise ValueError("max_chunks must be greater than zero when provided")

    df = pl.read_csv(index_csv)
    required = {
        "plate",
        "well",
        "site",
        "channel",
        "image_path",
        "image_set_id",
    }
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ChunkingError(f"Index CSV missing required columns: {missing}")

    image_sets = (
        df.select(["plate", "well", "site", "image_set_id"])
        .unique(maintain_order=True)
        .sort(["plate", "well", "site"])
    )
    chunk_rows = []
    for idx, row in enumerate(image_sets.iter_rows(named=True)):
        chunk_number = idx // chunk_size + 1
        if max_chunks is not None and chunk_number > max_chunks:
            continue
        chunk_id = f"{row['plate']}_chunk_{chunk_number:04d}"
        chunk_rows.append(
            {
                "image_set_id": row["image_set_id"],
                "chunk_id": chunk_id,
                "chunk_number": chunk_number,
            }
        )

    if not chunk_rows:
        raise ChunkingError("No chunks produced from image-set index")

    chunk_df = pl.DataFrame(chunk_rows)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    written = []
    joined = df.join(chunk_df, on="image_set_id", how="inner")
    for chunk_id in joined["chunk_id"].unique().sort():
        chunk = joined.filter(pl.col("chunk_id") == chunk_id).sort(
            ["well", "site", "channel", "image_path"]
        )
        path = output_path / f"{chunk_id}.csv"
        chunk.write_csv(path)
        written.append(str(path))
    return written


def _cmd_index(args):
    build_image_set_index(
        plate_id=args.plate_id,
        plate_dir=args.plate_dir,
        output_csv=args.output_csv,
        expected_channels=args.expected_channels,
        ext=args.ext,
    )


def _cmd_chunk(args):
    split_image_set_index(
        index_csv=args.index_csv,
        output_dir=args.output_dir,
        chunk_size=args.chunk_size,
        max_chunks=args.max_chunks,
    )


def build_parser():
    parser = argparse.ArgumentParser(
        prog="python -m cptools2.nextflow_chunking",
        description="Index and chunk staged ImageXpress plates for Nextflow",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    p_index = subparsers.add_parser("index")
    p_index.add_argument("--plate-id", required=True)
    p_index.add_argument("--plate-dir", required=True)
    p_index.add_argument("--output-csv", required=True)
    p_index.add_argument("--expected-channels", default=None)
    p_index.add_argument("--ext", default=".tif")
    p_index.set_defaults(func=_cmd_index)

    p_chunk = subparsers.add_parser("chunk")
    p_chunk.add_argument("--index-csv", required=True)
    p_chunk.add_argument("--output-dir", required=True)
    p_chunk.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    p_chunk.add_argument("--max-chunks", type=int, default=None)
    p_chunk.set_defaults(func=_cmd_chunk)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
