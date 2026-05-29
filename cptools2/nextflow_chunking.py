"""Helpers used by the Nextflow pipeline to index and chunk staged plates.

These functions do domain work for Nextflow processes. They do not submit jobs.
Nextflow owns orchestration; this module owns ImageXpress parsing and manifest
creation.
"""

import argparse
import csv
import json
import os
import shutil
import struct
from pathlib import Path

# Eddie login/local metadata tasks can hit process/thread limits if Polars/Rayon
# tries to create a full worker pool for small CSV writes.
os.environ.setdefault("POLARS_MAX_THREADS", "1")
os.environ.setdefault("RAYON_NUM_THREADS", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")

DEFAULT_CHUNK_SIZE = 96
CHANNEL_NAME_TO_INDEX = {
    "DNA": 1,
    "RNA": 2,
    "ER": 3,
    "AGP": 4,
    "MITO": 5,
}


class ChunkingError(Exception):
    """Raised when a staged plate cannot be indexed or chunked safely."""


def _pl():
    import polars as pl

    return pl


def _parserix_parse():
    from parserix import parse as parserix_parse

    return parserix_parse


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
    try:
        channel = _parserix_parse().img_channel(filename)
    except Exception:
        channel = Path(filename).stem.rsplit("_", 1)[-1]
        channel = CHANNEL_NAME_TO_INDEX.get(str(channel).upper(), channel)
    try:
        return int(channel)
    except (TypeError, ValueError):
        return channel


def _deepprofiler_image_value(image_path, plate):
    path = Path(image_path)
    plate = str(plate)
    try:
        parts = path.parts
        if plate in parts:
            plate_index = parts.index(plate)
            return Path(*parts[plate_index:]).as_posix()
    except (TypeError, ValueError):
        pass
    return Path(plate, path.name).as_posix()


def _read_json(path):
    return json.loads(Path(path).read_text())


def _normalise_image_format(value):
    return str(value or "tif").lower().lstrip(".")


def _resolve_deepprofiler_config_values(config):
    dataset = config.get("dataset", {})
    metadata = dataset.get("metadata", {})
    images = dataset.get("images", {})
    channels = images.get("channels") or []
    if not channels:
        raise ChunkingError("DeepProfiler config missing dataset.images.channels")
    config_label_field = metadata.get("label_field")
    if not config_label_field:
        raise ChunkingError("DeepProfiler config missing dataset.metadata.label_field")
    resolved_label_value = metadata.get("control_value")
    if resolved_label_value is None:
        raise ChunkingError(
            "DeepProfiler config missing dataset.metadata.control_value"
        )
    resolved_image_format = _normalise_image_format(images.get("file_format") or "tif")
    return channels, config_label_field, resolved_label_value, resolved_image_format


def _link_or_copy(src, dst):
    try:
        if dst.exists() or dst.is_symlink():
            dst.unlink()
        os.symlink(src, dst)
    except (OSError, NotImplementedError, AttributeError):
        shutil.copy2(src, dst)


def _collect_location_files(locations_dir):
    locations_dir = Path(locations_dir)
    nested = locations_dir / "locations"
    search_dir = nested if nested.is_dir() else locations_dir
    if not search_dir.is_dir():
        raise ChunkingError(
            "Cellpose locations directory does not exist: " + str(locations_dir)
        )
    return sorted(search_dir.glob("*.csv"))


def _is_zero_location_file_for_chunk(location_file, chunk_manifest):
    return location_file.name == f"{Path(chunk_manifest).stem}_locations.csv"


def _write_empty_nuclei_csv(output_file):
    output_file.parent.mkdir(parents=True, exist_ok=True)
    with output_file.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "Nuclei_Location_Center_X",
                "Nuclei_Location_Center_Y",
            ],
        )
        writer.writeheader()


def _read_csv_dicts(path):
    with Path(path).open(newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        return fieldnames, list(reader)


def _write_csv_dicts(path, rows, fieldnames):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _read_tiff_dimensions(path):  # noqa: C901
    """Read TIFF width/height from the image header without extra dependencies."""

    with Path(path).open("rb") as handle:
        header = handle.read(8)
        if len(header) != 8:
            raise ChunkingError(f"Cannot read TIFF header: {path}")
        if header[:2] == b"II":
            endian = "<"
        elif header[:2] == b"MM":
            endian = ">"
        else:
            raise ChunkingError(f"Unsupported TIFF byte order: {path}")
        if struct.unpack(endian + "H", header[2:4])[0] != 42:
            raise ChunkingError(f"Unsupported TIFF header: {path}")
        ifd_offset = struct.unpack(endian + "I", header[4:8])[0]
        handle.seek(ifd_offset)
        entry_count = struct.unpack(endian + "H", handle.read(2))[0]
        width = None
        height = None
        for _ in range(entry_count):
            entry = handle.read(12)
            if len(entry) != 12:
                raise ChunkingError(f"Truncated TIFF IFD entry: {path}")
            tag, value_type, count, value = struct.unpack(endian + "HHII", entry)
            if value_type not in {3, 4} or count != 1:
                continue
            numeric_value = value
            if value_type == 3:
                numeric_value = value & 0xFFFF if endian == "<" else value >> 16
            if tag == 256:
                width = numeric_value
            elif tag == 257:
                height = numeric_value
        if width is None or height is None:
            raise ChunkingError(f"TIFF dimensions not found: {path}")
        return int(width), int(height)


def _write_deepprofiler_config(config, config_output, image_dimensions):
    copied_config = json.loads(json.dumps(config))
    if image_dimensions is not None:
        images = copied_config.setdefault("dataset", {}).setdefault("images", {})
        images["width"], images["height"] = image_dimensions
    Path(config_output).write_text(json.dumps(copied_config, indent=2) + "\n")


def _deepprofiler_location_names(well, site):
    raw_site = str(site)
    names = [
        f"{well}-f{raw_site}-Nuclei.csv",
        f"{well}-{raw_site}-Nuclei.csv",
    ]
    try:
        padded_site = f"{int(raw_site):02d}"
    except ValueError:
        padded_site = raw_site
    for padded_name in [
        f"{well}-f{padded_site}-Nuclei.csv",
        f"{well}-{padded_site}-Nuclei.csv",
    ]:
        if padded_name not in names:
            names.append(padded_name)
    return names


def build_deepprofiler_input_package(  # noqa: C901
    chunk_manifest,
    locations_dir,
    output_root,
    config_path,
):
    chunk_manifest = Path(chunk_manifest)
    locations_dir = Path(locations_dir)
    output_root = Path(output_root)
    config_path = Path(config_path)

    config = _read_json(config_path)
    config_channels, label_field, resolved_label_value, resolved_image_format = (
        _resolve_deepprofiler_config_values(config)
    )
    config_channel_map = {
        str(index + 1): str(channel_name)
        for index, channel_name in enumerate(config_channels)
    }

    manifest_columns, manifest_rows = _read_csv_dicts(chunk_manifest)
    required = {"plate", "well", "site", "channel", "image_path", "image_set_id"}
    missing = sorted(required.difference(manifest_columns))
    if missing:
        raise ChunkingError(
            "DeepProfiler chunk manifest missing required columns: " + str(missing)
        )

    images_root = output_root / "images"
    metadata_dir = output_root / "metadata"
    locations_output = output_root / "locations"
    config_output = output_root / "config"
    images_root.mkdir(parents=True, exist_ok=True)
    metadata_dir.mkdir(parents=True, exist_ok=True)
    locations_output.mkdir(parents=True, exist_ok=True)
    config_output.mkdir(parents=True, exist_ok=True)

    metadata_rows = []
    expected_sites = []
    groups = {}
    image_dimensions = None
    for row in manifest_rows:
        row["channel"] = str(row["channel"])
        groups.setdefault(row["image_set_id"], []).append(row)
    for image_set_id in sorted(groups):
        group = groups[image_set_id]
        first = group[0]
        plate = str(first["plate"])
        well = str(first["well"])
        site = str(first["site"])
        expected_sites.append((plate, well, site))
        image_plate_dir = images_root / plate
        image_plate_dir.mkdir(parents=True, exist_ok=True)
        row = {
            "Metadata_Plate": plate,
            "Metadata_Well": well,
            "Metadata_Site": site,
            label_field: resolved_label_value,
            "Metadata_ImageSet": image_set_id,
        }
        observed = {str(value["channel"]) for value in group}
        for manifest_channel, column_name in config_channel_map.items():
            if manifest_channel not in observed:
                raise ChunkingError(
                    "Missing channel "
                    f"{manifest_channel} for DeepProfiler image set {image_set_id}"
                )
            channel_row = next(
                value for value in group if value["channel"] == manifest_channel
            )
            source_image = Path(channel_row["image_path"])
            if _normalise_image_format(source_image.suffix) != resolved_image_format:
                raise ChunkingError(
                    "DeepProfiler image format mismatch for "
                    f"{image_set_id}: expected {resolved_image_format}"
                )
            dimensions = _read_tiff_dimensions(source_image)
            if image_dimensions is None:
                image_dimensions = dimensions
            elif dimensions != image_dimensions:
                raise ChunkingError(
                    "DeepProfiler images have mixed dimensions: "
                    f"{image_dimensions} and {dimensions}"
                )
            dest_image = image_plate_dir / source_image.name
            _link_or_copy(source_image, dest_image)
            row[column_name] = _deepprofiler_image_value(
                dest_image, plate
            )
        metadata_rows.append(row)

    metadata_columns = [
        "Metadata_Plate",
        "Metadata_Well",
        "Metadata_Site",
        label_field,
        "Metadata_ImageSet",
        *config_channels,
    ]
    _write_csv_dicts(metadata_dir / "index.csv", metadata_rows, metadata_columns)
    _write_deepprofiler_config(config, config_output / "config.json", image_dimensions)

    location_files = _collect_location_files(locations_dir)
    if not location_files:
        raise ChunkingError(
            "No Cellpose location CSVs found under: " + str(locations_dir)
        )
    location_rows = {}
    chunk_locations_file_seen = False
    required_loc = {"plate", "well", "site", "x", "y"}
    for location_file in location_files:
        if _is_zero_location_file_for_chunk(location_file, chunk_manifest):
            chunk_locations_file_seen = True
        loc_columns, loc_rows = _read_csv_dicts(location_file)
        missing_loc = sorted(required_loc.difference(loc_columns))
        if missing_loc:
            raise ChunkingError(
                "Cellpose location CSV missing required columns: " + str(missing_loc)
            )
        if not loc_rows:
            continue
        for row in loc_rows:
            key = (str(row["plate"]), str(row["well"]), str(row["site"]))
            location_rows.setdefault(key, []).append(row)
    if not chunk_locations_file_seen:
        raise ChunkingError(
            "No Cellpose location CSV found for chunk: " + chunk_manifest.stem
        )

    for plate, well, site in expected_sites:
        per_site = location_rows.get((plate, well, site), [])
        for location_name in _deepprofiler_location_names(well, site):
            output_file = locations_output / plate / location_name
            output_file.parent.mkdir(parents=True, exist_ok=True)
            if per_site:
                nuclei_rows = [
                    {
                        "Nuclei_Location_Center_X": float(row["x"]),
                        "Nuclei_Location_Center_Y": float(row["y"]),
                    }
                    for row in per_site
                ]
                _write_csv_dicts(
                    output_file,
                    nuclei_rows,
                    [
                        "Nuclei_Location_Center_X",
                        "Nuclei_Location_Center_Y",
                    ],
                )
            elif chunk_locations_file_seen:
                _write_empty_nuclei_csv(output_file)
            else:
                raise ChunkingError(
                    "Missing Cellpose locations for DeepProfiler site: "
                    f"{plate} {well} site {site}"
                )

    return metadata_dir / "index.csv"


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
    pl = _pl()
    from cptools2 import filelist

    expected_channels = _normalise_expected_channels(expected_channels)
    _is_new_ix, image_paths = filelist.detect_plate_layout(
        plate_dir,
        ext=ext,
        clean=True,
        truncate=False,
    )

    rows = []
    parserix_parse = _parserix_parse()
    for image_path in sorted(image_paths):
        filename = parserix_parse.img_filename(image_path)
        well = parserix_parse.img_well(filename)
        site = parserix_parse.img_site(filename)
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
    pl = _pl()
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

    pl = _pl()
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


def _cmd_deepprofiler_package(args):
    build_deepprofiler_input_package(
        chunk_manifest=args.chunk_manifest,
        locations_dir=args.locations_dir,
        output_root=args.output_root,
        config_path=args.config_path,
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

    p_dp = subparsers.add_parser("deepprofiler-package")
    p_dp.add_argument("--chunk-manifest", required=True)
    p_dp.add_argument("--locations-dir", required=True)
    p_dp.add_argument("--output-root", required=True)
    p_dp.add_argument("--config-path", required=True)
    p_dp.set_defaults(func=_cmd_deepprofiler_package)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
