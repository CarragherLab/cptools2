#!/usr/bin/env python3
"""Create a tiny parserix-compatible plate for Phase 2.8 Loop 440 smoke tests."""

import argparse
import os
import struct
from pathlib import Path


DEFAULT_PLATE = "tiny-plate-001"
DEFAULT_WELL = "B02"
DEFAULT_SITE = 1
DEFAULT_CHANNELS = ("DNA", "RNA", "ER", "AGP", "Mito")
DEFAULT_SIZE = 64


def _write_uint16_tiff(path, width=DEFAULT_SIZE, height=DEFAULT_SIZE, value=1000):
    """Write a minimal uncompressed little-endian grayscale TIFF."""
    pixels = struct.pack("<" + "H" * (width * height), *([value] * width * height))
    entries = [
        (256, 4, 1, width),  # ImageWidth
        (257, 4, 1, height),  # ImageLength
        (258, 3, 1, 16),  # BitsPerSample
        (259, 3, 1, 1),  # Compression: none
        (262, 3, 1, 1),  # PhotometricInterpretation: BlackIsZero
        (273, 4, 1, 8),  # StripOffsets: immediately after header
        (277, 3, 1, 1),  # SamplesPerPixel
        (278, 4, 1, height),  # RowsPerStrip
        (279, 4, 1, len(pixels)),  # StripByteCounts
    ]
    ifd_offset = 8 + len(pixels)
    with open(path, "wb") as handle:
        handle.write(b"II")
        handle.write(struct.pack("<H", 42))
        handle.write(struct.pack("<I", ifd_offset))
        handle.write(pixels)
        handle.write(struct.pack("<H", len(entries)))
        for tag, field_type, count, value_or_offset in entries:
            handle.write(struct.pack("<HHII", tag, field_type, count, value_or_offset))
        handle.write(struct.pack("<I", 0))


def create_plate(root, plate=DEFAULT_PLATE, well=DEFAULT_WELL, site=DEFAULT_SITE):
    plate_dir = Path(root) / plate / "2015-07-31" / "4016"
    plate_dir.mkdir(parents=True, exist_ok=True)
    for index, channel in enumerate(DEFAULT_CHANNELS, start=1):
        filename = f"val screen_{well}_s{site}_{channel}.tif"
        _write_uint16_tiff(
            plate_dir / filename,
            value=1000 + index,
        )
    return plate_dir


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Create a tiny ImageXpress-style plate for Loop 440 smoke tests."
    )
    parser.add_argument(
        "--root",
        default=os.environ.get(
            "CPTOOLS2_LOOP440_STAGING",
            "/exports/eddie/scratch/${USER}/cptools2-ai-update/staging/tiny-plate-set",
        ),
        help="Directory that will contain the plate directory.",
    )
    parser.add_argument("--plate", default=DEFAULT_PLATE)
    parser.add_argument("--well", default=DEFAULT_WELL)
    parser.add_argument("--site", type=int, default=DEFAULT_SITE)
    args = parser.parse_args(argv)

    root = os.path.expandvars(args.root)
    plate_dir = create_plate(root, plate=args.plate, well=args.well, site=args.site)
    print(plate_dir)


if __name__ == "__main__":
    main()
