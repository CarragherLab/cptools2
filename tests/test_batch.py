import os
import pytest
from cptools2.batch import compute_plate_sizes, create_batches, get_scratch_quota, ScratchQuota


def test_compute_plate_sizes(tmp_path):
    """compute_plate_sizes returns correct sizes for plate directories."""
    plate1 = tmp_path / "plate1"
    plate1.mkdir()
    (plate1 / "img1.tif").write_bytes(b"x" * 1000)
    (plate1 / "img2.tif").write_bytes(b"x" * 2000)

    plate2 = tmp_path / "plate2"
    plate2.mkdir()
    (plate2 / "img1.tif").write_bytes(b"x" * 500)

    sizes = compute_plate_sizes(str(tmp_path))
    assert sizes["plate1"] == 3000
    assert sizes["plate2"] == 500


def test_compute_plate_sizes_empty_dir(tmp_path):
    """compute_plate_sizes returns empty dict for empty directory."""
    sizes = compute_plate_sizes(str(tmp_path))
    assert sizes == {}


def test_compute_plate_sizes_nonexistent_dir():
    """compute_plate_sizes returns empty dict for nonexistent directory."""
    sizes = compute_plate_sizes("/nonexistent/path")
    assert sizes == {}


def test_create_batches_single_plate_fits():
    """Single plate that fits in scratch produces 1 batch."""
    plate_sizes = {"plate1": 100 * 1024**3}  # 100GB
    available = 500 * 1024**3  # 500GB scratch
    batches = create_batches(plate_sizes, available)
    assert len(batches) == 1
    assert batches[0]["plates"] == ["plate1"]
    assert batches[0]["batch_id"] == 1


def test_create_batches_multiple_batches():
    """Multiple plates that don't fit in one batch produce multiple batches."""
    plate_sizes = {
        "plate_large": 200 * 1024**3,  # 200GB
        "plate_medium": 150 * 1024**3,  # 150GB
        "plate_small": 50 * 1024**3,   # 50GB
    }
    available = 400 * 1024**3  # 400GB scratch (75% = 300GB usable, with 30% overhead per plate)
    batches = create_batches(plate_sizes, available)
    assert len(batches) >= 2
    # All plates should be assigned to some batch
    all_plates = []
    for b in batches:
        all_plates.extend(b["plates"])
    assert set(all_plates) == {"plate_large", "plate_medium", "plate_small"}


def test_create_batches_empty():
    """Empty plate_sizes returns empty batch list."""
    batches = create_batches({}, 500 * 1024**3)
    assert batches == []


def test_scratch_quota_default():
    """get_scratch_quota returns 2TB default when all detection fails."""
    quota = get_scratch_quota()
    assert isinstance(quota, ScratchQuota)
    assert quota.total > 0
    assert quota.available > 0


def test_scratch_quota_config_override():
    """get_scratch_quota uses YAML config value when provided."""
    quota = get_scratch_quota(config_quota_gb=1000)
    assert quota.total == 1000 * 1024**3
    assert quota.available == 1000 * 1024**3
    assert quota.used_pct == 0
