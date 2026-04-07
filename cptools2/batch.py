"""
Batch-aware plate orchestration for scratch-constrained HPC environments.

Computes plate batches that fit within available scratch space,
using 75% utilization target and 30% overhead buffer.
"""

import os
import subprocess

from cptools2.colours import pretty_print, yellow, red


def compute_plate_sizes(input_dir):
    """
    Scan plate directories and compute their sizes in bytes.

    Parameters
    ----------
    input_dir : str
        Path to directory containing plate subdirectories.

    Returns
    -------
    dict
        Mapping of plate_name -> size_in_bytes.
    """
    plate_sizes = {}
    if not os.path.isdir(input_dir):
        pretty_print(f"Warning: input directory not found: {yellow(input_dir)}")
        return plate_sizes
    for item in os.listdir(input_dir):
        item_path = os.path.join(input_dir, item)
        if os.path.isdir(item_path):
            total_size = 0
            for dirpath, dirnames, filenames in os.walk(item_path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    try:
                        total_size += os.path.getsize(fp)
                    except OSError:
                        pass
            plate_sizes[item] = total_size
    return plate_sizes


def create_batches(plate_sizes, available_scratch):
    """
    Group plates into batches that fit within available scratch space.

    Uses 75% utilization target with 30% overhead buffer per plate.

    Parameters
    ----------
    plate_sizes : dict
        Mapping of plate_name -> size_in_bytes.
    available_scratch : int
        Available scratch space in bytes.

    Returns
    -------
    list of dict
        Each dict has: batch_id, plates (list), total_size_gb, plate_count.
    """
    if not plate_sizes:
        return []

    utilisation_fraction = 0.75
    overhead_factor = 1.3
    max_batch_size = available_scratch * utilisation_fraction

    sorted_plates = sorted(
        plate_sizes.items(),
        key=lambda x: x[1],
        reverse=True,
    )

    batches = []
    current_batch = []
    current_batch_size = 0
    batch_number = 1

    for plate_name, plate_size in sorted_plates:
        effective_plate_size = plate_size * overhead_factor
        if current_batch_size + effective_plate_size <= max_batch_size:
            current_batch.append(plate_name)
            current_batch_size += effective_plate_size
        else:
            if current_batch:
                batches.append({
                    "batch_id": batch_number,
                    "plates": current_batch.copy(),
                    "total_size_gb": current_batch_size / (1024**3),
                    "plate_count": len(current_batch),
                })
                batch_number += 1
            current_batch = [plate_name]
            current_batch_size = effective_plate_size

    if current_batch:
        batches.append({
            "batch_id": batch_number,
            "plates": current_batch.copy(),
            "total_size_gb": current_batch_size / (1024**3),
            "plate_count": len(current_batch),
        })

    return batches


class ScratchQuota:
    """Container for scratch quota information."""

    def __init__(self, total, used, available):
        self.total = total
        self.used = used
        self.available = available
        self.used_pct = (used / total * 100) if total > 0 else 0


def get_scratch_quota(config_quota_gb=None):
    """
    Detect available scratch space.

    Fallback chain: lfs quota -> df -> YAML config -> 2TB default.

    Parameters
    ----------
    config_quota_gb : float, optional
        User-specified quota in GB from YAML config.

    Returns
    -------
    ScratchQuota
        Quota information with total, used, available in bytes.
    """
    scratch_path = f"/exports/eddie/scratch/{os.environ.get('USER', 'unknown')}"

    # Try lfs quota (Lustre filesystems)
    try:
        result = subprocess.run(
            ["lfs", "quota", "-u", os.environ.get("USER", ""), scratch_path],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            # Parse lfs quota output
            for line in result.stdout.strip().split("\n"):
                parts = line.split()
                if len(parts) >= 4 and parts[0].startswith("/"):
                    used_kb = int(parts[1]) * 1024
                    quota_kb = int(parts[2]) * 1024
                    return ScratchQuota(quota_kb, used_kb, quota_kb - used_kb)
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
        pass

    # Try df
    try:
        result = subprocess.run(
            ["df", "-B1", scratch_path],
            capture_output=True, text=True, timeout=10,
        )
        if result.returncode == 0:
            lines = result.stdout.strip().split("\n")
            if len(lines) >= 2:
                parts = lines[1].split()
                if len(parts) >= 4:
                    total = int(parts[1])
                    used = int(parts[2])
                    available = int(parts[3])
                    return ScratchQuota(total, used, available)
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
        pass

    # YAML config fallback
    if config_quota_gb is not None:
        total = int(config_quota_gb * 1024**3)
        return ScratchQuota(total, 0, total)

    # Default: 2TB
    total = 2 * 1024**4
    return ScratchQuota(total, 0, total)
