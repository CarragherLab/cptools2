// Cellpose segmentation for AI feature extraction paths.
//
// Input:  tuple(plate_id, corrected_dir, chunk_manifest_csv)
// Output: tuple(plate_id, corrected_dir, chunk_manifest_csv, masks_dir)

process CELLPOSE_SEGMENT {
    tag "${plate_id}:${chunk_manifest.simpleName}"
    label 'segmentation'
    label 'gpu'
    publishDir "${params.output_dir}/${plate_id}/cellpose/${chunk_manifest.simpleName}", mode: 'copy', pattern: 'cellpose_masks/**'

    container params.containers.cellpose_sam

    input:
    tuple val(plate_id), path(corrected_dir), path(chunk_manifest)

    output:
    tuple val(plate_id), path(corrected_dir), path(chunk_manifest), path("cellpose_masks"), emit: masks

    script:
    """
    mkdir -p cellpose_masks
    mkdir -p cellpose_input

    echo '=== cptools2 GPU diagnostics: CELLPOSE_SEGMENT start ==='
    hostname || true
    echo "CUDA_VISIBLE_DEVICES=\${CUDA_VISIBLE_DEVICES:-unset}"
    nvidia-smi -L || true
    nvidia-smi --query-gpu=index,name,memory.total,memory.used --format=csv || true
    echo '=== cptools2 GPU diagnostics: CELLPOSE_SEGMENT end ==='

    python - <<'PY'
import csv
import os
from pathlib import Path

channel = str("${params.cellpose_channel}")
selected = []
with open("${chunk_manifest}", newline="") as handle:
    for row in csv.DictReader(handle):
        if str(row["channel"]) != channel:
            continue
        src = Path(row["image_path"])
        ext = src.suffix or ".tif"
        dst = Path("cellpose_input") / f"{row['image_set_id']}{ext}"
        if not dst.exists():
            os.symlink(src, dst)
        selected.append(row)

if not selected:
    raise SystemExit(
        "No images for Cellpose channel "
        + channel
        + " in chunk manifest ${chunk_manifest}"
    )

with open("cellpose_masks/cellpose_input_manifest.csv", "w", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=selected[0].keys())
    writer.writeheader()
    writer.writerows(selected)
PY

    python -m cellpose \
        --use_gpu \
        --dir cellpose_input \
        --savedir cellpose_masks \
        --pretrained_model ${params.cellpose_model} \
        --diameter ${params.cellpose_diameter} \
        --batch_size ${params.cellpose_batch_size} \
        --save_tif \
        --no_npy \
        --verbose

    python - <<'PY'
import csv
from pathlib import Path

import numpy as np
import tifffile
from scipy import ndimage

manifest = {}
with open("cellpose_masks/cellpose_input_manifest.csv", newline="") as handle:
    for row in csv.DictReader(handle):
        manifest[row["image_set_id"]] = row

rows = []
for mask_path in sorted(Path("cellpose_masks").glob("*_cp_masks.tif")):
    image_set_id = mask_path.name.replace("_cp_masks.tif", "")
    meta = manifest.get(image_set_id, {})
    mask = tifffile.imread(mask_path)
    labels = np.unique(mask)
    labels = labels[labels > 0]
    if labels.size == 0:
        continue
    centers = ndimage.center_of_mass(mask > 0, labels=mask, index=labels)
    for label, center in zip(labels, centers):
        rows.append({
            "plate": meta.get("plate", "${plate_id}"),
            "well": meta.get("well", ""),
            "site": meta.get("site", ""),
            "image_path": meta.get("image_path", ""),
            "image_set_id": image_set_id,
            "object_id": int(label),
            "x": float(center[1]),
            "y": float(center[0]),
            "mask_path": str(mask_path),
        })

Path("cellpose_masks/locations").mkdir(exist_ok=True)
with open("cellpose_masks/locations/${chunk_manifest.simpleName}_locations.csv", "w", newline="") as handle:
    fieldnames = [
        "plate", "well", "site", "image_path", "image_set_id",
        "object_id", "x", "y", "mask_path",
    ]
    writer = csv.DictWriter(handle, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
PY

    echo '=== cptools2 GPU diagnostics: CELLPOSE_SEGMENT post ==='
    nvidia-smi --query-gpu=index,name,memory.total,memory.used --format=csv || true
    echo '=== cptools2 GPU diagnostics: CELLPOSE_SEGMENT post end ==='
    """
}
