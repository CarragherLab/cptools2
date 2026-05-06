from pathlib import Path

import polars as pl

from cptools2 import nextflow_chunking
from scripts import create_loop440_tiny_plate


ROOT = Path(__file__).resolve().parents[1]
BOOTSTRAP = ROOT / "config" / "eddie_env.sh"
EDDIE_CONFIG = ROOT / "nextflow" / "conf" / "eddie.config"
CELLPROFILER_SMOKE = ROOT / "scripts" / "eddie_smoke_cellprofiler.sh"
CELLPOSE_SMOKE = ROOT / "scripts" / "eddie_smoke_cellpose.sh"
DEEPPROFILER_SMOKE = ROOT / "scripts" / "eddie_smoke_deepprofiler.sh"
LOOP440_CONFIG = ROOT / "config" / "loop440-eddie-smoke.yaml"
PERMANENT_ROOT = "/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2"
SCRATCH_ROOT = "/exports/eddie/scratch/${USER}/cptools2-ai-update"


def test_eddie_bootstrap_loads_pinned_modules():
    text = BOOTSTRAP.read_text()

    assert text.startswith("#!/usr/bin/env bash")
    assert "source /etc/profile.d/modules.sh" in text
    assert "module purge" in text
    assert "module load roslin/nextflow/25.10.2" in text
    assert "module load singularity/4.3.4" in text
    assert "module load miniforge/25.3.1-0" in text


def test_eddie_bootstrap_separates_permanent_and_scratch_paths():
    text = BOOTSTRAP.read_text()

    assert f'export CPTOOLS2_PROJECT_ROOT="{PERMANENT_ROOT}"' in text
    assert 'export CPTOOLS2_PERMANENT_ROOT="${CPTOOLS2_PROJECT_ROOT}"' in text
    assert f'export CPTOOLS2_SCRATCH_ROOT="{SCRATCH_ROOT}"' in text
    assert 'export CPTOOLS2_CONFIG_ROOT="${CPTOOLS2_PERMANENT_ROOT}/config"' in text
    assert 'export CPTOOLS2_CONTAINER_DIR="${CPTOOLS2_PERMANENT_ROOT}/containers"' in text
    assert 'export CPTOOLS2_WORK_ROOT="${CPTOOLS2_SCRATCH_ROOT}/work"' in text
    assert 'export CPTOOLS2_PARAMS_ROOT="${CPTOOLS2_SCRATCH_ROOT}/params"' in text
    assert 'export CPTOOLS2_CACHE_ROOT="${CPTOOLS2_WORK_ROOT}/cache"' in text
    assert 'export CPTOOLS2_LOG_ROOT="${CPTOOLS2_SCRATCH_ROOT}/logs"' in text
    assert 'export CPTOOLS2_TRACE_ROOT="${CPTOOLS2_SCRATCH_ROOT}/traces"' in text
    assert 'export CPTOOLS2_TEMP_ROOT="${CPTOOLS2_WORK_ROOT}/tmp"' in text
    assert 'export NXF_HOME="${CPTOOLS2_CACHE_ROOT}/nextflow"' in text
    assert 'export NXF_TEMP="${CPTOOLS2_TEMP_ROOT}/nextflow"' in text
    assert 'export SINGULARITY_CACHEDIR="${CPTOOLS2_CACHE_ROOT}/singularity"' in text
    assert 'export SINGULARITY_TMPDIR="${CPTOOLS2_TEMP_ROOT}/singularity"' in text
    assert 'export APPTAINER_CACHEDIR="${CPTOOLS2_CACHE_ROOT}/apptainer"' in text
    assert 'export APPTAINER_TMPDIR="${CPTOOLS2_TEMP_ROOT}/apptainer"' in text
    assert 'export TMPDIR="${CPTOOLS2_TEMP_ROOT}"' in text
    assert 'export TEMP="${CPTOOLS2_TEMP_ROOT}"' in text
    assert 'export TMP="${CPTOOLS2_TEMP_ROOT}"' in text
    assert 'export XDG_CACHE_HOME="${CPTOOLS2_CACHE_ROOT}/xdg"' in text
    assert 'export XDG_RUNTIME_DIR="${CPTOOLS2_TEMP_ROOT}/xdg-runtime"' in text
    assert "mkdir -p" in text


def test_eddie_bootstrap_documents_dry_run_usage():
    text = BOOTSTRAP.read_text()

    assert "cptools2 generate" in text
    assert "--dry-run" in text
    assert PERMANENT_ROOT in text


def test_nextflow_eddie_config_uses_permanent_containers_and_scratch_cache():
    text = EDDIE_CONFIG.read_text()

    assert "def cptools2ProjectRoot = System.getenv('CPTOOLS2_PROJECT_ROOT')" in text
    assert PERMANENT_ROOT in text
    assert "def cptools2ContainerDir = System.getenv('CPTOOLS2_CONTAINER_DIR')" in text
    assert 'cellprofiler_4.2.8.sif' in text
    assert 'deepprofiler_1.0.sif' in text
    assert 'cellpose_sam_1.0.sif' in text
    assert 'module load roslin/nextflow/25.10.2' in text
    assert 'module load singularity/4.3.4' in text
    assert 'module load miniforge/25.3.1-0' in text
    assert 'CPTOOLS2_SCRATCH_ROOT' in text
    assert 'cptools2-ai-update' in text
    assert 'SINGULARITY_CACHEDIR' in text


def test_cellprofiler_smoke_script_is_cpu_sge_and_scratch_only():
    text = CELLPROFILER_SMOKE.read_text()

    assert text.startswith("#!/bin/sh")
    assert "#$ -cwd" in text
    assert "#$ -l h_rt=00:20:00" in text
    assert "#$ -l h_rss=8G" in text
    assert "#$ -q staging" not in text
    assert "#$ -q gpu" not in text
    assert "module load singularity/4.3.4" in text
    assert "cellprofiler_4.2.8.sif" in text
    assert "cellprofiler --version" in text
    assert "tee " not in text
    assert 'exit "${status}"' in text
    assert "cptools2-ai-update" in text
    assert "SINGULARITY_CACHEDIR" in text
    assert "SINGULARITY_TMPDIR" in text


def test_gpu_smoke_scripts_request_gpu_and_use_nv():
    scripts = [
        (CELLPOSE_SMOKE, "cellpose_sam_1.0.sif", "torch.cuda.is_available()"),
        (DEEPPROFILER_SMOKE, "deepprofiler_1.0.sif", "tf.config.list_physical_devices('GPU')"),
    ]

    for script, container_name, gpu_probe in scripts:
        text = script.read_text()
        assert text.startswith("#!/bin/sh")
        assert "#$ -cwd" in text
        assert "#$ -q gpu" in text
        assert "#$ -l gpu=1" in text
        assert "#$ -l h_rss=16G" in text
        assert "module load singularity/4.3.4" in text
        assert "singularity exec --nv" in text
        assert container_name in text
        assert gpu_probe in text
        assert "tee " not in text
        assert 'exit "${status}"' in text
        assert "cptools2-ai-update" in text
        assert "SINGULARITY_CACHEDIR" in text
        assert "SINGULARITY_TMPDIR" in text


def test_loop440_smoke_config_uses_tiny_scratch_input_and_permanent_assets():
    text = LOOP440_CONFIG.read_text()

    assert "/exports/eddie/scratch/mharvey2/cptools2-ai-update/staging/tiny-plate-set" in text
    assert "/exports/eddie/scratch/mharvey2/cptools2-ai-update/results/loop440" in text
    assert "stage_data: false" in text
    assert "max_chunks: 1" in text
    assert "tiny-plate-001: 1" in text
    assert f"container_path: {PERMANENT_ROOT}/containers" in text
    assert f"{PERMANENT_ROOT}/cptools2/templates/deepprofiler_config.json" in text
    assert "/exports/eddie/scratch/mharvey2/cptools2-loop230" not in text


def test_loop440_tiny_plate_builder_creates_indexable_real_tiffs(tmp_path):
    plate_root = tmp_path / "tiny-plate-set"
    create_loop440_tiny_plate.create_plate(plate_root)

    tiffs = sorted(plate_root.rglob("*.tif"))
    assert len(tiffs) == 5
    assert all(path.stat().st_size > 0 for path in tiffs)

    index_csv = tmp_path / "image_sets.csv"
    nextflow_chunking.build_image_set_index(
        "tiny-plate-001",
        plate_root / "tiny-plate-001",
        index_csv,
        expected_channels="1,2,3,4,5",
    )
    df = pl.read_csv(index_csv)
    assert df["image_set_id"].n_unique() == 1
    assert sorted(df["channel"].to_list()) == [1, 2, 3, 4, 5]

    chunks = nextflow_chunking.split_image_set_index(
        index_csv,
        tmp_path / "chunks",
        chunk_size=96,
        max_chunks=1,
    )
    assert len(chunks) == 1
