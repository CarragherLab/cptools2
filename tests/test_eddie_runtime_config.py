import json
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
EDDIE_INSTALL = ROOT / "scripts" / "install_eddie.sh"
EDDIE_PATH_CONFIGURATOR = ROOT / "scripts" / "configure_eddie_paths.sh"
CONTAINER_MANIFEST_TEMPLATE = ROOT / "cptools2" / "container_manifest_template.json"
CONTAINER_README = ROOT / "cptools2" / "dockerfiles" / "README.md"
LOOP440_CONFIG = ROOT / "config" / "loop440-eddie-smoke.yaml"
PROJECT_ROOT_VAR = "${CPTOOLS2_PROJECT_ROOT}"
SCRATCH_ROOT_VAR = "${CPTOOLS2_SCRATCH_ROOT}"


def test_eddie_bootstrap_loads_pinned_modules():
    text = BOOTSTRAP.read_text()

    assert text.startswith("#!/usr/bin/env bash")
    assert "source /etc/profile.d/modules.sh" in text
    assert "module purge" in text
    assert "module load uge/2024.1.0" in text
    assert "module load roslin/nextflow/25.10.2" in text
    assert "module load singularity/4.3.4" in text
    assert "module load miniforge/25.3.1-0" in text


def test_eddie_bootstrap_separates_permanent_and_scratch_paths():
    text = BOOTSTRAP.read_text()

    assert "CPTOOLS2_PATHS_FILE" in text
    assert "-local/eddie_paths.env" in text
    assert ": \"${CPTOOLS2_PROJECT_ROOT:?" in text
    assert ": \"${CPTOOLS2_SCRATCH_ROOT:?" in text
    assert 'export CPTOOLS2_PERMANENT_ROOT="${CPTOOLS2_PROJECT_ROOT}"' in text
    assert 'export CPTOOLS2_CONFIG_ROOT="${CPTOOLS2_PERMANENT_ROOT}/config"' in text
    assert 'export CPTOOLS2_CONTAINER_DIR="${CPTOOLS2_CONTAINER_DIR:-${CPTOOLS2_PERMANENT_ROOT}/containers}"' in text
    assert 'export CPTOOLS2_MODEL_DIR="${CPTOOLS2_MODEL_DIR:-${CPTOOLS2_PROJECT_ROOT%/}-local/models}"' in text
    assert 'export CPTOOLS2_VENV="${CPTOOLS2_PERMANENT_ROOT}/.venv"' in text
    assert 'export CPTOOLS2_WORK_ROOT="${CPTOOLS2_SCRATCH_ROOT}/work"' in text
    assert 'export CPTOOLS2_PARAMS_ROOT="${CPTOOLS2_SCRATCH_ROOT}/params"' in text
    assert 'export CPTOOLS2_CACHE_ROOT="${CPTOOLS2_WORK_ROOT}/cache"' in text
    assert 'export CPTOOLS2_LOG_ROOT="${CPTOOLS2_SCRATCH_ROOT}/logs"' in text
    assert 'export CPTOOLS2_TRACE_ROOT="${CPTOOLS2_SCRATCH_ROOT}/traces"' in text
    assert 'export CPTOOLS2_TEMP_ROOT="${CPTOOLS2_WORK_ROOT}/tmp"' in text
    assert 'export CPTOOLS2_HOME="${CPTOOLS2_WORK_ROOT}/home/${USER}"' in text
    assert 'export NXF_HOME="${CPTOOLS2_CACHE_ROOT}/nextflow"' in text
    assert 'export NXF_TEMP="${CPTOOLS2_TEMP_ROOT}/nextflow"' in text
    assert "export NXF_OPTS=" in text
    assert "-XX:ActiveProcessorCount=2" in text
    assert "-Djava.io.tmpdir=${NXF_TEMP}" in text
    assert 'export SINGULARITY_CACHEDIR="${CPTOOLS2_CACHE_ROOT}/singularity"' in text
    assert 'export SINGULARITY_TMPDIR="${CPTOOLS2_TEMP_ROOT}/singularity"' in text
    assert 'export APPTAINER_CACHEDIR="${CPTOOLS2_CACHE_ROOT}/apptainer"' in text
    assert 'export APPTAINER_TMPDIR="${CPTOOLS2_TEMP_ROOT}/apptainer"' in text
    assert 'export TMPDIR="${CPTOOLS2_TEMP_ROOT}"' in text
    assert 'export TEMP="${CPTOOLS2_TEMP_ROOT}"' in text
    assert 'export TMP="${CPTOOLS2_TEMP_ROOT}"' in text
    assert 'export XDG_CACHE_HOME="${CPTOOLS2_CACHE_ROOT}/xdg"' in text
    assert 'export XDG_RUNTIME_DIR="${CPTOOLS2_TEMP_ROOT}/xdg-runtime"' in text
    assert 'export MPLCONFIGDIR="${CPTOOLS2_CACHE_ROOT}/matplotlib"' in text
    assert 'export PIP_CACHE_DIR="${CPTOOLS2_CACHE_ROOT}/pip"' in text
    assert '[ -f "${CPTOOLS2_VENV}/bin/activate" ]' in text
    assert '. "${CPTOOLS2_VENV}/bin/activate"' in text
    assert "mkdir -p" in text


def test_eddie_bootstrap_documents_dry_run_usage():
    text = BOOTSTRAP.read_text()

    assert "cptools2 pipeline" in text
    assert "--dry-run" in text
    assert "scripts/configure_eddie_paths.sh" in text


def test_eddie_installer_configures_paths_install_and_containers():
    text = EDDIE_INSTALL.read_text()

    assert "--project-root" in text
    assert "--scratch-root" in text
    assert "--container-action auto|check|build|skip" in text
    assert "scripts/configure_eddie_paths.sh" in text
    assert "REPO_ROOT=\"$(pwd -P)\"" in text
    assert "run this script from the cptools2 repository root" in text
    assert '--output "$PATHS_FILE"' in text
    assert 'PATHS_FILE="${PROJECT_ROOT%/}-local/eddie_paths.env"' in text
    assert 'ACTIVATE_SCRIPT="${expanded_scratch_root}/activate.sh"' in text
    assert 'python -m pip install -e "$REPO_ROOT"' in text
    assert "NEXTFLOW_VERSION" in text
    assert "cellprofiler_4.2.8.sif" in text
    assert "cellpose_sam_1.0.sif" in text
    assert "deepprofiler_1.0.sif" in text
    assert "CONTAINER_MANIFEST" in text
    assert "container_manifest_template.json" in text
    assert "Seeded container manifest" in text
    assert "qsub -v" in text
    assert "CPTOOLS2_SCRATCH_ROOT=${expanded_scratch_root}" in text
    assert '${REPO_ROOT}/cptools2/dockerfiles/build_containers.sh' in text
    assert "/exports/<college>/eddie/<school>/groups/<group>" in text
    assert "/exports/cmvm/eddie/" not in text


def test_container_manifest_template_tracks_metadata_not_binaries():
    text = CONTAINER_MANIFEST_TEMPLATE.read_text()

    assert "keep .sif and Docker archive files out of Git" in text
    assert '"cellprofiler_4.2.8.sif"' in text
    assert '"deepprofiler_1.0.sif"' in text
    assert '"cellpose_sam_1.0.sif"' in text
    assert '"archive": "cellprofiler_4.2.8.tar"' in text
    assert '"docker_image": "cellprofiler/cellprofiler:4.2.8"' in text
    assert '"recipe": "Dockerfile.deepprofiler"' in text
    assert '"sha256": null' in text


def test_container_readme_keeps_images_out_of_git_and_documents_external_builds():
    text = CONTAINER_README.read_text()

    assert "Keep these archives out of Git" in text
    assert "reuse the prebuilt `.sif` files" in text
    assert "container_manifest_template.json" in text
    assert "docker save -o cellprofiler_4.2.8.tar" in text
    assert "sha256sum *.sif" in text


def test_eddie_path_configurator_writes_ignored_env_file():
    text = EDDIE_PATH_CONFIGURATOR.read_text()

    assert "<project-root>-local/eddie_paths.env" in text
    assert "CPTOOLS2_PROJECT_ROOT" in text
    assert "CPTOOLS2_SCRATCH_ROOT" in text
    assert "CPTOOLS2_CONTAINER_DIR" in text
    assert "CPTOOLS2_MODEL_DIR" in text
    assert "--model-dir" in text
    assert "--create-dirs" in text
    assert "/exports/<college>/eddie/<school>/groups/<group>/cptools2" in text
    assert "/exports/cmvm/eddie/" not in text


def test_nextflow_eddie_config_uses_permanent_containers_and_scratch_cache():
    text = EDDIE_CONFIG.read_text()

    assert "def requireEnv" in text
    assert "def cptools2ProjectRoot = requireEnv('CPTOOLS2_PROJECT_ROOT')" in text
    assert "Source config/eddie_env.sh after creating local/eddie_paths.env" in text
    assert "def cptools2ContainerDir = System.getenv('CPTOOLS2_CONTAINER_DIR')" in text
    assert "def cptools2ModelDir = System.getenv('CPTOOLS2_MODEL_DIR')" in text
    assert 'cellprofiler_4.2.8.sif' in text
    assert 'deepprofiler_1.0.sif' in text
    assert 'cellpose_sam_1.0.sif' in text
    assert 'module load roslin/nextflow/25.10.2' in text
    assert 'module load uge/2024.1.0' in text
    assert 'module load singularity/4.3.4' in text
    assert 'module load miniforge/25.3.1-0' in text
    assert 'CPTOOLS2_SCRATCH_ROOT' in text
    assert 'CPTOOLS2_MODEL_DIR' in text
    assert 'CPTOOLS2_VENV' in text
    assert '. "\\$CPTOOLS2_VENV/bin/activate"' in text
    assert 'CPTOOLS2_HOME' in text
    assert 'export HOME="\\$CPTOOLS2_HOME"' in text
    assert "CPTOOLS2_HOME,HOME" not in text
    assert "CPTOOLS2_MODEL_DIR,CPTOOLS2_HOME" in text
    assert "--bind \\$CPTOOLS2_HOME:/home/\\$USER" in text
    assert "--bind \\$CPTOOLS2_MODEL_DIR:\\$CPTOOLS2_MODEL_DIR" in text
    assert "requireEnv('CPTOOLS2_SCRATCH_ROOT')" in text
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
    assert "config/eddie_env.sh" in text
    assert '"logs"' not in text
    assert "/exports/eddie/scratch/$USER/cptools2-ai-update/logs" in text
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
        assert "config/eddie_env.sh" in text
        assert '"logs"' not in text
        assert "/exports/eddie/scratch/$USER/cptools2-ai-update/logs" in text
        assert "SINGULARITY_CACHEDIR" in text
        assert "SINGULARITY_TMPDIR" in text


def test_loop440_smoke_config_uses_tiny_scratch_input_and_permanent_assets():
    text = LOOP440_CONFIG.read_text()
    deepprofiler_config = (
        ROOT / "cptools2" / "templates" / "deepprofiler_config.json"
    ).read_text()

    assert f"input_dir: {SCRATCH_ROOT_VAR}/staging/tiny-plate-set" in text
    assert f"output_dir: {SCRATCH_ROOT_VAR}/results/loop440" in text
    assert "stage_data: false" in text
    assert "max_chunks: 1" in text
    assert "tiny-plate-001: 1" in text
    assert f"container_path: {PROJECT_ROOT_VAR}/containers" not in text
    assert "container_path: ${CPTOOLS2_CONTAINER_DIR}" in text
    assert f"{PROJECT_ROOT_VAR}/cptools2/templates/deepprofiler_config.json" in text
    assert "${CPTOOLS2_MODEL_DIR}/deepprofiler/Cell_Painting_CNN_v1.hdf5" in text
    assert "/exports/eddie/scratch/" not in text
    assert '"file_format": "tif"' in deepprofiler_config


def test_deepprofiler_config_uses_documented_profile_and_train_sections():
    config = json.loads(
        (ROOT / "cptools2" / "templates" / "deepprofiler_config.json").read_text()
    )

    assert config["dataset"]["images"]["channels"] == [
        "DNA",
        "RNA",
        "ER",
        "AGP",
        "Mito",
    ]
    assert "profile" in config
    assert config["profile"]["feature_layer"] == "block6a_activation"
    assert config["profile"]["checkpoint"] == "Cell_Painting_CNN_v1.hdf5"
    assert config["train"]["partition"]["targets"] == ["Metadata_Compound"]
    assert config["train"]["partition"]["split_field"] == "Metadata_Plate"
    assert config["train"]["model"]["name"] == "efficientnet"
    assert config["train"]["model"]["params"]["label_smoothing"] == 0.0
    assert config["train"]["model"]["params"]["online_label_smoothing"] == 0.0
    assert config["train"]["model"]["params"]["online_lambda"] == 0.0
    assert "model" not in config


def test_loop440_tiny_plate_builder_creates_indexable_real_tiffs(tmp_path):
    plate_root = tmp_path / "tiny-plate-set"
    create_loop440_tiny_plate.create_plate(plate_root)

    tiffs = sorted(plate_root.rglob("*.tif"))
    assert len(tiffs) == 5
    assert all(path.stat().st_size > 0 for path in tiffs)
    assert [path.name for path in tiffs] == [
        "val screen_B02_s1_w100000000000000000000000000000001.tif",
        "val screen_B02_s1_w200000000000000000000000000000002.tif",
        "val screen_B02_s1_w300000000000000000000000000000003.tif",
        "val screen_B02_s1_w400000000000000000000000000000004.tif",
        "val screen_B02_s1_w500000000000000000000000000000005.tif",
    ]

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
