from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_nextflow_container_tags_match_local_builds():
    config = (ROOT / "nextflow" / "conf" / "containers.config").read_text()

    assert "cellprofiler/cellprofiler:4.2.8" in config
    assert "cptools2/deepprofiler:1.0" in config
    assert "cptools2/cellpose_sam:1.0" in config


def test_nextflow_main_accepts_documented_stage_aliases():
    main_nf = (ROOT / "nextflow" / "main.nf").read_text()

    assert "hasStage('illum')" in main_nf
    assert "hasStage('segment')" in main_nf
    assert "hasStage('extract')" in main_nf
    assert "hasStage('illum_calculate')" in main_nf
    assert "hasStage('segmentation')" in main_nf
    assert "hasStage('feature_extract')" in main_nf


def test_loop230_config_is_scratch_self_contained_and_staged():
    config_path = ROOT / "config" / "loop230-sarah-screen.yaml"
    config = yaml.safe_load(config_path.read_text())

    output_dir = config["output_dir"].rstrip("/")
    assert config["stage_data"] is True
    assert "/datastore/" in config["input_dir"]
    assert config["plates"] == ["3723-D-100"]
    assert config["stages"] == ["illum", "segment", "extract"]

    scratch_bound_paths = [
        config["illum_pipeline_calculate"],
        config["illum_pipeline_apply"],
        config["seg_pipeline"],
        config["pipeline"],
        config["commands location"],
        config["feature_extraction"]["config"],
    ]
    for path in scratch_bound_paths:
        assert path.startswith(output_dir + "/")
        assert "/home/" not in path


def test_container_dockerfiles_expose_working_cli_contracts():
    deep = (ROOT / "cptools2" / "dockerfiles" / "Dockerfile.deepprofiler").read_text()
    cellpose = (ROOT / "cptools2" / "dockerfiles" / "Dockerfile.cellpose").read_text()
    readme = (ROOT / "cptools2" / "dockerfiles" / "README.md").read_text()

    assert "/opt/DeepProfiler" in deep
    assert "PYTHONPATH=/opt/DeepProfiler" in deep
    assert "cellpose[gpu]" not in cellpose
    assert "cptools2/cellpose_sam:1.0" in readme
    assert "cptools2/cellpose-sam:1.0" not in readme
