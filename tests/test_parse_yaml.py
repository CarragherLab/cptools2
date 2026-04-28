import json
import os

import pytest

from cptools2 import parse_yaml

CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "test_config.yaml")
TEST_PATH2 = os.path.join(CURRENT_PATH, "test_config2.yaml")
TEST_BROKEN = os.path.join(CURRENT_PATH, "test_config_broken.yaml")
PIPELINE_CONFIG_PATH = os.path.join(CURRENT_PATH, "pipeline_config.yaml")


def test_open_yaml():
    """cptools2.parse_yaml.open_yaml(path_to_yaml)"""
    yaml_dict = parse_yaml.open_yaml(TEST_PATH)
    assert isinstance(yaml_dict, dict)
    yaml_dict = parse_yaml.open_yaml(TEST_PATH2)
    assert isinstance(yaml_dict, dict)


def test_check_yaml_args():
    """cptools2.parse_yaml.check_yaml_args(yaml_dict)"""
    yaml_dict = parse_yaml.open_yaml(TEST_BROKEN)
    with pytest.raises(ValueError):
        parse_yaml.check_yaml_args(yaml_dict)


def test_check_yaml_args_accepts_new_keys():
    """check_yaml_args accepts channels, stages, segmentation, feature_extraction, containers"""
    yaml_dict = parse_yaml.open_yaml(PIPELINE_CONFIG_PATH)
    # should not raise
    parse_yaml.check_yaml_args(yaml_dict)


def test_check_yaml_args_accepts_chunking_keys():
    yaml_dict = {
        "experiment": "/tmp/input",
        "location": "/tmp/output",
        "expected_channels": [1, 2, 3, 4, 5],
        "max_chunks": 2,
        "scratch_quota_gb": 500,
    }
    parse_yaml.check_yaml_args(yaml_dict)


def test_ai_feature_extraction_rejects_non_cellpose_segmentation():
    yaml_dict = {
        "experiment": "/tmp/input",
        "location": "/tmp/output",
        "feature_extraction": {"tool": "deepprofiler"},
        "segmentation": {"engine": "cellprofiler"},
    }
    with pytest.raises(ValueError, match="requires Cellpose"):
        parse_yaml.validate_ai_segmentation_contract(yaml_dict)


def test_ai_feature_extraction_allows_default_cellpose_path():
    yaml_dict = {
        "experiment": "/tmp/input",
        "location": "/tmp/output",
        "feature_extraction": {"tool": "deepprofiler"},
    }
    parse_yaml.validate_ai_segmentation_contract(yaml_dict)


def test_generate_params_json_maps_cellpose_segmentation_options(tmp_path):
    config = {
        "experiment_args": {"exp_dir": "/tmp/input"},
        "create_command_args": {"location": str(tmp_path)},
        "chunk_args": {"job_size": 96},
        "stage_data": True,
        "segmentation": {
            "engine": "cellpose",
            "diameter": 42,
            "channel": 1,
            "model": "cpsam",
            "batch_size": 4,
        },
    }
    params_path = tmp_path / "params.json"

    parse_yaml.generate_params_json(config, params_path)

    with open(params_path) as handle:
        params = json.load(handle)
    assert params["cellpose_diameter"] == 42
    assert params["cellpose_channel"] == 1
    assert params["cellpose_model"] == "cpsam"
    assert params["cellpose_batch_size"] == 4


def test_experiment():
    """cptools2.parse_yaml.experiment(yaml_dict)"""
    yaml_dict = parse_yaml.open_yaml(TEST_PATH)
    output = parse_yaml.experiment(yaml_dict)
    assert output == {"exp_dir": "/path/to/experiment"}


def test_chunk():
    """cptools2.parse_yaml.chunk(yaml_dict)"""
    yaml_dict = parse_yaml.open_yaml(TEST_PATH)
    output = parse_yaml.chunk(yaml_dict)
    assert output == {"job_size": 46}


def test_add_plate():
    """cptools2.parse_yaml.add_plate(yaml_dict)"""
    yaml_dict = parse_yaml.open_yaml(TEST_PATH)
    output = parse_yaml.add_plate(yaml_dict)
    assert output == {
        "exp_dir": "/path/to/new/experiment",
        "plates": ["plate_3", "plate_4"],
    }


def test_remove_plate():
    """cptools2.parse_yaml.remove_plate(yaml_dict):"""
    yaml_dict = parse_yaml.open_yaml(TEST_PATH)
    output = parse_yaml.remove_plate(yaml_dict)
    assert output == {"plates": ["plate_1", "plate_2"]}


def test_create_commands():
    """cptools2.parse_yaml.create_commands(yaml_dict)"""
    yaml_dict = parse_yaml.open_yaml(TEST_PATH)
    output = parse_yaml.create_commands(yaml_dict)
    pipeline_loc = os.path.abspath("./tests/example_pipeline.cppipe")
    assert output == {
        "pipeline": pipeline_loc,
        "location": "/example/location",
        "commands_location": "/home/user",
        "job_size": 46,
    }


def test_new_ix():
    """is_new_ix always returns False — layout is auto-detected per plate"""
    yaml_dict_1 = parse_yaml.open_yaml(TEST_PATH)
    yaml_dict_2 = parse_yaml.open_yaml(TEST_PATH2)
    # is_new_ix is a legacy stub — always returns False
    assert parse_yaml.is_new_ix(yaml_dict_1) is False
    assert parse_yaml.is_new_ix(yaml_dict_2) is False


def test_parse_config_file_returns_dict():
    """parse_config_file returns a plain dict, not a namedtuple"""
    config = parse_yaml.parse_config_file(TEST_PATH)
    assert isinstance(config, dict)
    assert "experiment_args" in config
    assert "chunk_args" in config
    assert "create_command_args" in config
    assert "is_new_ix" in config


def test_parse_config_file_new_keys():
    """parse_config_file includes new keys from pipeline config"""
    config = parse_yaml.parse_config_file(PIPELINE_CONFIG_PATH)
    assert isinstance(config, dict)
    assert config["channels"] == ["DAPI", "GFP", "CY5", "CY3", "BF"]
    assert config["stages"] == ["illum", "segment", "extract"]
    assert config["segmentation"] is not None
    assert config["feature_extraction"] is not None
    assert config["containers"] is not None


def test_resolve_stages():
    """resolve_stages expands illum, segment, extract and validates stage names"""
    assert parse_yaml.resolve_stages(["illum"]) == [
        "illum_calculate",
        "illum_apply",
    ]
    assert parse_yaml.resolve_stages(["segment"]) == ["segmentation"]
    assert parse_yaml.resolve_stages(["extract"]) == ["feature_extract"]
    assert parse_yaml.resolve_stages(["illum", "segment"]) == [
        "illum_calculate",
        "illum_apply",
        "segmentation",
    ]
    # None input returns None
    assert parse_yaml.resolve_stages(None) is None
    # valid stage name passes through unchanged
    assert parse_yaml.resolve_stages(["segmentation"]) == ["segmentation"]
    assert parse_yaml.resolve_stages(["illum_calculate"]) == ["illum_calculate"]
    # deduplication: illum + illum_calculate => no duplicate illum_calculate
    assert parse_yaml.resolve_stages(["illum", "illum_calculate"]) == [
        "illum_calculate",
        "illum_apply",
    ]
    # unrecognized stage raises ValueError
    with pytest.raises(ValueError, match="Unrecognized stage"):
        parse_yaml.resolve_stages(["not_a_stage"])


def test_generate_params_json(tmp_path):
    """generate_params_json writes valid JSON; stages must be pre-expanded by caller."""
    config = parse_yaml.parse_config_file(PIPELINE_CONFIG_PATH)
    # CLI pre-expands stages before calling generate_params_json
    if config.get("stages"):
        config["stages"] = parse_yaml.resolve_stages(config["stages"])
    output_path = str(tmp_path / "params.json")
    parse_yaml.generate_params_json(config, output_path)
    assert os.path.isfile(output_path)
    with open(output_path) as f:
        params = json.load(f)
    assert isinstance(params, dict)
    # stages should be expanded (pre-expanded by caller)
    assert "stages" in params
    assert "illum_calculate" in params["stages"]
    assert "illum_apply" in params["stages"]
    assert params["channels"] == ["DAPI", "GFP", "CY5", "CY3", "BF"]
    assert params["input_dir"] == "/path/to/experiment"
    assert params["output_dir"] == "/example/location"


def test_generate_params_json_minimal(tmp_path):
    """generate_params_json works with legacy config (no new keys)"""
    config = parse_yaml.parse_config_file(TEST_PATH)
    output_path = str(tmp_path / "params.json")
    parse_yaml.generate_params_json(config, output_path)
    with open(output_path) as f:
        params = json.load(f)
    assert isinstance(params, dict)
    # should not have stages/channels since legacy config lacks them
    assert "stages" not in params
    assert "channels" not in params
    assert params["input_dir"] == "/path/to/experiment"
    assert params["output_dir"] == "/example/location"


def test_resolve_stages_none():
    """resolve_stages(None) returns None."""
    assert parse_yaml.resolve_stages(None) is None


def test_resolve_stages_empty_list():
    """resolve_stages([]) returns []."""
    assert parse_yaml.resolve_stages([]) == []


def test_resolve_stages_dedup():
    """resolve_stages deduplicates when alias overlaps with expanded name."""
    result = parse_yaml.resolve_stages(["illum", "illum_calculate"])
    assert result == ["illum_calculate", "illum_apply"]


def test_resolve_stages_validation():
    """resolve_stages raises ValueError on unrecognized stage."""
    with pytest.raises(ValueError, match="Unrecognized stage"):
        parse_yaml.resolve_stages(["typo_stage"])


def test_generate_params_json_custom_channels(tmp_path):
    """generate_params_json handles non-Cell-Painting channel lists."""
    config = parse_yaml.parse_config_file(PIPELINE_CONFIG_PATH)
    config["channels"] = ["DAPI", "GFP", "mCherry"]
    output_path = str(tmp_path / "params.json")
    parse_yaml.generate_params_json(config, output_path)
    with open(output_path) as f:
        params = json.load(f)
    assert params["channels"] == ["DAPI", "GFP", "mCherry"]


def test_parse_config_file_explicit_plates_and_staging(tmp_path):
    """parse_config_file accepts explicit plates and stage_data for DataStore runs."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "input_dir: /exports/igmm/datastore/ImageXpress2020/imagexpress/Sarah-screen\n"
        "output_dir: /exports/eddie/scratch/mharvey2/cptools2-loop230\n"
        "pipeline: tests/example_pipeline.cppipe\n"
        "commands location: /tmp/commands\n"
        "plates:\n"
        "  - 3723-D-100\n"
        "stage_data: true\n"
    )

    config = parse_yaml.parse_config_file(str(config_file))

    assert config["experiment_args"] == {
        "exp_dir": "/exports/igmm/datastore/ImageXpress2020/imagexpress/Sarah-screen"
    }
    assert config["create_command_args"]["location"] == (
        "/exports/eddie/scratch/mharvey2/cptools2-loop230"
    )
    assert config["plates"] == ["3723-D-100"]
    assert config["stage_data"] is True


def test_generate_params_json_explicit_plates_and_staging(tmp_path):
    """generate_params_json emits Nextflow plates and stage_data params."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "input_dir: /exports/igmm/datastore/ImageXpress2020/imagexpress/Sarah-screen\n"
        "output_dir: /exports/eddie/scratch/mharvey2/cptools2-loop230\n"
        "pipeline: tests/example_pipeline.cppipe\n"
        "commands location: /tmp/commands\n"
        "plates: 3723-D-100,13738-D-30\n"
        "stage_data: true\n"
    )
    config = parse_yaml.parse_config_file(str(config_file))
    output_path = str(tmp_path / "params.json")
    parse_yaml.generate_params_json(config, output_path)

    with open(output_path) as f:
        params = json.load(f)

    assert params["plates"] == ["3723-D-100", "13738-D-30"]
    assert params["stage_data"] is True
    assert params["input_dir"] == (
        "/exports/igmm/datastore/ImageXpress2020/imagexpress/Sarah-screen"
    )
    assert params["output_dir"] == "/exports/eddie/scratch/mharvey2/cptools2-loop230"


def test_create_commands_defaults_commands_location_to_output_dir():
    """commands location defaults to <output_dir>/commands."""
    yaml_dict = {
        "pipeline": "tests/example_pipeline.cppipe",
        "output_dir": "/exports/eddie/scratch/mharvey2/cptools2-loop230",
    }

    result = parse_yaml.create_commands(yaml_dict)

    assert result["commands_location"] == (
        "/exports/eddie/scratch/mharvey2/cptools2-loop230/commands"
    )


def test_generate_params_json_nextflow_pipeline_paths(tmp_path):
    """generate_params_json emits explicit Nextflow CellProfiler pipeline paths."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "input_dir: /exports/igmm/datastore/ImageXpress2020/imagexpress/Sarah-screen\n"
        "output_dir: /exports/eddie/scratch/mharvey2/cptools2-loop230\n"
        "pipeline: /exports/eddie/scratch/mharvey2/cptools2-loop230/pipelines/nuclear_segmentation.cppipe\n"
        "illum_pipeline_calculate: /exports/eddie/scratch/mharvey2/cptools2-loop230/pipelines/illum_calculate.cppipe\n"
        "illum_pipeline_apply: /exports/eddie/scratch/mharvey2/cptools2-loop230/pipelines/illum_apply.cppipe\n"
        "seg_pipeline: /exports/eddie/scratch/mharvey2/cptools2-loop230/pipelines/nuclear_segmentation.cppipe\n"
        "stage_data: true\n"
    )
    config = parse_yaml.parse_config_file(str(config_file))
    output_path = str(tmp_path / "params.json")
    parse_yaml.generate_params_json(config, output_path)

    with open(output_path) as f:
        params = json.load(f)

    assert params["illum_pipeline_calculate"].endswith("/pipelines/illum_calculate.cppipe")
    assert params["illum_pipeline_apply"].endswith("/pipelines/illum_apply.cppipe")
    assert params["seg_pipeline"].endswith("/pipelines/nuclear_segmentation.cppipe")


def test_datastore_input_requires_stage_data(tmp_path):
    """DataStore-backed YAML configs must stage data before compute."""
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        "input_dir: /exports/igmm/datastore/ImageXpress2020/imagexpress/Sarah-screen\n"
        "output_dir: /exports/eddie/scratch/mharvey2/cptools2-loop230\n"
        "pipeline: tests/example_pipeline.cppipe\n"
        "stage_data: false\n"
    )

    with pytest.raises(ValueError, match="DataStore inputs must use stage_data"):
        parse_yaml.parse_config_file(str(config_file))


def test_generate_params_json_feature_extraction_tool_alias(tmp_path):
    """generate_params_json emits Nextflow feature extraction aliases."""
    config = parse_yaml.parse_config_file(PIPELINE_CONFIG_PATH)
    config["feature_extraction"] = {
        "tool": "deepprofiler",
        "config": "/models/deepprofiler/config.json",
        "weights": "/models/deepprofiler/model.ckpt",
        "batch_size": 32,
    }
    output_path = str(tmp_path / "params.json")
    parse_yaml.generate_params_json(config, output_path)
    with open(output_path) as f:
        params = json.load(f)
    assert params["feature_extraction_tool"] == "deepprofiler"
    assert params["feature_extraction_config"] == "/models/deepprofiler/config.json"
    assert params["feature_extraction_weights"] == "/models/deepprofiler/model.ckpt"
    assert params["feature_extraction_batch_size"] == 32

