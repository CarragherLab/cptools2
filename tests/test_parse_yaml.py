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

