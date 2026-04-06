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


def test_expand_stage_aliases():
    """expand_stage_aliases expands illum, segment, extract"""
    assert parse_yaml.expand_stage_aliases(["illum"]) == [
        "illum_calculate",
        "illum_apply",
    ]
    assert parse_yaml.expand_stage_aliases(["segment"]) == ["segmentation"]
    assert parse_yaml.expand_stage_aliases(["extract"]) == ["feature_extraction"]
    assert parse_yaml.expand_stage_aliases(["illum", "segment"]) == [
        "illum_calculate",
        "illum_apply",
        "segmentation",
    ]
    assert parse_yaml.expand_stage_aliases(None) is None
    # non-alias passes through
    assert parse_yaml.expand_stage_aliases(["segmentation"]) == ["segmentation"]


def test_generate_params_json(tmp_path):
    """generate_params_json writes valid JSON"""
    config = parse_yaml.parse_config_file(PIPELINE_CONFIG_PATH)
    output_path = str(tmp_path / "params.json")
    parse_yaml.generate_params_json(config, output_path)
    assert os.path.isfile(output_path)
    with open(output_path) as f:
        params = json.load(f)
    assert isinstance(params, dict)
    # stages should be expanded
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

