import json
import os

import pytest

from cptools2 import parse_yaml

CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "test_config.yaml")
TEST_PATH2 = os.path.join(CURRENT_PATH, "test_config2.yaml")
TEST_BROKEN = os.path.join(CURRENT_PATH, "test_config_broken.yaml")
PIPELINE_CONFIG = os.path.join(CURRENT_PATH, "pipeline_config.yaml")


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
    """check_yaml_args accepts channels, stages, segmentation, feature_extraction, containers."""
    new_keys_dict = {
        "channels": ["DAPI", "GFP", "Mito"],
        "stages": ["illum_calculate", "segment"],
        "segmentation": {"model": "cellpose"},
        "feature_extraction": {"pipeline": "cell_painting.cppipe"},
        "containers": {"cellprofiler": "cp.sif"},
    }
    # Should not raise
    parse_yaml.check_yaml_args(new_keys_dict)


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


def test_new_ix_removed_from_yaml_schema():
    """`new_ix` is no longer a valid YAML key; layout is auto-detected per-plate."""
    # test_config.yaml parses fine
    _ = parse_yaml.parse_config_file(TEST_PATH)
    # test_config2.yaml should still parse after fixture update
    _ = parse_yaml.parse_config_file(TEST_PATH2)


def test_parse_config_file_returns_dict():
    """parse_config_file returns a plain dict, not a namedtuple."""
    config = parse_yaml.parse_config_file(TEST_PATH)
    assert isinstance(config, dict)
    assert "experiment_args" in config
    assert "chunk_args" in config
    assert "create_command_args" in config
    assert "raw" in config


def test_parse_config_file_legacy_keys():
    """parse_config_file correctly parses legacy config keys."""
    config = parse_yaml.parse_config_file(TEST_PATH)
    assert config["experiment_args"] == {"exp_dir": "/path/to/experiment"}
    assert config["chunk_args"] == {"job_size": 46}
    assert config["remove_plate_args"] == {"plates": ["plate_1", "plate_2"]}


def test_parse_config_file_new_keys_absent():
    """New pipeline keys are None when not present in legacy config."""
    config = parse_yaml.parse_config_file(TEST_PATH)
    assert config["channels"] is None
    assert config["stages"] is None
    assert config["segmentation"] is None
    assert config["feature_extraction"] is None
    assert config["containers"] is None


def test_parse_config_file_pipeline_config():
    """parse_config_file parses new pipeline-style config with stages."""
    config = parse_yaml.parse_config_file(PIPELINE_CONFIG)
    assert isinstance(config, dict)
    assert config["channels"] == ["DAPI", "GFP", "Mito", "AGP", "DNA"]
    # "illum" alias should be expanded
    assert "illum_calculate" in config["stages"]
    assert "illum_apply" in config["stages"]
    assert config["segmentation"] is not None
    assert config["containers"] is not None


# --- resolve_stages tests ---


def test_resolve_stages_direct():
    """Direct stage names pass through unchanged."""
    result = parse_yaml.resolve_stages(["illum_calculate", "segment"])
    assert result == ["illum_calculate", "segment"]


def test_resolve_stages_alias_illum():
    """'illum' alias expands to illum_calculate + illum_apply."""
    result = parse_yaml.resolve_stages(["illum"])
    assert result == ["illum_calculate", "illum_apply"]


def test_resolve_stages_alias_qc():
    """'qc' alias expands to qc_plate + qc_well."""
    result = parse_yaml.resolve_stages(["qc"])
    assert result == ["qc_plate", "qc_well"]


def test_resolve_stages_mixed():
    """Mix of aliases and direct names resolves correctly."""
    result = parse_yaml.resolve_stages(["illum", "segment", "feature_extract"])
    assert result == ["illum_calculate", "illum_apply", "segment", "feature_extract"]


def test_resolve_stages_deduplication():
    """Duplicate stages are removed while preserving order."""
    result = parse_yaml.resolve_stages(["illum", "illum_calculate"])
    assert result == ["illum_calculate", "illum_apply"]


def test_resolve_stages_unknown_raises():
    """Unknown stage name raises ValueError."""
    with pytest.raises(ValueError, match="Unrecognized stage 'bogus'"):
        parse_yaml.resolve_stages(["bogus"])


# --- generate_params_json tests ---


def test_generate_params_json_legacy_config(tmp_path):
    """generate_params_json maps legacy experiment/location to input_dir/output_dir."""
    config = parse_yaml.parse_config_file(TEST_PATH)
    output = tmp_path / "params.json"
    result_path = parse_yaml.generate_params_json(config, str(output))
    assert os.path.isfile(result_path)
    with open(result_path) as f:
        params = json.load(f)
    assert params["input_dir"] == "/path/to/experiment"
    assert params["output_dir"] == "/example/location"
    assert params["chunk_size"] == 46


def test_generate_params_json_pipeline_config(tmp_path):
    """generate_params_json includes stages, channels, containers from pipeline config."""
    config = parse_yaml.parse_config_file(PIPELINE_CONFIG)
    output = tmp_path / "params.json"
    result_path = parse_yaml.generate_params_json(config, str(output))
    with open(result_path) as f:
        params = json.load(f)
    assert "stages" in params
    assert "illum_calculate" in params["stages"]
    assert "channels" in params
    assert params["channels"] == ["DAPI", "GFP", "Mito", "AGP", "DNA"]
    assert "containers" in params


def test_generate_params_json_valid_json(tmp_path):
    """Output file is valid JSON."""
    config = parse_yaml.parse_config_file(TEST_PATH)
    output = tmp_path / "params.json"
    parse_yaml.generate_params_json(config, str(output))
    with open(output) as f:
        data = json.load(f)
    assert isinstance(data, dict)
