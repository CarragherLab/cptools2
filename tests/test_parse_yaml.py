import os
import pytest
from cptools2 import parse_yaml

CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "test_config.yaml")
TEST_PATH2 = os.path.join(CURRENT_PATH, "test_config2.yaml")
TEST_BROKEN = os.path.join(CURRENT_PATH, "test_config_broken.yaml")

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


def test_experiment():
    """cptools2.parse_yaml.experiment(yaml_dict)"""
    yaml_dict = parse_yaml.open_yaml(TEST_PATH)
    output = parse_yaml.experiment(yaml_dict)
    assert output == {"exp_dir" : "/path/to/experiment"}


def test_chunk():
    """cptools2.parse_yaml.chunk(yaml_dict)"""
    yaml_dict = parse_yaml.open_yaml(TEST_PATH)
    output = parse_yaml.chunk(yaml_dict)
    assert output == {"job_size" : 46}


def test_add_plate():
    """cptools2.parse_yaml.add_plate(yaml_dict)"""
    yaml_dict = parse_yaml.open_yaml(TEST_PATH)
    output = parse_yaml.add_plate(yaml_dict)
    assert output == {"exp_dir" : "/path/to/new/experiment",
                      "plates" : ["plate_3", "plate_4"]}


def test_remove_plate():
    """cptools2.parse_yaml.remove_plate(yaml_dict):"""
    yaml_dict = parse_yaml.open_yaml(TEST_PATH)
    output = parse_yaml.remove_plate(yaml_dict)
    assert output == {"plates" : ["plate_1", "plate_2"]}


def test_create_commands():
    """cptools2.parse_yaml.create_commands(yaml_dict)"""
    yaml_dict = parse_yaml.open_yaml(TEST_PATH)
    output = parse_yaml.create_commands(yaml_dict)
    pipeline_loc = os.path.abspath("./tests/example_pipeline.cppipe")
    assert output == {"pipeline" : pipeline_loc,
                      "location" : "/example/location",
                      "commands_location" : "/home/user",
                      "job_size": 46}


def test_new_ix():
    """cptools2.parse_yaml.is_new_ix(yaml_dict)"""
    yaml_dict_1 = parse_yaml.open_yaml(TEST_PATH)
    yaml_dict_2 = parse_yaml.open_yaml(TEST_PATH2)
    assert parse_yaml.is_new_ix(yaml_dict_1) == False
    assert parse_yaml.is_new_ix(yaml_dict_2) == True
    assert parse_yaml.parse_config_file(TEST_PATH).is_new_ix == False
    assert parse_yaml.parse_config_file(TEST_PATH2).is_new_ix == True

