import os
from cptools2 import parse_yaml

CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "test_config.yaml")


def test_open_yaml():
    """cptools2.parse_yaml.open_yaml(path_to_yaml)"""
    yaml_dict = parse_yaml.open_yaml(TEST_PATH)
    assert isinstance(yaml_dict, dict)


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
    assert output == {"experiment" : "/path/to/new/experiment",
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
    assert output == {"pipeline" : "/path/to/example_pipeline.cppipe",
                      "location" : "/example/location",
                      "commands_location" : "/home/user"}



