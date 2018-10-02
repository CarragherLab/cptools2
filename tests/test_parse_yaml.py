import os

import pytest
from cptools2 import parse_yaml

CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "test_config.yaml")
TEST_BROKEN = os.path.join(CURRENT_PATH, "test_config_broken.yaml")
TEST_NO_CHUNK = os.path.join(CURRENT_PATH, "test_config_no_chunk.yaml")

def test_open_yaml():
    config = parse_yaml.ConfigParser(TEST_PATH)
    config_contents = config.config_contents
    assert isinstance(config_contents, dict)


def test_check_config_is_valid():
    with pytest.raises(ValueError):
        parse_yaml.ConfigParser(TEST_BROKEN)


def test_experiment():
    config = parse_yaml.ConfigParser(TEST_PATH)
    output = config.experiment
    assert output == {"exp_dir": "/path/to/experiment"}


def test_chunk():
    config = parse_yaml.ConfigParser(TEST_PATH)
    output = config.chunk
    assert output == {"job_size": 46}


def test_add_plate():
    config = parse_yaml.ConfigParser(TEST_PATH)
    output = config.add_plate
    assert output == {"exp_dir" : "/path/to/new/experiment",
                      "plates" : ["plate_3", "plate_4"]}


def test_remove_plate():
    config = parse_yaml.ConfigParser(TEST_PATH)
    output = config.remove_plate
    assert output == {"plates" : ["plate_1", "plate_2"]}


def test_pipeline():
    config = parse_yaml.ConfigParser(TEST_PATH)
    output = config.pipeline
    pipeline_loc = os.path.abspath("./tests/example_pipeline.cppipe")
    assert output == {"pipeline": pipeline_loc}


def test_location():
    config = parse_yaml.ConfigParser(TEST_PATH)
    output = config.location
    assert output == {"location": "/example/location"}


def test_commands_location():
    config = parse_yaml.ConfigParser(TEST_PATH)
    output = config.commands_location
    assert output == {"commands_location": "/home/user"}
