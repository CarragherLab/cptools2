"""
module docstring
"""

import os
from cptools2 import generate_scripts

CURRENT_PATH = os.path.dirname(__file__)
TEST_DIR_PATH = os.path.join(CURRENT_PATH, "example_commands")

def test_make_command_paths():
    """cptools2.generate_scripts.make_commands_paths(commands_location)"""
    paths = generate_scripts.make_command_paths(TEST_DIR_PATH)
    names = ["staging", "cp_commands", "destaging"]
    # check name is a key in the output dictionary
    for name in names:
        assert name in paths
    for path in paths.values():
        assert os.path.isfile(path)
    # check that the names match up to the paths
    # i.e {"staging": "/directory/staging.txt"}
    for name, path in paths.items():
        assert name in path


def test_lines_in_commnads():
    """cptools2.generate_scripts.lines_in_commands(commnads_location)"""
    output = generate_scripts.lines_in_commands(TEST_DIR_PATH)
    expected_count = 5
    expected_names = ["staging", "cp_commands", "destaging"]
    for count in output.values():
        assert count == expected_count
    for name in expected_names:
        assert name in output
    assert len(output.values()) == 3
