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


def test_join_script_quotes_plate_names_with_spaces(tmp_path):
    """Generated join scripts must shell-quote plate names so spaces are preserved."""
    class DummyConfig:
        join_files_patterns = ["data.csv"]
        create_command_args = {"location": "/tmp/outputs"}
        data_destination_path = None

    script_path = generate_scripts.make_join_files_script(
        config=DummyConfig(),
        commands_location=str(tmp_path),
        logfile_location=str(tmp_path / "logs"),
        job_name="join_test",
        timestamp="20250101_000000",
        dependency_job_name=None,
        plates_to_join=["2025-11-27 RC17 Con vs PSP"],
        batch_id=None,
    )
    assert script_path is not None
    with open(script_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Plate name must appear as a single shell argument (quoted).
    assert " --plates " in content
    assert "'2025-11-27 RC17 Con vs PSP'" in content