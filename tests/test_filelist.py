import os
from cptools2 import filelist

CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "example_dir")
TEST_PATH_NEW = os.path.join(CURRENT_PATH, "example_dir_new_paths")


def test_files_from_plate():
    """see if we get image files from a plate directory"""
    plate_path = os.path.join(TEST_PATH, "test-plate-1")
    output = filelist.files_from_plate(
        plate_path, clean=True, truncate=False
    )
    assert len(output) > 0
    for f in output:
        assert f.endswith(".tif")
    # with new IX paths
    plate_path_new = os.path.join(TEST_PATH_NEW, "test-plate-1")
    output_new = filelist.files_from_plate(
        plate_path_new, clean=True, truncate=False, is_new_ix=True
    )
    assert len(output_new) > 0
    for f in output_new:
        assert f.endswith(".tif")


def test_files_from_plate_clean_false():
    """files_from_plate with clean as false"""
    plate_path = os.path.join(TEST_PATH, "test-plate-1")
    output = filelist.files_from_plate(plate_path,
        clean=False, truncate=False)
    assert len(output) > 0


def test_files_from_plate_truncate():
    """files_from_plate with truncated file-paths"""
    plate_path = os.path.join(TEST_PATH, "test-plate-1")
    output = filelist.files_from_plate(plate_path,
                                       clean=True, truncate=True)
    for f in output:
        assert len(f.split(os.sep)) == 4


def test_paths_to_plates():
    """
    check paths_to_plates
    though don't actually know what the absolute path is going to be
    on other computers...
    """
    output = filelist.paths_to_plates(TEST_PATH)
    prefix = os.path.abspath(TEST_PATH)
    plate_names = ["test-plate-1", "test-plate-2",
                   "test-plate-3", "test-plate-4"]
    make_own = [os.path.join(prefix, name) for name in plate_names]
    assert len(output) == len(plate_names)
    for ans in output:
        assert ans in make_own

