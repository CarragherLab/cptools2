from cptools2 import create_filelist
import os

CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "example_dir")


def test_files_from_plate():
    """see if we get image files from a plate directory"""
    plate_path = os.path.join(TEST_PATH, "test-plate-1")
    output = create_filelist.files_from_plate(plate_path, clean=True)
    assert len(output) > 0
    for f in output:
        assert f.endswith(".tif")


def test_files_from_plate_clean_false():
    """files_from_plate with clean as false"""
    plate_path = os.path.join(TEST_PATH, "test-plate-1")
    output = create_filelist.files_from_plate(plate_path, clean=False)
    assert len(output) > 0


def test_paths_to_plates():
    """
    check paths_to_plates
    though don't actually know what the absolute path is going to be
    on other computers...
    """
    output = create_filelist.paths_to_plates(TEST_PATH)
    prefix = os.path.abspath(TEST_PATH)
    plate_names = ["test-plate-1", "test-plate-2",
                   "test-plate-3", "test-plate-4"]
    make_own = [os.path.join(prefix, name) for name in plate_names]
    assert len(output) == len(plate_names)
    for ans in output:
        assert ans in make_own


def test_exclude_plates_remove_true():
    """test exclude plates"""
    plate_list = create_filelist.paths_to_plates(TEST_PATH)
    exclude_these = ["test-plate-3", "test-plate-4"]
    sub_plate_list = create_filelist.exclude_plates(plate_list, exclude_these,
                                                    remove=True)
    for ans in sub_plate_list:
        assert ans.split(os.sep)[-1] not in exclude_these


def test_exclude_plates_remove_false():
    """test exclude plates with remove=False"""
    plate_list = create_filelist.paths_to_plates(TEST_PATH)
    include_these = ["test-plate-3", "test-plate-4"]
    sub_plate_list = create_filelist.exclude_plates(plate_list, include_these,
                                                    remove=False)
    for ans in sub_plate_list:
        assert ans.split(os.sep)[-1] in include_these
