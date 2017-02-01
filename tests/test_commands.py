import os
from cptools2 import commands

CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "example_dir")
TEST_PATH_PLATE_1 = os.path.join(TEST_PATH, "test-plate-1")


def test_make_cp_cmnd():
    """cptools2.commands.make_cp_cmnd(name, pipeline, location, output_loc)"""
    name = "test_name"
    pipeline = "test_pipeline.cppipe"
    location = "/path/to/test_location"
    output_loc = "/path/to/output_location"
    cmnd = commands.make_cp_cmnd(name, pipeline, location, output_loc)
    # will create a loaddata name from $location/loaddata/name
    correct = "cellprofiler -r -c -p test_pipeline.cppipe --data-file=/path/to/test_location/loaddata/test_name.csv -o /path/to/output_location"
    assert cmnd == correct


def make_rsync_cmnd():
    """cptools2.commands.make_rsync_cmnd(plate_loc, filelist_name, img_location)"""
    plate_loc = "/plate_location"
    filelist_name = "/path/to/filelist"
    img_location = "/path/to/images"
    cmnd = commands.make_rsync_cmnd(plate_loc, filelist_name, img_location)
    correct = "rsync --files-from=/path/to/filelist /plate_location /path/to/images"
    assert cmnd == correct
