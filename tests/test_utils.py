import os
from cptools2 import utils
import pandas as pd

CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "example_dir")
TEST_PATH_PLATE_1 = os.path.join(TEST_PATH, "test-plate-1")


def test_make_cp_cmnd():
    """utils.make_cp_cmnd(name, pipeline, location, output_loc)"""
    name = "test_name"
    pipeline = "test_pipeline.cppipe"
    location = "/path/to/test_location"
    output_loc = "/path/to/output_location"
    cmnd = utils.make_cp_cmnd(name, pipeline, location, output_loc)
    # will create a loaddata name from $location/loaddata/name
    correct = "cellprofiler -r -c -p test_pipeline.cppipe --data-file=/path/to/test_location/loaddata/test_name.csv -o /path/to/output_location"
    assert cmnd == correct


def make_rsync_cmnd():
    """utils.make_rsync_cmnd(plate_loc, filelist_name, img_location)"""
    plate_loc = "/plate_location"
    filelist_name = "/path/to/filelist"
    img_location = "/path/to/images"
    cmnd = utils.make_rsync_cmnd(plate_loc, filelist_name, img_location)
    correct = "rsync --files-from=/path/to/filelist /plate_location /path/to/images"
    assert cmnd == correct


def test_flatten():
    """utils.flatten(list)"""
    test1 = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    out1 = list(utils.flatten(test1))
    assert out1 == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    test2 = [ [[1, 2], [3, 4]], [[5, 6], [7, 8]] ]
    out2 = list(utils.flatten(test2))
    assert out2 == [1, 2, 3, 4, 5, 6, 7, 8]


def test_prefix_filepaths_simulated():
    """utils.prefix_filepaths(dataframe, location)"""
    # create a simulated dataframe
    test_df = pd.DataFrame({
        "x" : [1, 2, 3],
        "PathName_W1" : ["one", "two", "three"],
        "PathName_W2" : ["a", "b", "c"]})
    location = "/test/location"
    output_df = utils.prefix_filepaths(test_df, location)
    assert test_df.shape == output_df.shape
    assert output_df["PathName_W1"].tolist() == ["/test/location/img_data/one",
                                                 "/test/location/img_data/two",
                                                 "/test/location/img_data/three"]
    assert output_df["PathName_W2"].tolist() == ["/test/location/img_data/a",
                                                 "/test/location/img_data/b",
                                                 "/test/location/img_data/c"]

