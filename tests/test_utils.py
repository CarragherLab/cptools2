import os
import collections
from cptools2 import utils
import pandas as pd

CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "example_dir")
TEST_PATH_PLATE_1 = os.path.join(TEST_PATH, "test-plate-1")


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
    name="test_name"
    output_df = utils.prefix_filepaths(test_df, name, location)
    assert test_df.shape == output_df.shape
    assert output_df["PathName_W1"].tolist() == ["/test/location/img_data/test_name/one",
                                                 "/test/location/img_data/test_name/two",
                                                 "/test/location/img_data/test_name/three"]
    assert output_df["PathName_W2"].tolist() == ["/test/location/img_data/test_name/a",
                                                 "/test/location/img_data/test_name/b",
                                                 "/test/location/img_data/test_name/c"]


