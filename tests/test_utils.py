import os
from cptools2 import utils
import pandas as pd
import numpy as np

CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "example_dir")
TEST_PATH_PLATE_1 = os.path.join(TEST_PATH, "test-plate-1")


def test_flatten():
    """utils.flatten(list)"""
    test1 = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
    out1 = list(utils.flatten(test1))
    assert out1 == [1, 2, 3, 4, 5, 6, 7, 8, 9]
    test2 = [[[1, 2], [3, 4]], [[5, 6], [7, 8]]]
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
    name = "test_name"
    output_df = utils.prefix_filepaths(test_df, name, location)
    assert test_df.shape == output_df.shape
    assert output_df["PathName_W1"].tolist() == ["/test/location/img_data/test_name/one",
                                                 "/test/location/img_data/test_name/two",
                                                 "/test/location/img_data/test_name/three"]
    assert output_df["PathName_W2"].tolist() == ["/test/location/img_data/test_name/a",
                                                 "/test/location/img_data/test_name/b",
                                                 "/test/location/img_data/test_name/c"]


def test_any_nan_values():
    """utils.any_nan_values(dataframe)"""
    # create test DataFrame
    test_df = pd.DataFrame({
        "x" : [1, 2, 3],
        "y" : [1, 2, np.nan],
        "z" : [3, 2, 1]
    })
    test_df_2 = pd.DataFrame({
        "x" : [1, 2, 3],
        "y" : [1, 2, 3],
        "z" : [3, 2, 1]
    })
    assert utils.any_nan_values(test_df) == True
    assert utils.any_nan_values(test_df_2) == False


def test_count_lines_in_file():
    """utils.count_lines_in_file(input_file)"""
    path_to_test_file1 = os.path.join(CURRENT_PATH, "example_commands_file.txt")
    path_to_test_file2 = os.path.join(CURRENT_PATH, "example_commands_file2.txt")
    expected = 10
    answer1 = utils.count_lines_in_file(path_to_test_file1)
    answer2 = utils.count_lines_in_file(path_to_test_file1)
    assert answer1 == expected
    assert answer2 == expected

