import os
import pandas as pd
from cptools2 import pre_stage
from cptools2 import create_filelist

CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "example_dir")
TEST_PATH_PLATE_1 = os.path.join(TEST_PATH, "test-plate-1")
IMG_LIST = create_filelist.files_from_plate(TEST_PATH_PLATE_1)


def test_create_long_loaddata():
    """cptool2.pre_stage.create_long_loaddata(img_list)"""
    long_df = pre_stage.create_long_loaddata(IMG_LIST)
    assert isinstance(long_df, pd.DataFrame)
    assert long_df.shape == (len(IMG_LIST), 7)

def test_cast_dataframe():
    """cptools2.pre_stage.cast_dataframe(dataframe)"""
    long_df = pre_stage.create_long_loaddata(IMG_LIST)
    wide_df = pre_stage.cast_dataframe(long_df)
    assert isinstance(wide_df, pd.DataFrame)
    # check we have a row per imageset
    expected_rows = 60 * 6
    assert wide_df.shape[0] == expected_rows


def test_create_loaddata():
    """cptools2.pre_stage.create_loaddata(img_list)"""
    output = pre_stage.create_loaddata(IMG_LIST)
    # make our own
    long_df = pre_stage.create_long_loaddata(IMG_LIST)
    wide_df = pre_stage.cast_dataframe(long_df)
    assert output.equals(wide_df)
