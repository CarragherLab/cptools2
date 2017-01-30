import os
import pandas as pd
from cptools2 import job_splitter
from cptools2 import create_filelist

# need to have an image list
CURRENT_PATH = os.path.dirname(__file__)
TEST_PATH = os.path.join(CURRENT_PATH, "example_dir")
TEST_PATH_PLATE_1 = os.path.join(TEST_PATH, "test-plate-1")
# just use a single plate
IMG_LIST = create_filelist.files_from_plate(TEST_PATH_PLATE_1)

def test_well_site_table():
    """job_splitter._well_site_table(img_list)"""
    output = job_splitter._well_site_table(IMG_LIST)
    assert isinstance(output, pd.DataFrame)
    # check the dataframe is the right size
    # should have a row per image in image list and 3 columns
    assert output.shape == (len(IMG_LIST), 3)
    # need to short as for some unknown reason the order is being mixed up
    # though doesn't matter as always use column names rather than index
    assert sorted(output.columns.tolist()) == sorted(["img_paths",
                                                      "Metadata_well",
                                                      "Metadata_site"])


def test_group_images():
    """job_splitter._group_images(df_img)"""
    # create dataframe for _group_images
    df_img = job_splitter._well_site_table(IMG_LIST)
    output = job_splitter._group_images(df_img)
    assert isinstance(output, list)
    # list should be the number of sites per well by the number of wells
    n_well_sites = 60 * 6
    assert len(output) == n_well_sites
    # check we have 5 channels
    assert len(output[0]) == 5
    # check channels are grouped in the same well/site
    for i in output[0]:
        assert i.startswith("test-plate-1/2015-07-31/4016/val screen_B02_s1")
    for i in output[1]:
        assert i.startswith("test-plate-1/2015-07-31/4016/val screen_B02_s2")


def test_chunks():
    """job_splitter.chunks() on simulated data"""
    # test is works on a stupid dataset
    test_data = list(range(100))
    chunked_test_data = job_splitter.chunks(test_data, job_size=10)
    output = [i for i in chunked_test_data]
    assert len(output) == 10
    for i in output:
        assert len(i) == 10
    # on data with some with some left-over in final bin
    test_data_2 = list(range(109))
    chunk_test_data_2 = job_splitter.chunks(test_data_2, job_size=10)
    output2 = [i for i in chunk_test_data_2]
    assert len(output2) == 11
    for i in output2[:-1]:
        assert len(i) == 10
    assert len(output2[-1]) == 9


def test_split():
    """job_splitter.split()"""
    job_size = 96
    output = job_splitter.split(IMG_LIST)
    assert len(output[0]) == job_size
    for job in output[:-1]:
        assert len(job) == job_size
