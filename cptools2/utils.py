import os
import collections
import random

def make_dir(directory):
    """sensible way to create directory"""
    try:
        os.makedirs(directory)
    except OSError:
        if os.path.isdir(directory):
            pass
        else:
            err_msg = "failed to create directory {}".format(directory)
            raise RuntimeError(err_msg)


def flatten(list_like):
    """recursively flatten a nested list"""
    for i in list_like:
        if isinstance(i, collections.Iterable) and not isinstance(i, str):
            for sub in flatten(i):
                yield sub
        else:
            yield i


def prefix_filepaths(dataframe, name, location):
    """
    prefix the filepaths in a loaddata dataframe so that the paths point to the
    image location after the images have been staged
    """
    path_cols = [col for col in dataframe.columns if col.startswith("PathName")]
    dataframe[path_cols] = dataframe[path_cols].applymap(
        lambda x: os.path.join(location, "img_data", name, x)
        )
    return dataframe


def any_nan_values(dataframe):
    """Check if 'dataframe' contains any missing values"""
    return dataframe.isnull().any().any()


def count_lines_in_file(input_file):
    """
    count how many lines are in a file, excluding blank lines

    Parameters:
    -----------
    input_file: string
        path to a file

    Returns:
    --------
    integer,
        number of non-empty lines in `input_file`
    """
    total = 0
    with open(input_file) as f:
        for l in f:
            if l != "\n":
                total += 1
    return total


def sanitise_filename(filename):
    """add escape characters to spaces in filenames"""
    return filename.replace(" ", "\ ")