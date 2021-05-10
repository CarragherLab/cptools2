import os
import collections
import random

def make_dir(directory):
    """
    sensible way to create directory

    Parameters:
    ------------
    directory: string
        path to the directory to be created

    Returns:
    --------
    nothing, creates empty directory if successful, otherwise raises
    a RuntimeError
    """
    try:
        os.makedirs(directory)
    except OSError:
        if os.path.isdir(directory):
            pass
        else:
            err_msg = "failed to create directory {}".format(directory)
            raise RuntimeError(err_msg)


def flatten(list_like):
    """
    recursively flatten a nested list

    Parameters:
    -----------
    list_like: list
        nested list to flatten

    Returns:
    --------
    generator for an un-nested list
    """
    for i in list_like:
        if isinstance(i, collections.abc.Iterable) and not isinstance(i, str):
            for sub in flatten(i):
                yield sub
        else:
            yield i


def prefix_filepaths(dataframe, name, location):
    """
    prefix the filepaths in a loaddata dataframe so that the paths point to the
    image location after the images have been staged

    Parameters:
    -----------
    dataframe: pandas.DataFrame
        a loaddata dataframe
    name: string
        name of individual job
    location: string
        path prefix to where the images will be stored after staging

    Returns:
    --------
    pandas.DataFrame with altered `PathName_` columns
    """
    path_cols = [col for col in dataframe.columns if col.startswith("PathName")]
    dataframe[path_cols] = dataframe[path_cols].applymap(
        lambda x: os.path.join(location, "img_data", name, x)
        )
    return dataframe


def any_nan_values(dataframe):
    """
    Check if 'dataframe' contains any missing values

    Parameters:
    -----------
    dataframe: pandas.DataFrame

    Returns:
    --------
    Boolean
    """
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
    """
    add escape characters to spaces in filenames

    Parameters:
    ------------
    filename: string

    Returns:
    --------
    string
    """
    return filename.replace(" ", "\ ")


def on_staging_node():
    """
    Determine if this is being run on a staging node or not.
    Checks whether it can access IGMM's datastore

    Returns:
    ---------
    Boolean
    """
    try:
        _ = os.listdir("/exports/igmm/datastore")
        return True
    except OSError:
        return False


def make_executable(filepath):
    """chmod +x a file"""
    st = os.stat(filepath)
    os.chmod(filepath, st.st_mode | 0o111)

