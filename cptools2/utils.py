import os
import collections

import polars as pl


def make_dir(directory):
    """
    sensible way to create directory

    Parameters:
    Cursor - cptools2 - Cursor
    
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
    dataframe: _CompatDataFrame or polars DataFrame
        a loaddata dataframe
    name: string
        name of individual job (e.g., "14202-D-30_0")
    location: string
        path prefix to where the images will be stored after staging

    Returns:
    --------
    dataframe with altered `PathName_` columns
    """
    # Support both _CompatDataFrame wrapper and raw polars DataFrames
    from cptools2.loaddata import _CompatDataFrame

    is_compat = isinstance(dataframe, _CompatDataFrame)
    df = dataframe._df if is_compat else dataframe

    if isinstance(df, pl.DataFrame):
        path_cols = [col for col in df.columns if col.startswith("PathName")]
        prefix = os.path.join(location, "img_data", name).replace('\\', '/')
        for col in path_cols:
            df = df.with_columns(
                (pl.lit(prefix + "/") + pl.col(col)).alias(col)
            )
        if is_compat:
            dataframe._df = df
            return dataframe
        return df
    else:
        # Fallback for pandas DataFrames (used by other modules)
        path_cols = [col for col in dataframe.columns if col.startswith("PathName")]
        for col in path_cols:
            dataframe[col] = dataframe[col].map(
                lambda x: os.path.join(location, "img_data", name, x).replace('\\', '/')
            )
        return dataframe


def any_nan_values(dataframe):
    """
    Check if 'dataframe' contains any missing values

    Parameters:
    -----------
    dataframe: polars DataFrame or _CompatDataFrame

    Returns:
    --------
    Boolean
    """
    from cptools2.loaddata import _CompatDataFrame

    df = dataframe._df if isinstance(dataframe, _CompatDataFrame) else dataframe

    if isinstance(df, pl.DataFrame):
        return df.null_count().row(0) != tuple(0 for _ in df.columns)
    else:
        # Fallback for pandas
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
        for line in f:
            if line != "\n":
                total += 1
    return total


def sanitise_filename(filename):
    """
    Properly handle special characters in filenames, particularly spaces
    
    This function adds backslash escapes to spaces in filenames,
    which is needed for shell command compatibility. Note that this is often
    still insufficient for complex nested command execution in SGE array jobs.
    Consider using the base64 encoding approach for complete reliability.

    Parameters:
    ------------
    filename: string
        Path or filename that may contain spaces or special characters

    Returns:
    --------
    string
        Filename with spaces properly escaped
    """
    # Escape spaces with backslash
    return filename.replace(" ", "\\ ")


# COMMENTED OUT: Function appears unused in current codebase (as of 2025-01-30)
# May be useful for future LoadData processing where base64 encoding is not used
# def sanitise_paths_in_dataframe(dataframe):
#     """
#     Apply sanitise_filename to all paths in a dataframe
#     
#     This is useful for LoadData dataframes that contain file paths
#     which might contain spaces or special characters.
#     
#     Parameters:
#     ------------
#     dataframe: pandas.DataFrame
#         DataFrame containing PathName columns to sanitize
#     
#     Returns:
#     --------
#     pandas.DataFrame
#         DataFrame with sanitized paths
#     """
#     path_cols = [col for col in dataframe.columns if col.startswith("PathName")]
#     for col in path_cols:
#         dataframe[col] = dataframe[col].map(sanitise_filename)
#     return dataframe


# COMMENTED OUT: Function appears unused in current codebase (as of 2025-01-30)
# May be useful for future environment detection or conditional logic based on node type
# def on_staging_node():
#     """
#     Determine if this is being run on a staging node or not.
#     Checks whether it can access IGMM's datastore
# 
#     Returns:
#     ---------
#     Boolean
#     """
#     try:
#         _ = os.listdir("/exports/igmm/datastore")
#         return True
#     except OSError:
#         return False


def make_executable(filepath):
    """chmod +x a file"""
    st = os.stat(filepath)
    os.chmod(filepath, st.st_mode | 0o111)

