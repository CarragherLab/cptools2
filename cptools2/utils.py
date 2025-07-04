import os
import collections
import random
import subprocess
import re
from .colours import pretty_print, yellow

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
    # Updated from deprecated .applymap() to pandas 2.0+ compatible approach
    for col in path_cols:
        dataframe[col] = dataframe[col].map(
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
    return filename.replace(" ", "\ ")


def sanitise_paths_in_dataframe(dataframe):
    """
    Apply sanitise_filename to all paths in a dataframe
    
    This is useful for LoadData dataframes that contain file paths
    which might contain spaces or special characters.
    
    Parameters:
    ------------
    dataframe: pandas.DataFrame
        DataFrame containing PathName columns to sanitize
    
    Returns:
    --------
    pandas.DataFrame
        DataFrame with sanitized paths
    """
    path_cols = [col for col in dataframe.columns if col.startswith("PathName")]
    for col in path_cols:
        dataframe[col] = dataframe[col].map(sanitise_filename)
    return dataframe


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


def check_pipeline_version(pipeline_path):
    """
    Compares the major version of the CellProfiler pipeline file with the
    version of CellProfiler installed in the environment.

    Logs a warning if the major versions do not match.

    Parameters:
    -----------
    pipeline_path : string
        Path to the .cppipe pipeline file.
    """
    try:
        # 1. Get the pipeline version from the .cppipe file
        with open(pipeline_path, 'r') as f:
            first_line = f.readline()
        
        # Standard .cppipe files have "Version:X" or "Version: YYYY-MM-DD..."
        # We need to handle both formats.
        match = re.search(r'Version:(\d+)', first_line)
        if not match:
            # New format is a timestamp, e.g., "CellProfiler Pipeline:4"
            match = re.search(r'CellProfiler Pipeline:(\d+)', first_line)

        if not match:
            pretty_print(
                f"Could not determine version from pipeline file: {pipeline_path}. "
                "Skipping version check.",
                colour='yellow'
            )
            return

        pipeline_version = match.group(1)
        pipeline_major_version = str(pipeline_version[0])

        # 2. Get the installed CellProfiler version
        # Set thread limit for version check to avoid issues on login nodes
        env = os.environ.copy()
        env['OPENBLAS_NUM_THREADS'] = '1'
        result = subprocess.run(
            ['cellprofiler', '--version'],
            capture_output=True,
            text=True,
            check=True,
            env=env
        )
        # Expected output is "CellProfiler 4.2.8" or similar
        cp_output = result.stdout.strip()
        
        match = re.search(r'CellProfiler (\d+)', cp_output)
        if not match:
            pretty_print(
                f"Could not parse CellProfiler version from output: '{cp_output}'. "
                "Skipping version check.",
                colour='yellow'
            )
            return
            
        cp_major_version = match.group(1)

        # 3. Compare and warn if necessary
        if pipeline_major_version != cp_major_version:
            warning_msg = (
                f"Warning: Pipeline version ({pipeline_major_version}) does not match "
                f"installed CellProfiler version ({cp_major_version}). "
                f"This may cause errors during analysis."
            )
            pretty_print(warning_msg, colour='yellow')

    except FileNotFoundError:
        pretty_print(f"Pipeline file not found at: {pipeline_path}", colour='red')
    except subprocess.CalledProcessError as e:
        pretty_print(
            "Could not execute 'cellprofiler --version'. "
            "Please ensure CellProfiler is installed and in your PATH.",
            colour='red'
        )
    except Exception as e:
        pretty_print(f"An unexpected error occurred during version check: {e}", colour='red')

