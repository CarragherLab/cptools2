import glob
import os

import parserix
from cptools2 import utils


def detect_plate_layout(plate_dir, ext=".tif", clean=True, truncate=True, sanitise=False):
    """
    Detect whether a plate directory follows the old or new ImageXpress layout.

    Old layout (is_new_ix=False): plate_dir/*/*/*/*.tif  (3 levels)
    New layout (is_new_ix=True):  plate_dir/*/*/*/*/*.tif (4 levels)

    Returns
    -------
    (is_new_ix, files) : (bool, list[str])
    """
    if not os.path.isdir(plate_dir):
        raise RuntimeError(
            f"'{plate_dir}' is not a plate directory (not visible/readable from this node). "
            f"If this is on a restricted filesystem, run on a node that can access it or verify with: ls '{plate_dir}'"
        )

    old_glob = plate_dir + "/*/*/*" + ext
    new_glob = plate_dir + "/*/*/*/*" + ext

    old_files = glob.glob(old_glob)
    new_files = glob.glob(new_glob)

    # Prefer the layout that yields files; if both do, pick the larger match set.
    if len(old_files) == 0 and len(new_files) == 0:
        raise RuntimeError(
            f"No '{ext}' files found under '{plate_dir}'. Tried patterns: '{old_glob}' and '{new_glob}'"
        )
    is_new_ix = len(new_files) > len(old_files)
    files = new_files if is_new_ix else old_files

    if clean is True:
        files = parserix.clean.clean(file_list=files, ext=ext)
    if truncate is True:
        index = -5 if is_new_ix else -4
        files = [os.path.join(*i.split(os.sep)[index:]) for i in files]
    else:
        files = [os.path.abspath(f) for f in files]
    if sanitise is True:
        files = [utils.sanitise_filename(f) for f in files]

    if len(files) == 0:
        # Clean step can remove all files if only thumbs/non-images matched
        raise RuntimeError(
            f"No usable image files found in '{plate_dir}' after cleaning. "
            f"Tried patterns: '{old_glob}' and '{new_glob}'"
        )

    return is_new_ix, files


def files_from_plate(plate_dir, ext=".tif", clean=True, truncate=True,
                     sanitise=False, is_new_ix=False):
    """
    return all proper image files from a plate directory

    Parameters:
    -----------
    plate_dir : string
        path to plate directory
    ext : string (default=".tif")
        image extension, only used if clean is True
    clean : Boolean (default=True)
        whether to remove thumbnails and non-image files
    truncate : Boolean (default=True)
        whether to truncate the image path to just plate name onwards
    sanitise: Boolean (default=True)
        whether to escape whitespace in the filepaths
    is_new_ix: Boolean (default=False)
        whether image paths are from the new IX which are parsed differently
    """
    if not os.path.isdir(plate_dir):
        raise RuntimeError(
            f"'{plate_dir}' is not a plate directory (not visible/readable from this node). "
            f"If this is on a restricted filesystem, run on a node that can access it or verify with: ls '{plate_dir}'"
        )
    glob_str = "/*/*/*/*" if is_new_ix else "/*/*/*"
    files = glob.glob(plate_dir + glob_str + ext)
    if clean is True:
        files = parserix.clean.clean(file_list=files, ext=ext)
    if truncate is True:
        # get the last 4 directories including final file
        # sorry
        index = -5 if is_new_ix else -4
        files = [os.path.join(*i.split(os.sep)[index:]) for i in files]
    else:
        files = [os.path.abspath(f) for f in files]
    if sanitise is True:
        files = [utils.sanitise_filename(f) for f in files]
    if len(files) == 0:
        err_msg = "No files found in '{}'".format(plate_dir)
        raise RuntimeError(err_msg)
    return files


def paths_to_plates(experiment_directory):
    """
    Return the absolute file path to all plates contained within
    an ImageXpress experiment directory.

    Parameters:
    -----------
    experiment_directory: string
        Path to top-level experiment in the ImageXpress directory.
        This should contain sub-directories of plates.

    Returns:
    --------
    list of fully-formed paths to the plate directories.
    """
    exp_abs_path = os.path.abspath(experiment_directory)
    # check the experiment directory exists
    if os.path.isdir(exp_abs_path):
        plates = os.listdir(experiment_directory)
        plate_paths = [os.path.join(exp_abs_path, plate) for plate in plates]
        return [path for path in plate_paths if os.path.isdir(path)]
    else:
        err_msg = "'{}' directory not found".format(exp_abs_path)
        raise RuntimeError(err_msg)
