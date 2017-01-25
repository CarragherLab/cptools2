import glob
import os
import parserix

def files_from_plate(plate_dir, ext=".tif", clean=True, truncate=True):
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
    """
    files = glob.glob(plate_dir + "/*/*/*" + ext)
    if clean is True:
        files = parserix.clean.clean(files)
    if truncate is True:
        # get the last 4 directories including final file
        # sorry
        return [os.path.join(*i.split(os.sep)[-4:]) for i in files]
    else:
        return [os.path.abspath(f) for f in files]


def paths_to_plates(experiment_directory):
    """
    Return the absolute file path to all plates contained within
    an ImageXpress experiment directory
    """
    exp_abs_path = os.path.abspath(experiment_directory)
    plates = os.listdir(experiment_directory)
    return [os.path.join(exp_abs_path, plate) for plate in plates]


def exclude_plates(plate_list, sub_list, remove=True):
    """
    remove plates by plate-name in a list of plate paths

    Parameters:
    -----------
    plate_list : list of strings
        list of paths to plates produced by paths_to_plates()
    sub_list : list of strings
        list of plate names (not including the full path, just the names)
    remove : Boolean (default=True)
        if True, then sub-list will be removed from the plate_list
        if False, then everything except the sub-list will be removed
    """
    output = []
    if remove is True:
        for path in plate_list:
            if path.split(os.sep)[-1] not in sub_list:
                output.append(path)
    elif remove is False:
        for path in plate_list:
            if path.split(os.sep)[-1] in sub_list:
                output.append(path)
    else:
        raise ValueError("remove requires a Boolean")
    return output
