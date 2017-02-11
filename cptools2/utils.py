import os
import collections

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


def flatten(l):
    """recursively flatten a nested list"""
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, str):
            for sub in flatten(el):
                yield sub
        else:
            yield el


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

