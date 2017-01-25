import os

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

