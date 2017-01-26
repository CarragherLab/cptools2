import os
import collections
from cptools2 import pre_stage

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


def make_cp_cmnd(plate, job_num, pipeline, location, output_loc):
    name = "{}_{}".format(plate, str(job_num))
    loaddata_name = os.path.join(location, "loaddata", name)
    cmnd =  pre_stage.cp_command(pipeline=pipeline,
                                 load_data=loaddata_name + ".csv",
                                 output_location=output_loc)
    return cmnd


def write_loaddata(name, location, dataframe):
    loaddata_name = os.path.join(location, "loaddata", name + ".csv")
    dataframe.to_csv(loaddata_name, index=False)


def write_filelist(img_list, filelist_name):
    with open(filelist_name, "w") as f:
        for line in img_list:
            f.write(line + "\n")


def make_rsync_cmnd(plate_loc, filelist_name, img_location):
    cmnd = pre_stage.rsync_string(filelist=filelist_name,
                                  source=plate_loc,
                                  destination=img_location)
    return cmnd


def write_cp_commands(commands_location, cp_commands):
    cp_cmnd_loc = os.path.join(commands_location, "cp_commands.txt")
    with open(cp_cmnd_loc, "w") as f:
        for line in cp_commands:
            f.write(line + "\n")


def write_stage_commands(commands_location, rsync_commands):
    rsync_cmnd_loc = os.path.join(commands_location, "staging.txt")
    with open(rsync_cmnd_loc, "w") as f:
        for line in rsync_commands:
            f.write(line + "\n")


def write_destage_commands(commands_location, rm_commands):
    rm_cmnd_loc = os.path.join(commands_location, "destaging.txt")
    with open(rm_cmnd_loc, "w") as f:
        for line in rm_commands:
            f.write(line + "\n")


def make_output_directories(location):
    make_dir(os.path.join(location, "loaddata"))
    make_dir(os.path.join(location, "img_data"))
    make_dir(os.path.join(location, "filelist"))
    make_dir(os.path.join(location, "raw_data"))


def flatten(l):
    """recursively flatten a nested list"""
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
            for sub in flatten(el):
                yield sub
        else:
            yield el