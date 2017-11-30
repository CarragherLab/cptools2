import os
from cptools2 import utils

def make_cp_cmnd(name, pipeline, location, output_loc):
    """create cellprofiler command"""
    loaddata_name = os.path.join(location, "loaddata", name)
    cmnd = cp_command(pipeline=pipeline,
                      load_data=loaddata_name + ".csv",
                      output_location=output_loc)
    return cmnd


def write_loaddata(name, location, dataframe, fix_paths=True):
    """write a loaddata csv file to disk"""
    loaddata_name = os.path.join(location, "loaddata", name + ".csv")
    if fix_paths is True:
        dataframe = utils.prefix_filepaths(dataframe, name, location)
    dataframe.to_csv(loaddata_name, index=False)


def write_filelist(img_list, filelist_name):
    """write a filelist to disk"""
    with open(filelist_name, "w") as f:
        for line in img_list:
            f.write(line + "\n")


def make_rsync_cmnd(plate_loc, filelist_name, img_location):
    """create an rsync command to copy contents of a file list"""
    cmnd = rsync_string(filelist=filelist_name,
                        source=plate_loc,
                        destination=img_location)
    return cmnd


def _write_single(commands_location, commands, final_name):
    """write commands for single command list"""
    cmnd_loc = os.path.join(commands_location, final_name + ".txt")
    with open(cmnd_loc, "w") as outfile:
        for line in commands:
            outfile.write(line + "\n")


def write_commands(commands_location, rsync_commands, cp_commands, rm_commands):
    """writes all commands, for stage, cp and destage commands"""
    commands = [rsync_commands, cp_commands, rm_commands]
    names = ["staging", "cp_commands", "destaging"]
    for command, name in zip(commands, names):
        _write_single(commands_location, command, name)


def rsync_string(filelist, source, destination):
    """
    Create rsync string pointing to a file-list and a destination
    If the file-list is truncated, then source has to be the location of the
    file-list so it forms a complete path.
    Destination will include the entire path located in the file-list, therefore
    truncation is recommended,
    Desination can also begin with a directory that has not yet been created,
    the directory will be created by the rsync command.
    Escape characters will be added to spaces in filenames.
    """
    return "rsync --files-from={filelist} {source} {destination}".format(
        filelist=utils.sanitise_filename(filelist),
        source=utils.sanitise_filename(source),
        destination=utils.sanitise_filename(destination)
        )


def rm_string(directory):
    """
    create string to remove job's data after successful run
    NOTE DANGER ZONE!!!
    """
    return "rm -rf {}".format(directory)


def cp_command(pipeline, load_data, output_location):
    """create cellprofiler command"""
    cmnd = "cellprofiler -r -c -p {} --data-file={} -o {}".format(
        pipeline, load_data, output_location)
    return cmnd


def make_output_directories(location):
    """create the directories to store the output, used in job.Job()"""
    for direc in ["loaddata", "img_data", "filelist", "raw_data"]:
        utils.make_dir(os.path.join(location, direc))

