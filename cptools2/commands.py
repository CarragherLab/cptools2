import os
import base64

from cptools2 import utils


def make_cp_cmnd(name, pipeline, location, output_loc):
    """
    create cellprofiler command

    Parameters:
    -----------
    name: string
        name of the individual job
    pipeline: string
        filepath to the cellprofiler pipeline
    location: string
        filepath to the directory which contains the loaddata csv files
    output_loc: string
        where to store the results from the cellprofiler job

    Returns:
    --------
    string: a cellprofiler command
    """
    loaddata_name = os.path.join(location, "loaddata", name)
    cmnd = cp_command(pipeline=pipeline,
                      load_data=loaddata_name + ".csv",
                      output_location=output_loc)
    return cmnd


def write_loaddata(name, location, dataframe, fix_paths=True):
    """
    write a loaddata csv file to disk

    Parameters:
    -----------
    name: string
        name of the individual job
    location: string
        filepath to the directory which contains the loaddata csv files
    dataframe: pandas.DataFrame
        a dataframe suitable for Cellprofiler's LoadData module
    fix_paths: Boolean (default = True)
        whether to prefix the filepaths in a loaddata dataframe so that
        the paths point to the image location after the images have been staged

    Returns:
    --------
    nothing, writes the csv file to disk
    """
    loaddata_name = os.path.join(location, "loaddata", name + ".csv")
    if fix_paths is True:
        dataframe = utils.prefix_filepaths(dataframe, name, location)
    dataframe.to_csv(loaddata_name, index=False)


def write_filelist(img_list, filelist_name):
    """
    write a filelist to disk

    Parameters:
    -----------
    img_list: list
        list of images
    filelist_name: string
        what to call the saved filelist

    Returns:
    --------
    nothing, writes filelist to disk
    """
    with open(filelist_name, "w") as f:
        for line in img_list:
            f.write(line + "\n")


def _write_single(commands_location, commands, final_name):
    """
    write commands for single command list

    Parameters:
    -----------
    commands_location: string
        directory where to save the commands
    commands:
        list of commands to write to disk
    final_name:
        filename, what to call the commands file. This is joined to
        commands_location, e.g: `commands_location`/`file_name`.txt

    Returns:
    --------
    nothing, writes commands to disk
    """
    cmnd_loc = os.path.join(commands_location, final_name + ".txt")
    with open(cmnd_loc, "w") as outfile:
        for line in commands:
            outfile.write(line + "\n")


def _write_single_base64(commands_location, commands, final_name):
    """
    write commands as base64-encoded strings
    
    This helps preserve special characters (like spaces in file paths) that
    might otherwise be misinterpreted when executed in Grid Engine array jobs.
    
    Parameters:
    -----------
    commands_location: string
        directory where to save the commands
    commands: list
        list of commands to write to disk, each will be base64 encoded
    final_name: string
        filename, what to call the commands file
        
    Returns:
    --------
    nothing, writes base64-encoded commands to disk
    """
    cmnd_loc = os.path.join(commands_location, final_name + ".txt")
    with open(cmnd_loc, "w") as outfile:
        for line in commands:
            # Encode each command to bypass shell interpretation issues
            encoded_line = base64.b64encode(line.encode()).decode()
            outfile.write(encoded_line + "\n")


def write_commands(commands_location, rsync_commands, cp_commands, rm_commands,
                  staging_file=None, cp_commands_file=None, destaging_file=None):
    """Enhanced to support custom filenames for batch processing."""
    import os
    # Use custom filenames if provided, otherwise use defaults
    if staging_file and cp_commands_file and destaging_file:
        names = [
            os.path.splitext(os.path.basename(staging_file))[0],
            os.path.splitext(os.path.basename(cp_commands_file))[0],
            os.path.splitext(os.path.basename(destaging_file))[0]
        ]
    else:
        names = ["staging", "cp_commands", "destaging"]
    commands_list = [rsync_commands, cp_commands, rm_commands]
    for command, name in zip(commands_list, names):
        # Always encode commands to be safe, especially with file paths
        _write_single_base64(commands_location, command, name)


def make_rsync_cmnd(plate_loc, filelist_name, img_location):
    """
    Create rsync string pointing to a file-list and a destination
    If the file-list is truncated, then source has to be the location of the
    file-list so it forms a complete path.
    Destination will include the entire path located in the file-list, therefore
    truncation is recommended,
    Destination can also begin with a directory that has not yet been created,
    the directory will be created by the rsync command.
    
    This function properly quotes paths to handle spaces in filenames safely.

    Parameters:
    -----------
    plate_loc: string
        source destination of the plates in the ImageXpress directory
    filelist_name: string
        path to the filelist, this will be used with the --files-from flag
    img_location: string
        path to the directory in which to copy the file to in the rsync command

    Returns:
    --------
    string: an rsync command
    """
    # Use more secure permissions appropriate for Eddie
    # Only owner and group get write access, others get read-only
    # Use double quotes to properly handle paths with spaces
    return "rsync -s --perms --chmod=ug+rwx,o+rx --files-from=\"{filelist}\" \"{source}\" \"{destination}\""\
        .format(filelist=filelist_name,
                source=plate_loc,
                destination=img_location)


def rm_string(directory):
    """
    create string to remove job's data after successful run
    NOTE DANGER ZONE!!!

    Parameters:
    -----------
    directory: string
        directory to delete

    Returns:
    --------
    nothing, removes directory on disk
    """
    return "rm -rf \"{}\"".format(directory)


def cp_command(pipeline, load_data, output_location):
    """
    create cellprofiler commands

    Parameters:
    -----------
    pipeline: string
        path to cellprofiler pipeline
    load_data: string
        path to csv file suitable to Cellprofiler's LoadData module
    output_location: string
        where to store the results from the cellprofiler job

    Returns:
    --------
    string: a cellprofiler command
    """
    base_command = "cellprofiler -r -c -p {pipeline} --data-file={load_data} -o {output_location}".format(
        pipeline=pipeline,
        load_data=load_data,
        output_location=output_location)
    
    return base_command


def make_output_directories(location):
    """
    create the directories to store the output, used in job.Job()

    Parameters:
    -----------
    location: string
        path to directory, this is the location in which the images will
        be staged, loaddata csv files stored, filelists and output
        data from the cellprofiler job will be stored.

    Returns:
    --------
    nothing, creates empty directories
    """
    for direc in ["loaddata", "img_data", "filelist", "raw_data", "logfiles"]:
        utils.make_dir(os.path.join(location, direc))
    # then make sub-directories in the logfile directory
    for subdirec in ["staging", "analysis", "destaging"]:
        utils.make_dir(os.path.join(location, "logfiles", subdirec))


def check_commands(location):
    """
    Check commands files are not empty, raise an Error if they are.

    Parameters:
    -----------
    location: string
        location of commands file

    Returns:
    --------
    None if successful, otherwise raises a RuntimeError if the file
    is empty.
    """
    n_lines = utils.count_lines_in_file(location)
    err_msg = "Commands file '{}' is empty, something has gone wrong".format(location)
    if n_lines < 1:
        raise RuntimeError(err_msg)
