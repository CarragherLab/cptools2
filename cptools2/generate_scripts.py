"""
Generate eddie submission scripts for the
staging, analysis and destaging jobs
"""

from __future__ import print_function
import os
import sys
import textwrap
from datetime import datetime
import shutil

try:
    from scissorhands import script_generator
except ImportError:
    print("Error: scissorhands package not found. Please install it with:")
    print("pip install 'git+https://github.com/carragherlab/scissorhands.git@master#egg=scissorhands'")
    sys.exit(1)

from cptools2 import utils
from cptools2 import colours
from cptools2.colours import pretty_print, green


def make_command_paths(commands_location):
    """
    create the paths to the commands
    """
    names = ["staging", "cp_commands", "destaging"]
    return {name: os.path.join(commands_location, name+".txt") for name in names}


def _lines_in_commands(staging, cp_commands, destaging):
    """
    Number of lines in each of the commands file.
    While the number of lines in each of the files *should* be the same,
    it's worth checking.

    Parameters:
    -----------
    staging: string
        path to staging commands

    cp_commands: string
        path to cellprofiler commands

    destaging: string
        path to destaging commands

    Returns:
    --------
    Dictionary, e.g:
        {staging: 128, cp_commands: 128, destaging: 128}
    """
    names = ["staging", "cp_commands", "destaging"]
    paths = [staging, cp_commands, destaging]
    counts = [utils.count_lines_in_file(i) for i in paths]
    # check if the counts differ
    if len(set(counts)) > 1:
        raise RuntimeWarning("command files contain differing number of lines")
    return {name: count for name, count in zip(names, counts)}


def lines_in_commands(commands_location):
    """
    Given a path to a directory which contains the commands:
        1. staging
        2. cellprofiler commands
        3. destaging
    This will return the number of lines in each of these text files.

    Parameters:
    -----------
    commands_location: string
        path to directory containing commands

    Returns:
    ---------
    Dictionary

        {"staging":     int,
         "cp_commands": int,
         "destaging":   int}
    """
    command_paths = make_command_paths(commands_location)
    return _lines_in_commands(**command_paths)


def load_module_text(is_cellprofiler=False):
    """returns load module commands, optionally activating cellprofiler env"""
    script_text = textwrap.dedent(
        """
        module load anaconda/2024.02
        """
    )
    if is_cellprofiler:
        script_text += textwrap.dedent(
            """
            conda activate cellprofiler
            """
        )
    return script_text


def make_logfile_text(logfile_location, job_file, n_tasks):
    text = """
    # get the exit code from the cellprofiler job
    RETURN_VAL=$?

    if [[ $RETURN_VAL == 0 ]]; then
        RETURN_STATUS="Finished"
    else
        RETURN_STATUS="Failed with error code: $RETURN_VAL"
    fi

    LOG_FILE_LOC={logfile_location}/{job_file}.log
    echo "`date +"%Y-%m-%d %H:%M"`  "$JOB_ID"  "$SGE_TASK_ID"  "$RETURN_STATUS"" >> "$LOG_FILE_LOC"
    """.format(logfile_location=logfile_location,
               job_file=job_file)
    return textwrap.dedent(text)


class SafePathScript(script_generator.SGEScript):
    """
    Overwrites the __add__ method of the script_generator.SGEScript
    class to enable adding new lines which contain paths, without
    them being converted to absolute paths by the script_generator
    """
    def __add__(self, new_line):
        self.script += new_line
        return self


def create_master_submit_script(commands_location, logfile_location, enable_batching=False, batches=None):
    """Enhanced to handle both single and batch workflows with unified script naming."""
    import textwrap
    import os
    from cptools2.colours import pretty_print, green
    from cptools2 import utils, colours
    from datetime import datetime
    batch_count = len(batches) if (enable_batching and batches) else 1
    script_content = textwrap.dedent(f'''
        #!/bin/sh
        #$ -N cptools_batch_master
        #$ -cwd
        echo "[cptools2] Batch Submission Script"
        echo "Created: $(date)"
        echo "Batches: {batch_count}"
        echo "Command files: {commands_location}"
        echo "[cptools2] batch processing ready for implementation"
        ''').strip()
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    script_path = os.path.join(commands_location, f"{timestamp}_SUBMIT_MASTER.sh")
    with open(script_path, 'w') as f:
        f.write(script_content)
    utils.make_executable(script_path)
    pretty_print(colours.green("[cptools2] master script created:"))
    pretty_print(f"\t {colours.yellow(os.path.basename(script_path))}")
    return script_path


def get_available_scratch_space(path):
    """Returns available space in bytes on the drive of the given path."""
    check_path = path
    while not os.path.exists(check_path):
        check_path = os.path.dirname(check_path)
        if not check_path or check_path == os.path.dirname(check_path):
            # we've reached the root and it doesn't exist, something is very wrong
            raise FileNotFoundError(f"Could not find existing path from {path} to check disk space.")
    total, used, free = shutil.disk_usage(check_path)
    return free


def make_join_files_script(config, commands_location, logfile_location, job_hex, time_now, dependency_job_name=None, plates_to_join=None):
    """
    Create a qsub submission script for joining result files based on patterns.
    """
    if not hasattr(config, 'join_files_patterns') or not config.join_files_patterns:
        return None

    patterns = config.join_files_patterns
    location = config.create_command_args["location"]

    patterns_str = ", ".join([colours.yellow(p) for p in patterns])
    
    # Base command
    join_command = f'cptools2 join --location "{location}" --patterns {" ".join(patterns)}'
    
    # Add specific plates if provided
    if plates_to_join:
        pretty_print(f"  -> Generating join script for plates: {', '.join(plates_to_join)}")
        join_command += f" --plates {' '.join(plates_to_join)}"
    else:
        pretty_print(f"  -> Generating join script for all plates with patterns: {patterns_str}")

    join_script = SafePathScript(
        name=f"join_{job_hex}",
        memory="2G",
        tasks=1,
        output=os.path.join(logfile_location, "join")
    )

    if dependency_job_name:
        join_script += f"#$ -hold_jid {dependency_job_name}\n"

    join_script += ". /etc/profile.d/modules.sh\n"
    join_script += load_module_text(is_cellprofiler=False)
    join_script += "export PATH=\"$HOME/.local/bin:$PATH\"\n"
    join_script += join_command + "\n"

    join_loc = os.path.join(commands_location, f"{time_now}_join_script.sh")
    join_script.save(join_loc)
    utils.make_executable(join_loc)

    return join_loc


def make_datastore_transfer_script(config, commands_location, logfile_location, job_hex, time_now, eddie_source_dir, dependency_job_name=None):
    """
    Create a script to transfer joined data to DataStore.
    """
    datastore_dest = config.data_destination_path
    if not datastore_dest:
        return None

    specific_eddie_source_for_transfer = os.path.join(eddie_source_dir, "joined_files").replace("\\", "/")
    pretty_print(f"  -> Generating datastore transfer script to: {colours.yellow(datastore_dest)}")

    transfer_script = SafePathScript(
        name=f"transfer_{job_hex}",
        memory="1G",
        tasks=1,
        output=os.path.join(logfile_location, "transfer")
    )
    
    transfer_script += "#$ -q staging\n"
    if dependency_job_name:
        transfer_script += f"#$ -hold_jid {dependency_job_name}\n"
    
    transfer_script += "#$ -m e\n"
    transfer_script += "#$ -M $USER@ed.ac.uk\n"
    
    transfer_script += f"EDDIE_SOURCE_DIR=\"{specific_eddie_source_for_transfer}\"\n"
    transfer_script += f"DATASTORE_DEST_DIR=\"{datastore_dest}\"\n"
    
    rsync_log_file = os.path.join(logfile_location, f"transfer_to_datastore_{job_hex}.log")
    transfer_script += f"RSYNC_LOG_FILE=\"{rsync_log_file}\"\n"
    
    transfer_script += "echo \"Starting data transfer at $(date)\" > \"$RSYNC_LOG_FILE\"\n"
    transfer_script += "mkdir -p \"$DATASTORE_DEST_DIR\"\n"
    transfer_script += "rsync -av --stats \"$EDDIE_SOURCE_DIR/\" \"$DATASTORE_DEST_DIR/\" >> \"$RSYNC_LOG_FILE\" 2>&1\n"
    
    transfer_loc = os.path.join(commands_location, f"{time_now}_transfer_script.sh")
    transfer_script.save(transfer_loc)
    utils.make_executable(transfer_loc)
    
    return transfer_loc







