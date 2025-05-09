"""
Generate eddie submission scripts for the
staging, analysis and destaging jobs
"""

from __future__ import print_function
import os
import textwrap
from datetime import datetime
import yaml
import base64
from scissorhands import script_generator
from cptools2 import utils
from cptools2 import colours
from cptools2.colours import pretty_print


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
            source activate cellprofiler
            """
        )
    return script_text


def make_qsub_scripts(config, commands_location, commands_count_dict, logfile_location):
    """
    Create and save qsub submission scripts in the same location as the
    commands.

    Parameters:
    -----------
    config: object
        Configuration object from parse_yaml.parse_config_file.
    commands_location: string
        path to directory that contains staging, cp_commands, and destaging
        command files.

    commands_count_dict: dictionary
        dictionary of the number of commands contain in each of the jobs

    logfile_location: string
        where to store the log files. By default this will store them
        in a directory alongside the results.

    Returns:
    --------
    Nothing, writes files to `commands_location`
    """
    cmd_path = make_command_paths(commands_location)
    time_now = datetime.now().replace(microsecond=0)
    time_now = str(time_now).replace(" ", "-")
    # append random hex to job names - this allows you to run multiple jobs
    # without the -hold_jid flags from clashing
    job_hex = script_generator.generate_random_hex()
    n_tasks = commands_count_dict["cp_commands"]
    # FIXME: using AnalysisScript class for everything, due to the 
    #        {Staging, Destaging}Script class not having loop_through_file
    stage_script = SafePathScript(
        name="staging_{}".format(job_hex),
        memory="1G",
        output=os.path.join(logfile_location, "staging"),
        tasks=commands_count_dict["staging"]
    )
    stage_script += "#$ -q staging\n"
    # limit staging node requests
    stage_script += "#$ -p -500\n"
    stage_script += "#$ -tc 20\n"
    
    # Use the base64-safe method for staging commands
    stage_script.base64_safe_array_loop(phase="staging",
                                        input_file=cmd_path["staging"])
    stage_loc = os.path.join(commands_location,
                             "{}_staging_script.sh".format(time_now))
    stage_script.save(stage_loc)
    analysis_script = script_generator.AnalysisScript(
        name="analysis_{}".format(job_hex),
        tasks=n_tasks,
        hold_jid_ad="staging_{}".format(job_hex),
        pe="sharedmem 1",
        memory="24G",
        output=os.path.join(logfile_location, "analysis")
    )
    analysis_script += load_module_text(is_cellprofiler=True)
    analysis_script.loop_through_file(cmd_path["cp_commands"])
    analysis_loc = os.path.join(commands_location,
                                "{}_analysis_script.sh".format(time_now))
    analysis_script += make_logfile_text(logfile_location,
                                         job_file=job_hex,
                                         n_tasks=n_tasks)
    analysis_script.save(analysis_loc)
    destaging_script = SafePathScript(
        name="destaging_{}".format(job_hex),
        memory="1G",
        hold_jid_ad="analysis_{}".format(job_hex),
        tasks=commands_count_dict["destaging"],
        output=os.path.join(logfile_location, "destaging")
    )
    # Use the base64-safe method for destaging commands
    destaging_script.base64_safe_array_loop(phase="destaging",
                                          input_file=cmd_path["destaging"])
    destage_loc = os.path.join(commands_location,
                               "{}_destaging_script.sh".format(time_now))
    destaging_script.save(destage_loc)

    # Generate join script if patterns are specified in config
    join_script_loc = make_join_files_script(config=config,
                                             commands_location=commands_location,
                                             logfile_location=logfile_location,
                                             job_hex=job_hex,
                                             time_now=time_now)

    # create script to submit staging, analysis and destaging scripts
    transfer_script_loc = None
    if hasattr(config, 'data_destination_path') and config.data_destination_path:
        # Define the source directory for the transfer script
        # This assumes joined results are in a subdirectory named 'joined_results' 
        # under the main 'location' specified in the config.
        if not hasattr(config, 'create_command_args') or "location" not in config.create_command_args:
            pretty_print("Error: 'location' not found in config.create_command_args. Cannot determine source for transfer script.", colour='red')
        else:
            # Assuming 'location' is the base for results, and joined files are in a subfolder
            # We need to construct the full path to the *source* of the joined files on Eddie for rsync
            # The `config.create_command_args["location"]` is the *base* output directory for CP.
            # We assume the `join_files` operation places its output within this directory, potentially in a specific subdirectory.
            # For now, let's assume the `join` operation places files directly in `config.create_command_args["location"]`
            # or that `config.create_command_args["location"]` itself is what needs to be copied after joining.
            # The user's bash script implies a `joined_results` subdirectory.
            # Let's use `config.create_command_args["location"]` as the source and users can make it more specific if needed.
            eddie_source_dir = config.create_command_args["location"] # This is the directory where cellprofiler output and subsequently joined files are expected
            
            transfer_script_loc = make_datastore_transfer_script(config=config,
                                                              commands_location=commands_location,
                                                              logfile_location=logfile_location,
                                                              job_hex=job_hex,
                                                              time_now=time_now,
                                                              eddie_source_dir=eddie_source_dir)

    submit_script_path = make_submit_script(commands_location, time_now, join_script_loc, transfer_script_loc)
    pretty_print("saving master submission script at {}".format(colours.yellow(submit_script_path)))
    utils.make_executable(submit_script_path)


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
               job_file=job_file,
               n_tasks=n_tasks)
    return textwrap.dedent(text)


def make_submit_script(commands_location, job_date, join_script_loc=None, transfer_script_loc=None):
    """
    Create a shell script which will submit the staging, analysis and
    destaging scripts.

    Parameters:
    -----------
    commands_location: string
        path to where the commands are stored
    job_date: string
        date for the submission scripts
    join_script_loc: string or None
        path to the join script, or None if no join script is to be submitted
    transfer_script_loc: string or None
        path to the transfer script, or None if no transfer script is to be submitted

    Returns:
    --------
    path to submit_script
    also writes script to disk in `commands_location`.
    """
    # create full paths to the generated scripts
    names = ["staging", "analysis", "destaging"]
    script_dict = {}
    for name in names:
        script_name = "{}_{}_script.sh".format(job_date, name)
        script_path = os.path.join(commands_location, script_name)
        script_dict[name] = script_path
    # create text for a shell script that qsub's the scripts
    output = """
             #!/bin/sh

             # This script submits the staging, analysis and destaging
             # scripts in the correct order

             # NOTE: run this as a shell script, NOT a submission script
             # so either call `./name_of_script` or `bash name_of_script`

             qsub {staging_script}
             qsub {analysis_script}
             qsub {destaging_script}
            """.format(staging_script=script_dict["staging"],
                       analysis_script=script_dict["analysis"],
                       destaging_script=script_dict["destaging"])

    # Add join script submission if it was created
    if join_script_loc:
        output += "qsub {}\n".format(join_script_loc)
    
    # Add transfer script submission if it was created
    if transfer_script_loc:
        output += "qsub {}\n".format(transfer_script_loc)

    save_location = "{}/{}_SUBMIT_JOBS.sh".format(commands_location, job_date)
    # save this shell script and return it's path
    with open(save_location, "w") as f:
        f.write(textwrap.dedent(output))
    return save_location


def make_join_files_script(config, commands_location, logfile_location, job_hex, time_now):
    """
    Create a qsub submission script for joining result files based on patterns.

    Parameters:
    -----------
    config: object
        Configuration object created by parse_yaml.parse_config_file.
        Expected to have 'join_files_patterns' and 'create_command_args["location"]'.
    commands_location: string
        Path to directory where commands and scripts are stored.
    logfile_location: string
        Path to directory where log files should be stored.
    job_hex: string
        Random hex string to make job names unique.
    time_now: string
        Timestamp string for script naming.

    Returns:
    --------
    string or None
        Path to the generated join script, or None if no patterns were specified.
    """
    # Check if join_files_patterns exists on the config object and has content
    if not hasattr(config, 'join_files_patterns') or not config.join_files_patterns:
        pretty_print("No file joining patterns specified or attribute missing, skipping join script generation.")
        return None

    patterns = config.join_files_patterns
    # Ensure create_command_args and location exist before accessing
    if not hasattr(config, 'create_command_args') or "location" not in config.create_command_args:
        pretty_print("Error: 'location' not found in config.create_command_args. Cannot generate join script.", colour='red')
        return None
    location = config.create_command_args["location"]

    patterns_str = ", ".join([colours.yellow(p) for p in patterns])
    pretty_print(f"Generating join script for patterns: {patterns_str}")

    # Construct the command to run the join operation
    # Assuming a CLI like 'cptools2 join ...' exists. Adjust if needed.
    patterns_arg = " ".join([f'--patterns "{p}"' for p in patterns])
    join_command = f'cptools2 join --location "{location}" {patterns_arg}'

    join_script = script_generator.SGEScript(
        name=f"join_{job_hex}",
        memory="2G",  # Adjust memory as needed
        tasks=1,
        output=os.path.join(logfile_location, "join")
    )
    # Manually add the correct directive to wait for all tasks of the destaging array job
    join_script += f"#$ -hold_jid destaging_{job_hex}\n"

    # Add necessary environment setup (adjust if different modules are needed)
    join_script += ". /etc/profile.d/modules.sh\n"  # Ensure module command is available
    join_script += load_module_text(is_cellprofiler=False) # Only load base module

    # Add ~/.local/bin to PATH
    join_script += "export PATH=\"$HOME/.local/bin:$PATH\"\n"

    # Add the join command
    join_script += join_command + "\n"

    # Add logging similar to analysis script
    # Customize log message if needed
    join_script += textwrap.dedent(f"""
    # get the exit code from the join job
    RETURN_VAL=$?

    if [[ $RETURN_VAL == 0 ]]; then
        RETURN_STATUS="Finished"
    else
        RETURN_STATUS="Failed with error code: $RETURN_VAL"
    fi

    LOG_FILE_LOC={logfile_location}/join_{job_hex}.log
    echo "`date +"%Y-%m-%d %H:%M"`  "$JOB_ID"  "$SGE_TASK_ID"  "$RETURN_STATUS"" >> "$LOG_FILE_LOC"
    """)

    join_loc = os.path.join(commands_location, f"{time_now}_join_script.sh")
    join_script.save(join_loc)
    pretty_print(f"Saving join script at {colours.yellow(join_loc)}")
    utils.make_executable(join_loc) # Make it executable

    return join_loc


def make_datastore_transfer_script(config, commands_location, logfile_location, job_hex, time_now, eddie_source_dir):
    """
    Create a script to transfer joined data to DataStore.
    This script will run on a staging node after the join operation completes.
    
    Parameters:
    -----------
    config: object
        Configuration object containing DataStore paths
    commands_location: string
        Path where scripts are being saved
    logfile_location: string
        Path where logs should be saved
    job_hex: string
        Unique identifier for this job set
    time_now: string
        Timestamp for naming files
    eddie_source_dir: string
        Source directory on Eddie for the rsync operation.
    
    Returns:
    --------
    string
        Path to the created transfer script, or None if not created.
    """
    datastore_dest = config.data_destination_path
    if not datastore_dest:
        pretty_print("No datastore_destination specified in config, skipping transfer script generation.")
        return None

    pretty_print(f"Generating datastore transfer script from {colours.yellow(eddie_source_dir)} to {colours.yellow(datastore_dest)}")

    transfer_script = script_generator.SGEScript(
        name=f"transfer_{job_hex}",
        memory="1G", # As per user's bash script example
        tasks=1,
        output=os.path.join(logfile_location, "transfer") # Log for the transfer job itself
    )
    
    # Set queue and dependency
    transfer_script += "#$ -q staging\n" # Run on staging node
    transfer_script += f"#$ -hold_jid join_{job_hex}\n" # Wait for the join job to complete
    
    # Define paths within the script
    transfer_script += f"EDDIE_SOURCE_DIR=\"{eddie_source_dir}\"\n"
    transfer_script += f"DATASTORE_DEST_DIR=\"{datastore_dest}\"\n"
    
    # Add logging setup (within the script)
    # Ensure logfile_location is accessible from the staging node or use a path on scratch
    # For simplicity, using the provided logfile_location, assuming it's on scratch.
    transfer_script += f"LOG_PARENT_DIR=\"{logfile_location}\"\n" # This is where the transfer.oXXX and transfer.eXXX will go
    transfer_script += "mkdir -p \"$LOG_PARENT_DIR\"\n" # Ensure main log dir exists for .o and .e files
    # Specific log file for rsync output etc.
    transfer_script += f"RSYNC_LOG_FILE=\"$LOG_PARENT_DIR/transfer_to_datastore_{job_hex}_${{JOB_ID}}.log\"\n"
    transfer_script += "echo \"Starting data transfer at $(date)\" > \"$RSYNC_LOG_FILE\"\n"
    transfer_script += "echo \"From: $EDDIE_SOURCE_DIR\" >> \"$RSYNC_LOG_FILE\"\n"
    transfer_script += "echo \"To: $DATASTORE_DEST_DIR\" >> \"$RSYNC_LOG_FILE\"\n"
    
    # Create destination directory (rsync can also do this with -R, but explicit mkdir -p is safer)
    transfer_script += "mkdir -p \"$DATASTORE_DEST_DIR\"\n"
    
    # Transfer data using rsync
    transfer_script += "rsync -av --stats \"$EDDIE_SOURCE_DIR/\" \"$DATASTORE_DEST_DIR/\" >> \"$RSYNC_LOG_FILE\" 2>&1\n"
    
    # Check result and log completion status
    transfer_script += textwrap.dedent(""" \
    # Check transfer result
    RETURN_VAL=$?
    if [[ $RETURN_VAL == 0 ]]; then
        RETURN_STATUS="Transfer completed successfully"
    else
        RETURN_STATUS="Transfer failed with error code: $RETURN_VAL"
    fi
    
    # Log completion
    echo "Transfer finished at $(date): $RETURN_STATUS" >> "$RSYNC_LOG_FILE"
    """)
    
    # Optional email notification - checking for attribute directly on config
    if hasattr(config, 'notification_email') and config.notification_email:
        email_address = config.notification_email
        email_content_file = f"$LOG_PARENT_DIR/email_content_{job_hex}_${{JOB_ID}}.txt"

        # Script to generate and send email
        email_script = textwrap.dedent(f"""

# Determine email content based on transfer status
if [[ \"$RETURN_STATUS\" == \"Transfer completed successfully\" ]]; then
    FILE_COUNT=$(find \"$DATASTORE_DEST_DIR\" -type f | wc -l)
    TOTAL_SIZE=$(du -sh \"$DATASTORE_DEST_DIR\" | cut -f1)
    EMAIL_SUBJECT=\"cptools2 Analysis Completed - Job ${{JOB_ID}} - Success\"
    cat > \"{email_content_file}\" << EOF
Your cptools2 analysis script is complete, you can find your data saved in your project directory ${{DATASTORE_DEST_DIR}}.

Successfully transferred ${{FILE_COUNT}} files with a total size of ${{TOTAL_SIZE}}.

Thank you for using cptools2.

If you have any feedback or noticed any bugs then please let me know.

Best Wishes,

Mungo Harvey (he/him)
Postdoctoral Research Assistant

E: mharvey2@ed.ac.uk

UK Dementia Research Institute at The University of Edinburgh
Centre for Clinical Brain Sciences | Selveraj Lab & Chandran Lab, Chancellor's Building
Institute of Genetics and Cancer  | Carragher Lab, Edinburgh Cancer Research
EOF
else
    EMAIL_SUBJECT=\"cptools2 Analysis Completed - Job ${{JOB_ID}} - Transfer Failed\"
    cat > \"{email_content_file}\" << EOF
Your cptools2 analysis script completed, but the data transfer from ${{EDDIE_SOURCE_DIR}} to ${{DATASTORE_DEST_DIR}} failed.

Status: ${{RETURN_STATUS}}

Please check the logs in {logfile_location} for more details (specifically the transfer log: {os.path.join(logfile_location, f'transfer_to_datastore_{job_hex}_${{JOB_ID}}.log')}).

Best Wishes,

Mungo Harvey (he/him)
Postdoctoral Research Assistant

E: mharvey2@ed.ac.uk

UK Dementia Research Institute at The University of Edinburgh
Centre for Clinical Brain Sciences | Selveraj Lab & Chandran Lab, Chancellor's Building
Institute of Genetics and Cancer  | Carragher Lab, Edinburgh Cancer Research
EOF
fi

# Send the email
mail -s \"$EMAIL_SUBJECT\" \"{email_address}\" < \"{email_content_file}\"

# Clean up the email content file
rm -f \"{email_content_file}\"        
""")
        transfer_script += email_script
    
    # Save the script
    transfer_loc = os.path.join(commands_location, f"{time_now}_transfer_script.sh")
    transfer_script.save(transfer_loc)
    pretty_print(f"Saving datastore transfer script at {colours.yellow(transfer_loc)}")
    utils.make_executable(transfer_loc)
    
    return transfer_loc


class SafePathScript(script_generator.AnalysisScript):
    """
    This class provides methods to handle rsync commands with filepaths containing spaces
    or other special characters when running within Grid Engine array jobs.
    
    It inherits from scissorhands.script_generator.AnalysisScript and adds
    methods that properly handle special characters in command execution.
    """

    def __init__(self, *args, **kwargs):
        script_generator.AnalysisScript.__init__(self, *args, **kwargs)

    def base64_safe_array_loop(self, phase, input_file):
        """
        Uses base64 encoding/decoding to preserve all special characters in commands.
        
        This method assumes the commands in input_file have been base64 encoded.
        See commands.py's write_commands() function for the encoding step.
        
        Parameters:
        -----------
        phase: string
            prefix of the hidden commands file, e.g "staging" or "destaging"
        input_file: string
            path to a file containing base64-encoded commands
        
        Returns:
        ---------
        nothing, adds text to template
        """
        text = textwrap.dedent(
            """
            SEEDFILE="{input_file}"
            ENCODED_SEED=$(awk "NR==$SGE_TASK_ID" "$SEEDFILE")
            # Decode the base64 command
            SEED=$(echo "$ENCODED_SEED" | base64 --decode)
            # Execute the decoded command directly
            eval "$SEED"
            # Log the command for debugging
            echo "[{phase}] Executed command for task $SGE_TASK_ID" >&2
            """.format(phase=phase, input_file=input_file)
        )
        self.template += text


