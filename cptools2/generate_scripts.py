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
import shutil
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


def plan_plate_batches(job_object, scratch_path, logfile_location):
    """
    Analyze plates and create batching plan
    This runs before any script generation
    """
    # Calculate space for all plates
    plate_sizes = job_object.calculate_plate_sizes()
    
    # Get current scratch space availability
    available_space = get_available_scratch_space(scratch_path)
    
    # Create plate batches
    plate_batches = job_object.create_plate_batches(available_space)
    
    # Log the batching plan
    log_content = log_plate_batching_plan(plate_sizes, plate_batches, available_space)
    log_file_path = os.path.join(logfile_location, "plate_batching_plan.log")
    with open(log_file_path, "w") as f:
        f.write("\n".join(log_content))
    
    return {
        'plate_batches': plate_batches,
        'total_plates': len(plate_sizes),
        'available_space_gb': available_space / (1024**3),
        'batch_count': len(plate_batches)
    }

def log_plate_batching_plan(plate_sizes, plate_batches, available_space):
    """
    Log plate batching decisions for audit and debugging
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Create detailed log of batching decisions
    log_content = []
    log_content.append(f"Plate Batching Plan - {timestamp}")
    log_content.append(f"Available scratch space: {available_space / (1024**3):.2f} GB")
    log_content.append(f"Safety allocation per batch (50%): {(available_space * 0.5) / (1024**3):.2f} GB")
    log_content.append(f"Processing overhead factor: 1.5x")
    log_content.append("")
    
    log_content.append("Individual Plate Sizes:")
    for plate_name, size in sorted(plate_sizes.items(), key=lambda x: x[1], reverse=True):
        log_content.append(f"  {plate_name}: {size / (1024**3):.2f} GB")
    log_content.append("")
    
    log_content.append("Plate Batch Configuration:")
    for batch in plate_batches:
        log_content.append(f"  Batch {batch['batch_id']}: {batch['plate_count']} plates, {batch['total_size_gb']:.2f} GB")
        for plate in batch['plates']:
            plate_size_gb = plate_sizes[plate] / (1024**3)
            log_content.append(f"    - {plate}: {plate_size_gb:.2f} GB")
        log_content.append("")
    
    return log_content

def generate_batch_workflows(config, job_object, batch_plan, commands_location, logfile_location):
    """
    Generate complete workflows for each plate batch
    Each batch gets its own commands and scripts
    """
    batch_workflows = []
    
    for batch_info in batch_plan['plate_batches']:
        batch_id = batch_info['batch_id']
        
        # Create Job object for this batch only
        batch_job = job_object.create_batch_job(batch_id)
        
        # Create batch-specific directories
        batch_commands_location = os.path.join(commands_location, f"batch_{batch_id}")
        os.makedirs(batch_commands_location, exist_ok=True)
        
        # Generate commands for this batch (using existing logic)
        batch_job.create_commands(
            pipeline=config.create_command_args['pipeline'],
            location=config.create_command_args['location'],
            commands_location=batch_commands_location,
            job_size=config.chunk_args.get('job_size', 96) if config.chunk_args else 96
        )
        
        # Generate scripts for this batch (using existing logic)
        batch_commands_count = lines_in_commands(batch_commands_location)
        
        batch_workflow = {
            'batch_id': batch_id,
            'plates': batch_info['plates'],
            'commands_location': batch_commands_location,
            'commands_count': batch_commands_count,
            'job_object': batch_job
        }
        
        batch_workflows.append(batch_workflow)
    
    return batch_workflows


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
    # email_notification_script_loc: string or None
    #     path to the email notification script, or None if no email script is to be submitted

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

    # Add email notification script submission if it was created
    # if email_notification_script_loc:
    #     output += "qsub {}\n".format(email_notification_script_loc)

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

    # Construct the specific source directory for rsync
    # eddie_source_dir is config.create_command_args["location"]
    specific_eddie_source_for_transfer = os.path.join(eddie_source_dir, "joined_files").replace("\\", "/")

    pretty_print(f"Generating datastore transfer script from {colours.yellow(specific_eddie_source_for_transfer)} to {colours.yellow(datastore_dest)}")

    transfer_script = script_generator.SGEScript(
        name=f"transfer_{job_hex}",
        memory="1G", # As per user's bash script example
        tasks=1,
        output=os.path.join(logfile_location, "transfer") # Log for the transfer job itself
    )
    
    # Set queue and dependency
    transfer_script += "#$ -q staging\n" # Run on staging node
    transfer_script += f"#$ -hold_jid join_{job_hex}\n" # Wait for the join job to complete
    
    # Add SGE email notification directives
    transfer_script += "#$ -m e\n"  # Email on job end
    transfer_script += "#$ -M $USER@ed.ac.uk\n" # Email to user's uni address

    # Define paths within the script
    transfer_script += f"EDDIE_SOURCE_DIR=\"{specific_eddie_source_for_transfer}\"\n" # Use the more specific source path
    transfer_script += f"DATASTORE_DEST_DIR=\"{datastore_dest}\"\n"
    
    # Add logging setup (within the script)
    # Ensure logfile_location is accessible from the staging node or use a path on scratch
    # For simplicity, using the provided logfile_location, assuming it's on scratch.
    transfer_script += f"LOG_PARENT_DIR=\"{logfile_location}\"\n" # This is where the transfer.oXXX and transfer.eXXX will go
    transfer_script += "mkdir -p \"$LOG_PARENT_DIR\"\n" # Ensure main log dir exists for .o and .e files
    # Specific log file for rsync output etc.
    transfer_script += f"RSYNC_LOG_FILE=\"$LOG_PARENT_DIR/transfer_to_datastore_{job_hex}.log\"\n" # Standardized log name
    transfer_script += "echo \"Starting data transfer at $(date)\" > \"$RSYNC_LOG_FILE\"\n"
    transfer_script += "echo \"From: $EDDIE_SOURCE_DIR\" >> \"$RSYNC_LOG_FILE\"\n"
    
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
        full_script_addition = ""

        if phase == "staging":
            # New dynamic scratch space management for staging phase
            dynamic_space_management = textwrap.dedent(
                '''
                # --- BEGIN DYNAMIC SCRATCH SPACE MANAGEMENT for {phase} ---
                # Core configuration
                SCRATCH_DIR="/exports/eddie/scratch/$USER"
                
                # Get project directory from the commands location or use a default subdirectory
                # This ensures control files are scoped to the specific project
                if [[ -n "{input_file}" ]]; then
                    PROJECT_DIR=$(dirname "{input_file}")
                    PROJECT_NAME=$(basename "$PROJECT_DIR")
                else
                    PROJECT_NAME="cptools_project"
                    PROJECT_DIR="$SCRATCH_DIR/$PROJECT_NAME"
                fi
                
                CONTROL_DIR="$PROJECT_DIR/.{phase}_control"
                mkdir -p "$CONTROL_DIR" 2>/dev/null || true
                
                echo "[{phase}] $(date +'%Y-%m-%d %H:%M:%S') INFO: Task $SGE_TASK_ID: Using control directory: $CONTROL_DIR" >&2
                
                # Dynamic scratch space management function
                wait_for_safe_space() {{
                    local usage
                    local total_active_tasks
                    local new_tasks_allowed
                    local current_new_tasks
                    
                    while true; do
                        # Get disk usage percentage
                        usage=$(df --output=pcent "$SCRATCH_DIR" 2>/dev/null | tail -1 | sed 's/%//' | tr -d ' ')

                        # Count all active tasks (no time limit - some analyses run for many hours)
                        total_active_tasks=$(find "$CONTROL_DIR" -name "active_*" 2>/dev/null | wc -l)
                        
                        # Conservative exponential scale-down for NEW tasks based on space usage
                        # Much more conservative limits with finer granularity to prevent overflow
                        if [[ $usage -lt 50 ]]; then
                            new_tasks_allowed=200     # Conservative peak when plenty of space
                        elif [[ $usage -lt 60 ]]; then
                            new_tasks_allowed=50      # Start throttling earlier
                        elif [[ $usage -lt 70 ]]; then
                            new_tasks_allowed=20      # Moderate throttling
                        elif [[ $usage -lt 75 ]]; then
                            new_tasks_allowed=5       # Aggressive throttling
                        elif [[ $usage -lt 80 ]]; then
                            new_tasks_allowed=2       # Very conservative
                        elif [[ $usage -lt 85 ]]; then
                            new_tasks_allowed=1       # Only 1 at a time
                        else
                            new_tasks_allowed=0       # Stop completely at 85%
                        fi
                        
                        # Additional safety check: monitor available space trend
                        SPACE_LOG="$CONTROL_DIR/space_check.log"
                        available_gb=$(df -BG "$SCRATCH_DIR" 2>/dev/null | awk 'NR==2 {{print $4}}' | sed 's/G//')
                        current_time=$(date +%s)
                        echo "$current_time $available_gb" >> "$SPACE_LOG" 2>/dev/null || true
                        
                        # Keep only last 10 measurements and check consumption rate
                        if [[ -f "$SPACE_LOG" ]]; then
                            tail -10 "$SPACE_LOG" > "$SPACE_LOG.tmp" 2>/dev/null && mv "$SPACE_LOG.tmp" "$SPACE_LOG" 2>/dev/null || true
                            
                            # If we have enough data points, check consumption rate
                            line_count=$(wc -l < "$SPACE_LOG" 2>/dev/null || echo "0")
                            if [[ $line_count -ge 5 ]]; then
                                # Check if space is dropping rapidly (more than 2GB in 5 minutes)
                                space_change=$(tail -5 "$SPACE_LOG" | awk 'NR==1{{first=$2}} NR==5{{last=$2}} END{{print first-last}}' 2>/dev/null || echo "0")
                                if [[ $(echo "$space_change > 2" | bc -l 2>/dev/null || echo "0") -eq 1 ]]; then
                                    # Rapid consumption detected - reduce limits by half
                                    new_tasks_allowed=$((new_tasks_allowed / 2))
                                    echo "[{phase}] $(date +'%Y-%m-%d %H:%M:%S') WARNING: Task $SGE_TASK_ID: Rapid space consumption detected (${{space_change}}GB in 5min), reducing new task limit to $new_tasks_allowed" >&2
                                fi
                            fi
                        fi
                        
                        # Count tasks that started recently (last 15 minutes) as "new tasks"
                        # This gives us a proxy for recent task additions vs long-running tasks
                        current_new_tasks=$(find "$CONTROL_DIR" -name "active_*" -mmin -15 2>/dev/null | wc -l)
                        
                        # Check if we can proceed with adding this new task
                        if [[ $current_new_tasks -lt $new_tasks_allowed ]]; then
                            echo "[{phase}] $(date +'%Y-%m-%d %H:%M:%S') INFO: Task $SGE_TASK_ID: Space check passed (usage: ${{usage}}%, total_active: $total_active_tasks, recent_new: $current_new_tasks/$new_tasks_allowed)" >&2
                            break
                        fi
                        
                        echo "[{phase}] $(date +'%Y-%m-%d %H:%M:%S') INFO: Task $SGE_TASK_ID: Waiting for safe space (usage: ${{usage}}%, total_active: $total_active_tasks, recent_new: $current_new_tasks/$new_tasks_allowed allowed)" >&2
                        
                        # Random delay to prevent thundering herd (30-60 seconds)
                        sleep_time=$((30 + RANDOM % 30))
                        sleep "$sleep_time"
                    done
                }}
                
                # Mark task as active
                mark_active() {{
                    touch "$CONTROL_DIR/active_$SGE_TASK_ID"
                    echo "[{phase}] $(date +'%Y-%m-%d %H:%M:%S') INFO: Task $SGE_TASK_ID: Marked as active" >&2
                }}
                
                # Cleanup function
                cleanup_control() {{
                    rm -f "$CONTROL_DIR/active_$SGE_TASK_ID" 2>/dev/null || true
                    echo "[{phase}] $(date +'%Y-%m-%d %H:%M:%S') INFO: Task $SGE_TASK_ID: Cleaned up control file" >&2
                }}
                
                # Set up cleanup trap
                trap cleanup_control EXIT
                
                # Execute space management
                wait_for_safe_space
                mark_active
                
                echo "[{phase}] $(date +'%Y-%m-%d %H:%M:%S') INFO: Task $SGE_TASK_ID: Proceeding to execute {phase} command (after dynamic space check)" >&2
                # --- END DYNAMIC SCRATCH SPACE MANAGEMENT ---
                '''
            ).format(phase=phase, input_file=input_file)
            
            full_script_addition += dynamic_space_management

        # Always include the command execution logic
        # These main command execution logs go to stderr (SGE default error file)
        command_execution_logic = textwrap.dedent(
            '''
            SEEDFILE="{input_file}"
            ENCODED_SEED=$(awk "NR==$SGE_TASK_ID" "$SEEDFILE")
            # Decode the base64 command
            SEED=$(echo "$ENCODED_SEED" | base64 --decode)
            # Execute the decoded command directly
            eval "$SEED"
            # Log the command completion for debugging
            echo "[{phase}] $(date +\'%Y-%m-%d %H:%M:%S\') INFO: Task $SGE_TASK_ID: Completed executing {phase} command." >&2
            '''
        ).format(phase=phase, input_file=input_file)
        full_script_addition += command_execution_logic

        self.template += full_script_addition


def generate_batch_scripts(workflow, config, logfile_location, job_hex):
    """
    Generate SGE scripts for a single plate batch
    Uses existing script generation logic but for subset of plates
    """
    batch_id = workflow['batch_id']
    commands_location = workflow['commands_location']
    commands_count = workflow['commands_count']
    
    # Use existing make_qsub_scripts logic but with batch-specific parameters
    time_now = datetime.now().replace(microsecond=0)
    time_now = str(time_now).replace(" ", "-")
    
    # Create batch-specific job names
    staging_job_name = f"staging_batch_{batch_id}_{job_hex}"
    analysis_job_name = f"analysis_batch_{batch_id}_{job_hex}"
    destaging_job_name = f"destaging_batch_{batch_id}_{job_hex}"
    
    # Set dependencies (batch 2+ waits for previous batch destaging)
    if batch_id > 1:
        staging_dependency = f"destaging_batch_{batch_id-1}_{job_hex}"
    else:
        staging_dependency = None
    
    # Generate scripts using existing patterns
    cmd_path = make_command_paths(commands_location)
    
    # Create staging script
    staging_script = SafePathScript(
        name=staging_job_name,
        memory="1G",
        output=os.path.join(logfile_location, "staging"),
        tasks=commands_count["staging"]
    )
    staging_script += "#$ -q staging\n"
    staging_script += "#$ -p -500\n"
    staging_script += "#$ -tc 20\n"
    
    if staging_dependency:
        staging_script += f"#$ -hold_jid {staging_dependency}\n"
    
    # Use existing base64_safe_array_loop (no changes needed)
    staging_script.base64_safe_array_loop(
        phase="staging",
        input_file=cmd_path["staging"]
    )
    
    staging_loc = os.path.join(commands_location, f"{time_now}_staging_script.sh")
    staging_script.save(staging_loc)

    # Create analysis script
    analysis_script = script_generator.AnalysisScript(
        name=analysis_job_name,
        tasks=commands_count["cp_commands"],
        hold_jid_ad=staging_job_name,
        pe="sharedmem 1",
        memory="24G",
        output=os.path.join(logfile_location, "analysis")
    )
    analysis_script += load_module_text(is_cellprofiler=True)
    analysis_script.loop_through_file(cmd_path["cp_commands"])
    analysis_script += make_logfile_text(logfile_location,
                                         job_file=f"batch_{batch_id}_{job_hex}",
                                         n_tasks=commands_count["cp_commands"])
    analysis_loc = os.path.join(commands_location, f"{time_now}_analysis_script.sh")
    analysis_script.save(analysis_loc)
    
    # Create destaging script
    destaging_script = SafePathScript(
        name=destaging_job_name,
        memory="1G",
        hold_jid_ad=analysis_job_name,
        tasks=commands_count["destaging"],
        output=os.path.join(logfile_location, "destaging")
    )
    destaging_script.base64_safe_array_loop(phase="destaging",
                                          input_file=cmd_path["destaging"])
    destage_loc = os.path.join(commands_location, f"{time_now}_destaging_script.sh")
    destaging_script.save(destage_loc)
    
    return {
        'batch_id': batch_id,
        'plates': workflow['plates'],
        'staging_script': staging_loc,
        'analysis_script': analysis_loc,
        'destaging_script': destage_loc,
        'job_hex': job_hex
    }

def create_sequential_submission_script(batch_scripts, commands_location):
    """
    Create master submission script that submits all batches
    Dependencies are already built into the individual scripts
    """
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    script_content = []
    
    script_content.append("#!/bin/sh")
    script_content.append("# Sequential plate batch submission script")
    script_content.append(f"# Generated: {timestamp}")
    script_content.append("# Batches run sequentially to prevent scratch space conflicts")
    script_content.append("")
    
    for batch_info in batch_scripts:
        batch_id = batch_info['batch_id']
        plates = ", ".join(batch_info['plates'])
        
        script_content.append(f"# Batch {batch_id}: {plates}")
        script_content.append(f"echo 'Submitting batch {batch_id} ({len(batch_info['plates'])} plates)...'")
        script_content.append(f"qsub {batch_info['staging_script']}")
        script_content.append(f"qsub {batch_info['analysis_script']}")
        script_content.append(f"qsub {batch_info['destaging_script']}")
        script_content.append("")
    
    script_content.append(f"echo 'Submitted {len(batch_scripts)} plate batches for sequential execution'")
    
    # Save submission script
    submit_script_path = os.path.join(commands_location, f"{timestamp}_SUBMIT_PLATE_BATCHES.sh")
    with open(submit_script_path, 'w') as f:
        f.write('\\n'.join(script_content))
    
    utils.make_executable(submit_script_path)
    return submit_script_path


