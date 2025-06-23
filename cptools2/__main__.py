import sys
import os
import argparse
from scissorhands import script_generator
from cptools2 import generate_scripts
from cptools2 import job
from cptools2 import parse_yaml
from cptools2 import utils
from cptools2 import colours
from cptools2 import file_tools
from cptools2.colours import pretty_print


def configure_job(config):
    """
    configure job to generate the commands and scripts

    Parameters:
    ------------
    config: namedtuple
        config namedtuple containing the dictionaries which are
        passed as arguments via **kwargs to the Job class.

    Returns:
    --------
    Job object
    """
    jobber = job.Job(is_new_ix=config.is_new_ix)
    # some of the optional arguments might be none if that option was not present in the
    # configuration file, in which case don't pass them as arguments to the methods
    if config.experiment_args is not None:
        jobber.add_experiment(**config.experiment_args)
    if config.remove_plate_args is not None:
        jobber.remove_plate(**config.remove_plate_args)
    if config.add_plate_args is not None:
        # add_plate_args is now a list of dictionaries
        for plate_args_dict in config.add_plate_args:
             jobber.add_plate(**plate_args_dict) # Call add_plate for each dictionary in the list
    if config.chunk_args is not None:
        jobber.chunk(**config.chunk_args)
    # jobber.create_commands(**config.create_command_args) # This is now handled by batch processing
    return jobber


def make_scripts(config_file):
    """
    creates the qsub scripts

    Parameters:
    -----------
    config_file: string
        path to configuration file

    Returns:
    ---------
    nothing
    """
    yaml_dict = parse_yaml.open_yaml(config_file)
    config = parse_yaml.parse_config_file(config_file)
    commands_location = config.create_command_args["commands_location"]
    commands_line_count = generate_scripts.lines_in_commands(commands_location)
    logfile_location = os.path.join(yaml_dict["location"], "logfiles")
    generate_scripts.make_qsub_scripts(config=config,
                                       commands_location=commands_location,
                                       commands_count_dict=commands_line_count,
                                       logfile_location=logfile_location)


def handle_generate(args):
    """Handles the 'generate' subcommand: creates job commands and scripts."""
    config_file = args.config_file
    if not os.path.isfile(config_file):
        raise ValueError(f"'{config_file}' is not a file")

    # Parse config and create job as before
    config = parse_yaml.parse_config_file(config_file)
    yaml_dict = parse_yaml.open_yaml(config_file)
    commands_location = os.path.expandvars(config.create_command_args["commands_location"])
    location = os.path.expandvars(config.create_command_args["location"])
    logfile_location = os.path.expandvars(os.path.join(yaml_dict["location"], "logfiles"))

    # Ensure directories exist
    os.makedirs(commands_location, exist_ok=True)
    os.makedirs(logfile_location, exist_ok=True)

    # Configure job with ALL plates (this remains the same)
    jobber = configure_job(config)

    # NEW: PLATE BATCH PLANNING PHASE
    from cptools2.generate_scripts import plan_plate_batches, generate_batch_workflows, generate_batch_scripts, create_sequential_submission_script
    
    # Analyze plates and create batching plan
    batch_plan = plan_plate_batches(jobber, location, logfile_location)
    
    # Generate workflows for each plate batch
    batch_workflows = generate_batch_workflows(
        config, jobber, batch_plan, commands_location, logfile_location
    )
    
    # Generate a single hex for the entire job run to manage dependencies
    job_hex = script_generator.generate_random_hex()

    # Generate scripts for each batch workflow
    batch_scripts = []
    for workflow in batch_workflows:
        # Generate SGE scripts for this batch using existing logic
        batch_script_info = generate_batch_scripts(
            workflow, config, logfile_location, job_hex
        )
        batch_scripts.append(batch_script_info)
    
    # Create master submission script with dependencies
    create_sequential_submission_script(batch_scripts, commands_location)
    
    print("Plate-based batch generation DONE!")


def handle_join(args):
    """Handles the 'join' subcommand: joins result files."""
    pretty_print(f"Joining files in {colours.yellow(args.location)} for patterns: {colours.yellow(', '.join(args.patterns))}")
    # Construct raw_data location from the base location provided
    raw_data_location = os.path.join(args.location, "raw_data")
    
    # Call join_plate_files directly. Pass None for plate_store 
    # as it will discover plates from the directory structure.
    results = file_tools.join_plate_files(
        plate_store=None, 
        raw_data_location=raw_data_location, 
        patterns=args.patterns
    )
    
    if results:
        pretty_print("File joining DONE!")
    else:
        pretty_print("File joining finished (no files joined or patterns specified).")


def main():
    """Main entry point: parses arguments and calls appropriate handler."""
    parser = argparse.ArgumentParser(description="cptools2: Generate and manage CellProfiler analysis jobs.")
    subparsers = parser.add_subparsers(dest='command', help='Sub-command help')
    subparsers.required = True # Require a subcommand

    # Subparser for the original functionality: generating scripts from config
    parser_generate = subparsers.add_parser('generate', help='Generate SGE scripts from a YAML config file.')
    parser_generate.add_argument('config_file', type=str, help='Path to the YAML configuration file.')
    parser_generate.set_defaults(func=handle_generate)

    # Subparser for the new join functionality
    parser_join = subparsers.add_parser('join', help='Join chunked result files (e.g., Image.csv).')
    parser_join.add_argument('--location', type=str, required=True, help='Base location directory containing the raw_data subdirectory.')
    parser_join.add_argument('--patterns', type=str, required=True, nargs='+', help='File name patterns to join (e.g., Image.csv Cells.csv).')
    parser_join.set_defaults(func=handle_join)

    args = parser.parse_args()

    # Basic environment check (can be adapted based on where 'join' might run)
    # If 'join' might run off-node, this check needs refinement.
    # For now, assume it runs where datastore access is possible if needed by underlying funcs.
    # Note: utils.on_staging_node() might not be universally applicable.
    # Consider if specific checks are needed only for 'generate'.
    # if not utils.on_staging_node():
    #     raise EddieNodeError("Environment check failed (e.g., not on staging node)")

    # Call the function associated with the chosen subcommand
    args.func(args)


class EddieNodeError(Exception):
    pass


if __name__ == "__main__":
    main()
