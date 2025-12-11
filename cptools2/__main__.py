import os
import argparse
from cptools2 import generate_scripts
from cptools2 import job
from cptools2 import parse_yaml
from cptools2 import colours
from cptools2 import file_tools
from cptools2 import __version__ as CPT_VERSION
from cptools2.colours import pretty_print
from cptools2.generate_scripts import get_user_scratch_quota


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
        # add_plate_args may be a single dict or a list of dicts depending on the config
        if isinstance(config.add_plate_args, dict):
            jobber.add_plate(**config.add_plate_args)
        elif isinstance(config.add_plate_args, list):
            for plate_args_dict in config.add_plate_args:
                jobber.add_plate(**plate_args_dict) # Call add_plate for each dictionary in the list
        else:
            raise TypeError(f"Unexpected add_plate_args type: {type(config.add_plate_args)}")
    if config.chunk_args is not None:
        jobber.chunk(**config.chunk_args)
    # jobber.create_commands(**config.create_command_args) # This is now handled by batch processing
    return jobber


def handle_generate(args):
    """Enhanced generate handler with integrated batch planning and progress reporting"""
    config_file = args.config_file
    if not os.path.isfile(config_file):
        raise ValueError(f"'{config_file}' is not a file")

    # 1. Parse Config
    pretty_print("[cptools2] parsing configuration...")
    config = parse_yaml.parse_config_file(config_file)
    yaml_dict = parse_yaml.open_yaml(config_file)
    commands_location = os.path.expandvars(config.create_command_args["commands_location"])
    logfile_location = os.path.expandvars(os.path.join(yaml_dict["location"], "logfiles"))

    # 2. Ensure Directories Exist
    pretty_print("[cptools2] creating output directories...")
    os.makedirs(commands_location, exist_ok=True)
    os.makedirs(logfile_location, exist_ok=True)

    # 3. Configure Job and Build Plate Store
    pretty_print("[cptools2] configuring job...")
    jobber = configure_job(config)

    # 4. Print Plate Detection Results (preserve existing format)
    platenames = sorted(jobber.plate_store.keys())
    pretty_print("[cptools2] creating image list")
    pretty_print("detected {} {}".format(
        colours.yellow(len(platenames)),
        colours.purple("plates"))
    )
    for plate in platenames:
        pretty_print(f"\t {colours.purple(plate)}")

    # 5. Batch Planning (Always enabled for consistency)
    # Use new quota-based scratch space detection
    available_scratch = get_user_scratch_quota()
    pretty_print(f"[cptools2] detected user scratch quota: {colours.yellow(f'{available_scratch/(1024**3):.1f}GB')}")
    pretty_print("[cptools2] calculating plate sizes for batch planning...")
    jobber.calculate_plate_sizes()
    pretty_print("[cptools2] creating space-optimal batches...")
    batches = jobber.create_plate_batches(available_scratch)
    pretty_print(f"[cptools2] created {colours.yellow(len(batches))} optimized batches:")
    for batch in batches:
        plate_list = ", ".join(batch['plates'][:3])
        if len(batch['plates']) > 3:
            plate_list += f" (+ {len(batch['plates'])-3} more)"
        size_str = f"{batch['total_size_gb']:.1f}GB"
        pretty_print(f"\t batch {colours.yellow(batch['batch_id'])}: "
                     f"{colours.purple(len(batch['plates']))} plates, "
                     f"{colours.green(size_str)} - {plate_list}")
    pretty_print("[cptools2] batch planning complete. Ready to generate command files.")
    pretty_print("[cptools2] generating batch-specific command files...")
    jobber.create_commands(
        **config.create_command_args,
        enable_batching=True,
        available_scratch_space=available_scratch
    )
    pretty_print("[cptools2] creating batch submission scripts...")
    generate_scripts.create_master_submit_script(
        commands_location=commands_location,
        logfile_location=logfile_location,
        batches=batches,
        config=config  # Pass config for join/transfer functionality
    )
    pretty_print(colours.green("[cptools2] workflow generation complete!"))


def handle_join(args):
    """Handles the 'join' subcommand: joins result files."""
    pretty_print(f"Joining files in {colours.yellow(args.location)} for patterns: {colours.yellow(', '.join(args.patterns))}")
    
    # Construct raw_data location from the base location provided
    raw_data_location = os.path.join(args.location, "raw_data")
    
    # If specific plates are provided, create a dummy plate_store to pass to the joiner.
    # Otherwise, it will discover all plates in the raw_data directory.
    plate_store = None
    if args.plates:
        pretty_print(f"Targeting specific plates: {colours.yellow(', '.join(args.plates))}")
        # The value doesn't matter, only the keys (plate names) are used.
        plate_store = {plate_name: None for plate_name in args.plates}

    # Call join_plate_files directly.
    results = file_tools.join_plate_files(
        plate_store=plate_store, 
        raw_data_location=raw_data_location, 
        patterns=args.patterns,
        batch_id=args.batch_id
    )
    
    if results:
        pretty_print("File joining DONE!")
    else:
        pretty_print("File joining finished (no files joined or patterns specified).")


def main():
    """Main entry point: parses arguments and calls appropriate handler."""
    parser = argparse.ArgumentParser(description="cptools2: Generate and manage CellProfiler analysis jobs.")
    parser.add_argument("--version", "-V", action="version", version=f"cptools2 {CPT_VERSION}")
    subparsers = parser.add_subparsers(dest='command', help='Sub-command help')
    subparsers.required = True # Require a subcommand

    # Subparser for the original functionality: generating scripts from config
    parser_generate = subparsers.add_parser('generate', help='Generate a master SGE submission script from a YAML config file.')
    parser_generate.add_argument('config_file', type=str, help='Path to the YAML configuration file.')
    parser_generate.add_argument('--disable-batching', action='store_true', help='Disable batch planning (default: batching enabled).')
    parser_generate.set_defaults(func=handle_generate)

    # Subparser for the new join functionality
    parser_join = subparsers.add_parser('join', help='Join chunked result files (e.g., Image.csv).')
    parser_join.add_argument('--location', type=str, required=True, help='Base location directory containing the raw_data subdirectory.')
    parser_join.add_argument('--patterns', type=str, required=True, nargs='+', help='File name patterns to join (e.g., Image.csv Cells.csv).')
    parser_join.add_argument('--plates', type=str, nargs='+', help='(Optional) Specific plate names to join. If not provided, all plates found will be joined.')
    parser_join.add_argument('--batch-id', type=int, help='(Optional) Batch ID for creating batch-specific output directories.')
    parser_join.set_defaults(func=handle_join)

    args = parser.parse_args()

    # Environment check for generate command (requires staging node access) - this check is no longer strictly necessary
    # as the heavy lifting is done on the cluster, but we can leave it as a safeguard.
    if args.command == 'generate':
        pass # The check for being on a staging node is deferred to the master script itself.

    # Call the function associated with the chosen subcommand
    args.func(args)


class EddieNodeError(Exception):
    pass


if __name__ == "__main__":
    main()
