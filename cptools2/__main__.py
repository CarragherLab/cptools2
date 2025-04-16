import sys
import os
from cptools2 import generate_scripts
from cptools2 import job
from cptools2 import parse_yaml
from cptools2 import utils
from cptools2 import colours
from cptools2.colours import pretty_print


def check_arguments():
    """docstring"""
    if len(sys.argv) < 2:
        msg = "missing argument: need to pass a config file as an argument"
        raise ValueError(msg)


def check_config_file():
    """check that the config file exists, raise an error if it doesnt"""
    config_file = sys.argv[1]
    if os.path.isfile(config_file) is False:
        msg = "'{}' is not a file".format(config_file)
        raise ValueError(msg)
    return config_file


def configure_job(config):
    """
    configure job to generate the commands and scripts

    Parameters:
    ------------
    config: namedtuple
        config namedtuple containing the dictionaries which are
        passed as arguments via **kwargs to the Job class.

    Returns:
    ---------
    nothing, saves commands and scripts to disk
    """
    jobber = job.Job(is_new_ix=config.is_new_ix)
    # some of the optional arguments might be none if that option was not present in the
    # configuration file, in which case don't pass them as arguments to the methods
    if config.experiment_args is not None:
        jobber.add_experiment(**config.experiment_args)
    if config.remove_plate_args is not None:
        jobber.remove_plate(**config.remove_plate_args)
    if config.add_plate_args is not None:
        jobber.add_plate(**config.add_plate_args)
    if config.chunk_args is not None:
        jobber.chunk(**config.chunk_args)
    jobber.create_commands(**config.create_command_args)
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
    generate_scripts.make_qsub_scripts(commands_location, commands_line_count,
                                       logfile_location=logfile_location)


def main():
    """run cptools.job.Job on a yaml file containing arguments"""
    check_arguments()
    if not utils.on_staging_node():
        raise EddieNodeError("Not on a staging node, cannot access datastore")
    # parse yaml file into a dictionary
    config_file = check_config_file()
    pretty_print("parsing config file {}".format(colours.yellow(config_file)))
    config = parse_yaml.parse_config_file(config_file)
    jobber = configure_job(config)
    make_scripts(config_file)
    
    # Perform file joining if patterns are specified
    if config.join_files_patterns:
        patterns_str = ", ".join([colours.yellow(p) for p in config.join_files_patterns])
        pretty_print("Joining files for patterns: {}".format(patterns_str))
        jobber.join_results(location=config.create_command_args["location"],
                            patterns=config.join_files_patterns)
    else:
        pretty_print("No file joining will be performed (not specified in config)")
    
    pretty_print("DONE!")


class EddieNodeError(Exception):
    pass


if __name__ == "__main__":
    main()
