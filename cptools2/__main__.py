import sys
import os
from cptools2 import generate_scripts
from cptools2 import job
from cptools2 import parse_yaml

def main():
    """run cptools.job.Job on a yaml file containing arguments"""
    # check arguments
    if len(sys.argv) < 2:
        msg = "missing argument: need to pass a config file as an argument"
        raise ValueError(msg)
    config_file = sys.argv[1]
    if os.path.isfile(config_file) is False:
        msg = "'{}' is not a file".format(config_file)
        raise ValueError(msg)
    # parse yaml file into a dictionary
    print("** parsing config file '{}'".format(config_file))
    yaml_dict = parse_yaml.open_yaml(config_file)
    # check the arguments in the yaml file are recognised
    parse_yaml.check_yaml_args(yaml_dict)
    # parse all possible commands
    experiment_args = parse_yaml.experiment(yaml_dict)
    chunk_args = parse_yaml.chunk(yaml_dict)
    remove_plate_args = parse_yaml.remove_plate(yaml_dict)
    add_plate_args = parse_yaml.add_plate(yaml_dict)
    create_command_args = parse_yaml.create_commands(yaml_dict)
    # some of the optional arguments might be none, in which case don't
    # pass them as arguments to the methods
    jobber = job.Job()
    if experiment_args is not None:
        jobber.add_experiment(**experiment_args)
    if remove_plate_args is not None:
        jobber.remove_plate(**remove_plate_args)
    if add_plate_args is not None:
        jobber.add_plate(**add_plate_args)
    if chunk_args is not None:
        jobber.chunk(**chunk_args)
    jobber.create_commands(**create_command_args)
    # get the number of commands in order to create the submissions scripts
    commands_location = create_command_args["commands_location"]
    commands_line_count = generate_scripts.lines_in_commands(commands_location)
    logfile_location = os.path.join(yaml_dict["location"], "logfiles")
    generate_scripts.make_qsub_scripts(commands_location, commands_line_count,
                                       logfile_location=logfile_location)
    print("DONE!")


if __name__ == "__main__":
    main()
