"""
Option to give an executable a yaml file and parse all the information rather
than a load of command line arguments. More readable and serves as a record.


General idea is to convert the configuration yaml file into a dictionary,
then these functions parse that dictionary into separate dictionaries
which can be used in other functions in cptools2 using **kwargs.
"""

import os
from collections import namedtuple
import yaml


def open_yaml(path_to_yaml):
    """
    return a dictionary representation of a yaml file

    Parameters:
    -----------
    path_to_yaml: string
        file path

    Returns:
    --------
    dictionary version of the yaml file
    """
    with open(path_to_yaml, "r") as f:
        yaml_dict = yaml.load(f, Loader=yaml.FullLoader)
    return yaml_dict


def experiment(yaml_dict):
    """
    get argument for Job.add_experiment method

    this is optional, so if not there then return none

    Parameters:
    -----------
    yaml_dict: dict
        dictionary version of the config yaml file

    Returns:
    -------
    dictionary
    """
    if "experiment" in yaml_dict:
        experiment_arg = yaml_dict["experiment"]
        if isinstance(experiment_arg, list):
            experiment_arg = experiment_arg[0]
        return {"exp_dir" : experiment_arg}
    else:
        return None


def chunk(yaml_dict):
    """
    get argument for Job.chunk method

    this is optional, so if not there then return none

    Parameters:
    -----------
    yaml_dict: dict
        dictionary version of the config yaml file

    Returns:
    --------
    dictionary
    """
    if "chunk" in yaml_dict:
        chunk_arg = yaml_dict["chunk"]
        if isinstance(chunk_arg, list):
            chunk_arg = chunk_arg[0]
        return {"job_size" : int(chunk_arg)}
    else:
        return None


def add_plate(yaml_dict):
    """
    get argument for Job.add_plate method

    this is optional, so if not there then return None

    Parameters:
    -----------
    yaml_dict: dict
        dictionary version of the config yaml file

    Returns:
    --------
    dictionary
    """
    if "add plate" in yaml_dict:
        add_plate_dicts = yaml_dict["add plate"]
        # returns a list of dictionaries
        if isinstance(add_plate_dicts, list):
            for d in add_plate_dicts:
                if "experiment" in d.keys():
                    # is the experiment labels
                    experiment = str(d["experiment"])
                if "plates" in d.keys():
                    # is the plates, either a string or a list
                    plate_args = d["plates"]
                    if isinstance(plate_args, str):
                        plates = [d["plates"]]
                    if isinstance(plate_args, list):
                        plates = d["plates"]
            return {"exp_dir" : experiment, "plates" : plates}
    else:
        return None


def remove_plate(yaml_dict):
    """
    get argument for Job.remove_plate method

    this is optional, so not there then return None

    Parameters:
    -----------
    yaml_dict: dict
        dictionary version of the config yaml file

    Returns:
    --------
    dictionary
    """
    if "remove plate" in yaml_dict:
        remove_arg = yaml_dict["remove plate"]
        # can either be a string or a list in Job.remove plate
        return {"plates" : remove_arg}
    else:
        return None


def is_new_ix(yaml_dict):
    """docstring"""
    if "new_ix" in yaml_dict:
        new_ix = yaml_dict["new_ix"]
    else:
        new_ix = False
    return new_ix


def create_commands(yaml_dict):
    """
    get arguments for Job.create_commands

    not optional, so error if no matching keys are found

    Parameters:
    -----------
    yaml_dict: dict
        dictionary version of the config yaml file

    Returns:
    --------
    dictionary
    """
    if "pipeline" in yaml_dict:
        pipeline_arg = yaml_dict["pipeline"]
        if isinstance(pipeline_arg, list):
            pipeline_arg = pipeline_arg[0]
        pipeline_arg = os.path.abspath(pipeline_arg)
        if not os.path.isfile(pipeline_arg):
            raise IOError("'{}' pipeline not found".format(pipeline_arg))
    if "location" in yaml_dict:
        location_arg = yaml_dict["location"]
        if isinstance(location_arg, list):
            location_arg = location_arg[0]
    # TODO more options rather than exactly "commands location"
    if "commands location" in yaml_dict:
        commands_loc_arg = yaml_dict["commands location"]
        if isinstance(commands_loc_arg, list):
            commands_loc_arg = commands_loc_arg[0]
    # need the chunk size to check LoadData dataframes are the correct size
    if "chunk" in yaml_dict:
        chunk_arg = yaml_dict["chunk"]
        if isinstance(chunk_arg, list):
            chunk_arg = int(chunk_arg[0])
    else:
        chunk_arg = None
    return {"pipeline"          : pipeline_arg,
            "location"          : location_arg,
            "commands_location" : commands_loc_arg,
            "job_size"          : chunk_arg}


def check_yaml_args(yaml_dict):
    """
    check the validity of the yaml arguments

    raises a ValueError if any of the arguments in the yaml setup file are
    not recognised

    Parameters:
    -----------
    yaml_dict: dict
        dictionary version of the config yaml file

    Returns:
    --------
    nothing if successful, otherwise raises a ValueError
    """
    valid_args = ["experiment",
                  "chunk",
                  "pipeline",
                  "location",
                  "commands location",
                  "remove plate",
                  "add plate",
                  "new_ix"]
    bad_arguments = []
    for argument in yaml_dict.keys():
        if argument not in valid_args:
            bad_arguments.append(argument)
    if len(bad_arguments) > 0:
        err_msg = "Unrecognized argument(s) : {}".format(bad_arguments)
        raise ValueError(err_msg)


def parse_config_file(config_file):
    """
    parse config file, store dictionaries in a named tuple

    Parameters:
    ------------
    config_file: string
        path to configuration/yaml file which lists the experiment, pipeline etc.

    Returns:
    ---------
    namedtuple:
        config.experiment_args     : dict
        config.chunk_args          : dict
        config.remove_plate_args   : dict
        config.add_plate_args      : dict
        config.create_command_args : dict
        config.is_new_ix           : bool
    """
    yaml_dict = open_yaml(config_file)
    # check the arguments in the yaml file are recognised
    check_yaml_args(yaml_dict)
    # create namedtuple to store the configuration dictionaries
    names = ["experiment_args", "chunk_args", "add_plate_args",
             "remove_plate_args", "create_command_args", "is_new_ix"]
    config = namedtuple("config", names)
    return config(experiment_args=experiment(yaml_dict),
                  chunk_args=chunk(yaml_dict),
                  remove_plate_args=remove_plate(yaml_dict),
                  add_plate_args=add_plate(yaml_dict),
                  create_command_args=create_commands(yaml_dict),
                  is_new_ix=is_new_ix(yaml_dict))
