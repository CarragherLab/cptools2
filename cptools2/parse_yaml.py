import yaml

"""
option to give a executable a yaml file and parse all the information
from rather rather than creating python commands
more readable and serves as a record
"""

def open_yaml(path_to_yaml):
    """return a dict representation of a yaml file"""
    with open(path_to_yaml, "r") as f:
        yaml_dict = yaml.load(f)
    return yaml_dict


def experiment(yaml_dict):
    """
    get argument for Job.add_experiment method

    not an optional argument, so error if not found
    """
    if "experiment" in yaml_dict:
        experiment_arg = yaml_dict["experiment"]
        if isinstance(experiment_arg, list):
            experiment_arg = experiment_arg[0]
    return {"exp_dir" : experiment_arg}


def chunk(yaml_dict):
    """
    get argument for Job.chunk method

    this is optional, so if not there then return none
    """
    if "chunk" in yaml_dict:
        chunk_arg = yaml_dict["chunk"]
        if isinstance(chunk_arg, list):
            chunk_arg = chunk_arg[0]
        return {"job_size" : int(chunk_arg)}


def add_plate(yaml_dict):
    """
    get argument for Job.add_plate method

    this is optional, so if not there then return None
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
            return {"experiment" : experiment, "plates" : plates}


def remove_plate(yaml_dict):
    """
    get argument for Job.remove_plate method

    this is optional, so not there then return None
    """
    if "remove plate" in yaml_dict:
        remove_arg = yaml_dict["remove plate"]
        # can either be a string or a list in Job.remove plate
        return {"plates" : remove_arg}


def create_commands(yaml_dict):
    """
    get arguments for Job.create_commands

    not optional, so error if no matching keys are found
    """
    if "pipeline" in yaml_dict:
        pipeline_arg = yaml_dict["pipeline"]
        if isinstance(pipeline_arg, list):
            pipeline_arg = pipeline_arg[0]
    if "location" in yaml_dict:
        location_arg = yaml_dict["location"]
        if isinstance(location_arg, list):
            location_arg = location_arg[0]
    # TODO more options rather than exactly "commands location"
    if "commands location" in yaml_dict:
        commands_loc_arg = yaml_dict["commands location"]
        if isinstance(commands_loc_arg, list):
            commands_loc_arg = commands_loc_arg[0]
    return {"pipeline" : pipeline_arg,
            "location" : location_arg,
            "commands_location" : commands_loc_arg}
