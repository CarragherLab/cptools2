"""
Option to give an executable a yaml file and parse all the information rather
than a load of command line arguments. More readable and serves as a record.


General idea is to convert the configuration yaml file into a dictionary,
then these functions parse that dictionary into separate dictionaries
which can be used in other functions in cptools2 using **kwargs.
"""

import os

import yaml


class ConfigParser(object):

    def __init__(self, yaml_path):
        """
        class docstring
        """
        self.config_path = yaml_path
        self.config_contents = self.open_config()

    @staticmethod
    def check_config_is_valid(yaml_dict):
        valid_args = [
            "experiment",
            "chunk",
            "pipeline",
            "location",
            "commands location",
            "remove plate",
            "add plate"
        ]
        # This method checks for multiple bar arguments and lists them all
        # rather than erroring at the first bad argument, saving users repeatedly
        # modifying their config file at each error.
        bad_arguments = []
        for argument in yaml_dict.keys():
            if argument not in valid_args:
                bad_arguments.append(argument)
        if len(bad_arguments) > 0:
            err_msg = "Unrecognized argument(s) : {}".format(bad_arguments)
            raise ValueError(err_msg)

    def open_config(self):
        with open(self.config_path, "r") as f:
            config_contents = yaml.load(f)
        self.check_config_is_valid(config_contents)
        return config_contents

    @property
    def experiment(self):
        if "experiment" in self.config_contents:
            experiment_arg = self.config_contents["experiment"]
            experiment_arg = unlist(experiment_arg)
            return {"exp_dir": experiment_arg}

    @property
    def chunk(self):
        if "chunk" in self.config_contents:
            chunk_arg = self.config_contents["chunk"]
            chunk_arg = unlist(chunk_arg)
            return {"job_size": int(chunk_arg)}

    @property
    def add_plate(self):
        if "add plate" in self.config_contents:
            add_plate_dicts = self.config_contents["add plate"]
            # returns a list of dictionaries
            for dictionary in add_plate_dicts:
                if "experiment" in dictionary.keys():
                    # is the experiment labels
                    experiment_arg = str(dictionary["experiment"])
                if "plates" in dictionary.keys():
                    # is the plates, either a string or a list
                    plate_args = dictionary["plates"]
                    if isinstance(plate_args, str):
                        plates = [dictionary["plates"]]
                    if isinstance(plate_args, list):
                        plates = dictionary["plates"]
            return {"exp_dir" : experiment_arg, "plates" : plates}

    @property
    def remove_plate(self):
        if "remove plate" in self.config_contents:
            remove_arg = self.config_contents["remove plate"]
            return {"plates" : remove_arg}

    @property
    def pipeline(self):
        pipeline_arg = self.config_contents["pipeline"]
        pipeline_arg = unlist(pipeline_arg)
        pipeline_arg = os.path.abspath(pipeline_arg)
        if not os.path.isfile(pipeline_arg):
            raise IOError("'{}' pipeline not found".format(pipeline_arg))
        return {"pipeline": pipeline_arg}

    @property
    def location(self):
        location_arg = self.config_contents["location"]
        location_arg = unlist(location_arg)
        return {"location": location_arg}

    @property
    def commands_location(self):
        commands_loc_arg = self.config_contents["commands location"]
        commands_loc_arg = unlist(commands_loc_arg)
        return {"commands_location": commands_loc_arg}


def unlist(possible_list):
    """
    Not sure if arguments are going to be a string or a list of length 1.
    So this function converts lists of length 1 to a string, or if
    `possible_list` is a string, will just return that.

    Parameters:
    -----------
    possible_list: list or string

    Returns:
    --------
    string
    """
    if isinstance(possible_list, list):
        possible_list = possible_list[0]
    return possible_list
