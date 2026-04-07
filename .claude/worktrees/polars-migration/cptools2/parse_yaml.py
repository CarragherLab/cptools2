"""
Parse YAML configuration files for cptools2 workflows.

Converts a YAML config file into a plain dictionary that can be used
by other cptools2 modules or serialized to JSON for Nextflow params.
"""

import json
import os

import yaml

from cptools2.containers import resolve_container_path, validate_container_path

# Stage alias expansion table
STAGE_ALIASES = {
    "illum": ["illum_calculate", "illum_apply"],
    "qc": ["qc_plate", "qc_well"],
}

# All valid stage names
VALID_STAGES = [
    "illum_calculate",
    "illum_apply",
    "segment",
    "feature_extract",
    "qc_plate",
    "qc_well",
]


def open_yaml(path_to_yaml):
    """
    Return a dictionary representation of a yaml file.

    Parameters
    ----------
    path_to_yaml : str
        File path to the YAML configuration file.

    Returns
    -------
    dict
        Dictionary version of the yaml file.
    """
    with open(path_to_yaml, "r") as f:
        yaml_dict = yaml.load(f, Loader=yaml.FullLoader)
    return yaml_dict


def experiment(yaml_dict):
    """
    Get argument for Job.add_experiment method.

    This is optional, so if not there then return None.

    Parameters
    ----------
    yaml_dict : dict
        Dictionary version of the config yaml file.

    Returns
    -------
    dict or None
    """
    if "experiment" in yaml_dict:
        experiment_arg = yaml_dict["experiment"]
        if isinstance(experiment_arg, list):
            experiment_arg = experiment_arg[0]
        return {"exp_dir": experiment_arg}
    else:
        return None


def chunk(yaml_dict):
    """
    Get argument for Job.chunk method.

    This is optional, so if not there then return None.

    Parameters
    ----------
    yaml_dict : dict
        Dictionary version of the config yaml file.

    Returns
    -------
    dict or None
    """
    if "chunk" in yaml_dict:
        chunk_arg = yaml_dict["chunk"]
        if isinstance(chunk_arg, list):
            chunk_arg = chunk_arg[0]
        return {"job_size": int(chunk_arg)}
    else:
        return None


def add_plate(yaml_dict):
    """
    Get argument for Job.add_plate method.

    This is optional, so if not there then return None.

    Parameters
    ----------
    yaml_dict : dict
        Dictionary version of the config yaml file.

    Returns
    -------
    dict, list of dicts, or None
    """
    if "add plate" in yaml_dict:
        add_plate_entries = yaml_dict["add plate"]
        plate_list = []

        # Normalize to list
        if not isinstance(add_plate_entries, list):
            add_plate_entries = [add_plate_entries]

        # Support entries where 'experiment' and 'plates' may be split across
        # consecutive list items
        i = 0
        while i < len(add_plate_entries):
            entry = add_plate_entries[i]
            if not isinstance(entry, dict):
                i += 1
                continue

            exp_dir = None
            plates = None

            if "experiment" in entry:
                exp_dir = str(entry["experiment"])

            if "plates" in entry:
                plate_args = entry["plates"]
                if isinstance(plate_args, str):
                    plates = [plate_args]
                elif isinstance(plate_args, list):
                    plates = plate_args

            # If plates missing but next entry contains plates, combine them
            if plates is None and (i + 1) < len(add_plate_entries):
                next_entry = add_plate_entries[i + 1]
                if isinstance(next_entry, dict) and "plates" in next_entry:
                    plate_args = next_entry["plates"]
                    if isinstance(plate_args, str):
                        plates = [plate_args]
                    elif isinstance(plate_args, list):
                        plates = plate_args
                    i += 1  # skip the next entry as we've consumed it

            if exp_dir is not None and plates is not None:
                plate_list.append({"exp_dir": exp_dir, "plates": plates})

            i += 1

        if not plate_list:
            return None
        if len(plate_list) == 1:
            return plate_list[0]
        return plate_list
    else:
        return None


def remove_plate(yaml_dict):
    """
    Get argument for Job.remove_plate method.

    This is optional, so if not there then return None.

    Parameters
    ----------
    yaml_dict : dict
        Dictionary version of the config yaml file.

    Returns
    -------
    dict or None
    """
    if "remove plate" in yaml_dict:
        remove_arg = yaml_dict["remove plate"]
        return {"plates": remove_arg}
    else:
        return None


def is_new_ix(yaml_dict):
    """
    Legacy helper retained for backwards compatibility only.

    The ``new_ix`` argument has been removed. ImageXpress layout is
    auto-detected per-plate. This function always returns False.
    """
    return False


def create_commands(yaml_dict):
    """
    Get arguments for Job.create_commands.

    Not optional, so error if no matching keys are found.

    Parameters
    ----------
    yaml_dict : dict
        Dictionary version of the config yaml file.

    Returns
    -------
    dict
    """
    # Check for required keys first
    required_keys = ["pipeline", "location", "commands location"]
    missing_keys = [key for key in required_keys if key not in yaml_dict]
    if missing_keys:
        raise ValueError(
            f"Missing required configuration key(s): {', '.join(missing_keys)}"
        )

    # Process pipeline argument
    pipeline_arg = yaml_dict["pipeline"]
    if isinstance(pipeline_arg, list):
        pipeline_arg = pipeline_arg[0]
    pipeline_arg = os.path.expandvars(pipeline_arg)
    pipeline_arg = os.path.abspath(pipeline_arg)
    if not os.path.isfile(pipeline_arg):
        raise IOError(f"'{pipeline_arg}' pipeline not found")

    # Process location argument
    location_arg = yaml_dict["location"]
    if isinstance(location_arg, list):
        location_arg = location_arg[0]
    location_arg = os.path.expandvars(location_arg)

    # Process commands location argument
    commands_loc_arg = yaml_dict["commands location"]
    if isinstance(commands_loc_arg, list):
        commands_loc_arg = commands_loc_arg[0]
    commands_loc_arg = os.path.expandvars(commands_loc_arg)

    # Process optional chunk argument
    chunk_arg = None
    if "chunk" in yaml_dict:
        chunk_val = yaml_dict["chunk"]
        if isinstance(chunk_val, list):
            chunk_arg = int(chunk_val[0])
        elif isinstance(chunk_val, (int, str)):
            try:
                chunk_arg = int(chunk_val)
            except ValueError:
                raise ValueError(
                    f"Invalid value for 'chunk': {chunk_val}. Must be an integer."
                )
        else:
            raise ValueError(
                f"Invalid type for 'chunk': {type(chunk_val)}. "
                "Must be an integer or list containing an integer."
            )

    return {
        "pipeline": pipeline_arg,
        "location": location_arg,
        "commands_location": commands_loc_arg,
        "job_size": chunk_arg,
    }


def check_yaml_args(yaml_dict):
    """
    Check the validity of the yaml arguments.

    Raises a ValueError if any of the arguments in the yaml setup file are
    not recognised.

    Parameters
    ----------
    yaml_dict : dict
        Dictionary version of the config yaml file.

    Returns
    -------
    None
        Nothing if successful, otherwise raises a ValueError.
    """
    valid_args = [
        # Legacy keys (generate workflow)
        "experiment",
        "chunk",
        "pipeline",
        "location",
        "commands location",
        "remove plate",
        "add plate",
        "join_files",
        "data_destination",
        "container_path",
        # New keys (pipeline/Nextflow workflow)
        "channels",
        "stages",
        "segmentation",
        "feature_extraction",
        "containers",
        "output_dir",
        "input_dir",
        "plate_list",
    ]
    bad_arguments = []
    for argument in yaml_dict.keys():
        if argument not in valid_args:
            bad_arguments.append(argument)
    if len(bad_arguments) > 0:
        err_msg = "Unrecognized argument(s) : {}".format(bad_arguments)
        raise ValueError(err_msg)


def resolve_stages(stages):
    """
    Resolve stage aliases to their constituent stage names.

    Parameters
    ----------
    stages : list of str
        Stage names or aliases from the config file.

    Returns
    -------
    list of str
        Expanded list of stage names with aliases resolved.

    Raises
    ------
    ValueError
        If any stage name is not recognized.
    """
    resolved = []
    for stage in stages:
        if stage in STAGE_ALIASES:
            resolved.extend(STAGE_ALIASES[stage])
        elif stage in VALID_STAGES:
            resolved.append(stage)
        else:
            raise ValueError(
                f"Unrecognized stage '{stage}'. "
                f"Valid stages: {VALID_STAGES}. "
                f"Valid aliases: {list(STAGE_ALIASES.keys())}."
            )
    # Deduplicate while preserving order
    seen = set()
    deduped = []
    for s in resolved:
        if s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped


def container_path(yaml_dict):
    """
    Resolve the path to the CellProfiler Singularity container (.sif).

    Delegates to the ``containers`` module which handles the full resolution
    chain: YAML config -> CPTOOLS2_CONTAINER_DIR env var -> None.

    Parameters
    ----------
    yaml_dict : dict
        Dictionary version of the config yaml file.

    Returns
    -------
    str or None
        Absolute path to the .sif container, or None if not configured.
    """
    return resolve_container_path(yaml_dict)


def join_files(yaml_dict):
    """
    Get specifications for joining files after analysis.

    Parameters
    ----------
    yaml_dict : dict
        Dictionary version of the config yaml file.

    Returns
    -------
    list or None
        List of file patterns to join, or None if not specified.
    """
    if "join_files" in yaml_dict:
        join_files_arg = yaml_dict["join_files"]
        if isinstance(join_files_arg, str):
            return [join_files_arg]
        elif isinstance(join_files_arg, list):
            return join_files_arg
    return None


def data_destination(yaml_dict):
    """
    Get the destination path for transferring joined data.

    This is optional, so if not there then return None.

    Parameters
    ----------
    yaml_dict : dict
        Dictionary version of the config yaml file.

    Returns
    -------
    str or None
        The data destination path, or None if not specified.
    """
    if "data_destination" in yaml_dict:
        dest_arg = yaml_dict["data_destination"]
        if isinstance(dest_arg, list):
            return os.path.expandvars(str(dest_arg[0]))
        elif isinstance(dest_arg, str):
            return os.path.expandvars(dest_arg)
        else:
            raise ValueError(
                f"Invalid type for 'data_destination': {type(dest_arg)}. "
                "Must be a string or list of strings."
            )
    return None


def parse_config_file(config_file):
    """
    Parse config file and return a plain dictionary.

    The returned dict contains all parsed configuration, including both
    legacy keys (experiment_args, chunk_args, etc.) and new pipeline keys
    (channels, stages, containers, etc.).

    Parameters
    ----------
    config_file : str
        Path to configuration/yaml file.

    Returns
    -------
    dict
        Configuration dictionary with the following keys:
        - experiment_args : dict or None
        - chunk_args : dict or None
        - add_plate_args : dict, list of dicts, or None
        - remove_plate_args : dict or None
        - create_command_args : dict or None
        - join_files_patterns : list or None
        - data_destination_path : str or None
        - container_path : str or None
        - channels : list or None
        - stages : list or None (aliases resolved)
        - segmentation : dict or None
        - feature_extraction : dict or None
        - containers : dict or None
        - raw : dict (the original yaml dict)
    """
    yaml_dict = open_yaml(config_file)
    check_yaml_args(yaml_dict)

    # Resolve container path via the containers module
    resolved_container = validate_container_path(container_path(yaml_dict))

    config = {
        "experiment_args": experiment(yaml_dict),
        "chunk_args": chunk(yaml_dict),
        "add_plate_args": add_plate(yaml_dict),
        "remove_plate_args": remove_plate(yaml_dict),
        "join_files_patterns": join_files(yaml_dict),
        "data_destination_path": data_destination(yaml_dict),
        "container_path": resolved_container,
        # New pipeline keys
        "channels": yaml_dict.get("channels"),
        "stages": yaml_dict.get("stages"),
        "segmentation": yaml_dict.get("segmentation"),
        "feature_extraction": yaml_dict.get("feature_extraction"),
        "containers": yaml_dict.get("containers"),
        # Keep raw yaml for passthrough
        "raw": yaml_dict,
    }

    # Only parse create_commands if legacy keys are present
    if all(k in yaml_dict for k in ("pipeline", "location", "commands location")):
        config["create_command_args"] = create_commands(yaml_dict)
    else:
        config["create_command_args"] = None

    # Resolve stage aliases if stages are specified
    if config["stages"] is not None:
        config["stages"] = resolve_stages(config["stages"])

    return config


def generate_params_json(config_dict, output_path):
    """
    Generate a Nextflow params.json file from a parsed config dictionary.

    The params.json file is a flat JSON dictionary suitable for passing
    to Nextflow via the ``-params-file`` flag.

    Parameters
    ----------
    config_dict : dict
        Configuration dictionary as returned by parse_config_file().
    output_path : str
        Path where the params.json file will be written.

    Returns
    -------
    str
        Absolute path to the written params.json file.
    """
    raw = config_dict.get("raw", {})
    params = {}

    # Map config keys to Nextflow params
    if raw.get("input_dir"):
        params["input_dir"] = raw["input_dir"]
    elif raw.get("experiment"):
        params["input_dir"] = raw["experiment"]

    if raw.get("output_dir"):
        params["output_dir"] = raw["output_dir"]
    elif raw.get("location"):
        params["output_dir"] = raw["location"]

    if config_dict.get("stages"):
        params["stages"] = config_dict["stages"]

    if config_dict.get("channels"):
        params["channels"] = config_dict["channels"]

    if config_dict.get("segmentation"):
        params["segmentation"] = config_dict["segmentation"]

    if config_dict.get("feature_extraction"):
        params["feature_extraction"] = config_dict["feature_extraction"]

    if config_dict.get("containers"):
        params["containers"] = config_dict["containers"]

    if raw.get("container_path"):
        params["container_path"] = raw["container_path"]

    if raw.get("chunk"):
        chunk_val = raw["chunk"]
        if isinstance(chunk_val, list):
            chunk_val = chunk_val[0]
        params["chunk_size"] = int(chunk_val)

    if raw.get("plate_list"):
        params["plate_list"] = raw["plate_list"]

    # Write params.json
    output_path = os.path.abspath(output_path)
    parent_dir = os.path.dirname(output_path)
    if parent_dir:
        os.makedirs(parent_dir, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(params, f, indent=2)

    return output_path
