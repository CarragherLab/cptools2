"""
Option to give an executable a yaml file and parse all the information rather
than a load of command line arguments. More readable and serves as a record.


General idea is to convert the configuration yaml file into a dictionary,
then these functions parse that dictionary into separate dictionaries
which can be used in other functions in cptools2 using **kwargs.
"""

import json
import os

import yaml

from cptools2.nextflow_diagnostics import diagnostics_flags_for_mode


def _path_arg(value):
    """Normalize a scalar-or-list path argument without requiring local existence."""
    if isinstance(value, list):
        value = value[0]
    path = os.path.expandvars(str(value))
    if not os.path.isabs(path):
        path = os.path.abspath(path)
    return path


def _join_remote_safe(base, *parts):
    """Join paths without converting Unix-style Eddie paths to Windows separators."""
    base = str(base)
    if base.startswith("/"):
        return "/".join([base.rstrip("/")] + [str(p).strip("/") for p in parts])
    return os.path.join(base, *parts)


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
    if "experiment" in yaml_dict or "input_dir" in yaml_dict:
        experiment_arg = yaml_dict.get("experiment", yaml_dict.get("input_dir"))
        return {"exp_dir": _path_arg(experiment_arg)}
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
        return {"job_size": int(chunk_arg)}
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
            return {"exp_dir": experiment, "plates": plates}
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
        return {"plates": remove_arg}
    else:
        return None


def is_new_ix(yaml_dict):
    """Legacy stub. Layout auto-detection in filelist.py handles this."""
    return False


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
    pipeline_arg = None
    if "pipeline" in yaml_dict:
        pipeline_arg = _path_arg(yaml_dict["pipeline"])
    if "location" in yaml_dict or "output_dir" in yaml_dict:
        location_arg = yaml_dict.get("location", yaml_dict.get("output_dir"))
        if isinstance(location_arg, list):
            location_arg = location_arg[0]
        location_arg = _path_arg(location_arg)
    else:
        location_arg = os.path.dirname(os.path.abspath("params.json"))
    # TODO more options rather than exactly "commands location"
    if "commands location" in yaml_dict:
        commands_loc_arg = yaml_dict["commands location"]
        if isinstance(commands_loc_arg, list):
            commands_loc_arg = commands_loc_arg[0]
        commands_loc_arg = _path_arg(commands_loc_arg)
    else:
        commands_loc_arg = _join_remote_safe(location_arg, "commands")
    # need the chunk size to check LoadData dataframes are the correct size
    if "chunk" in yaml_dict:
        chunk_arg = yaml_dict["chunk"]
        if isinstance(chunk_arg, list):
            chunk_arg = int(chunk_arg[0])
    else:
        chunk_arg = None
    return {
        "pipeline": pipeline_arg,
        "location": location_arg,
        "commands_location": commands_loc_arg,
        "job_size": chunk_arg,
    }


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
    valid_args = [
        "experiment",
        "chunk",
        "pipeline",
        "location",
        "commands location",
        "remove plate",
        "add plate",
        "new_ix",
        "channels",
        "stages",
        "segmentation",
        "feature_extraction",
        "feature_export",
        "containers",
        "join_files",
        "data_destination",
        "container_path",
        "output_dir",
        "input_dir",
        "plate_list",
        "plates",
        "stage_data",
        "expected_channels",
        "max_chunks",
        "scratch_quota_gb",
        "plate_sizes_gb",
        "scratch_utilisation_fraction",
        "scratch_work_factor",
        "nextflow_diagnostics",
        "illum_pipeline_calculate",
        "illum_pipeline_apply",
        "seg_pipeline",
    ]
    bad_arguments = []
    for argument in yaml_dict.keys():
        if argument not in valid_args:
            bad_arguments.append(argument)
    if len(bad_arguments) > 0:
        err_msg = "Unrecognized argument(s) : {}".format(bad_arguments)
        raise ValueError(err_msg)


def join_files(yaml_dict):
    """
    Get specifications for joining files after analysis.

    Parameters:
    -----------
    yaml_dict: dict
        dictionary version of the config yaml file

    Returns:
    --------
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

    Parameters:
    -----------
    yaml_dict: dict
        dictionary version of the config yaml file

    Returns:
    --------
    str or None
        The data destination path, or None if not specified.
    """
    if "data_destination" in yaml_dict:
        dest_arg = yaml_dict["data_destination"]
        if isinstance(dest_arg, list):
            return _path_arg(dest_arg[0])
        elif isinstance(dest_arg, str):
            return _path_arg(dest_arg)
    return None


def plates(yaml_dict):
    """Get explicit plate IDs for Nextflow execution."""
    plate_arg = yaml_dict.get("plates", yaml_dict.get("plate_list"))
    if plate_arg is None:
        return None
    if isinstance(plate_arg, str):
        return [p.strip() for p in plate_arg.split(",") if p.strip()]
    if isinstance(plate_arg, list):
        return [str(p) for p in plate_arg]
    return [str(plate_arg)]


def stage_data(yaml_dict):
    """Return whether DataStore staging should be enabled."""
    return bool(yaml_dict.get("stage_data", True))


def validate_staging_contract(yaml_dict):
    """DataStore-backed runs must stage data before compute."""
    input_arg = yaml_dict.get("input_dir", yaml_dict.get("experiment"))
    if isinstance(input_arg, list):
        input_arg = input_arg[0]
    input_arg = str(input_arg or "")
    if "/datastore/" in input_arg.replace("\\", "/") and not stage_data(yaml_dict):
        raise ValueError(
            "DataStore inputs must use stage_data: true. "
            "Staging nodes copy data; compute/GPU nodes process staged data."
        )


def validate_ai_segmentation_contract(yaml_dict):
    """AI feature extraction paths require Cellpose segmentation."""
    feature_extraction = yaml_dict.get("feature_extraction")
    if not isinstance(feature_extraction, dict):
        return
    tool = str(feature_extraction.get("tool", "")).lower()
    if tool in {"", "cellprofiler"}:
        return
    segmentation = yaml_dict.get("segmentation", {})
    engine = ""
    if isinstance(segmentation, dict):
        engine = str(segmentation.get("engine", "")).lower()
    elif isinstance(segmentation, str):
        engine = segmentation.lower()
    if engine and engine != "cellpose":
        raise ValueError(
            "AI feature extraction tool '{}' requires Cellpose segmentation. "
            "Set segmentation.engine: cellpose or omit segmentation.engine to use "
            "the AI pipeline default.".format(tool)
        )


def nextflow_pipeline_paths(yaml_dict):
    """Return explicit Nextflow pipeline template paths from the YAML config."""
    paths = {}
    for key in ["illum_pipeline_calculate", "illum_pipeline_apply", "seg_pipeline"]:
        if key in yaml_dict:
            paths[key] = _path_arg(yaml_dict[key])
    return paths


def parse_config_file(config_file):
    """
    parse config file, return a plain dict

    Parameters:
    ------------
    config_file: string
        path to configuration/yaml file which lists the experiment, pipeline etc.

    Returns:
    ---------
    dict with keys:
        experiment_args     : dict or None
        chunk_args          : dict or None
        remove_plate_args   : dict or None
        add_plate_args      : dict or None
        create_command_args : dict
        is_new_ix           : bool
        channels            : list or None
        stages              : list or None
        segmentation        : dict or None
        feature_extraction  : dict or None
        containers          : dict or None
    """
    yaml_dict = open_yaml(config_file)
    # check the arguments in the yaml file are recognised
    check_yaml_args(yaml_dict)
    validate_staging_contract(yaml_dict)
    validate_ai_segmentation_contract(yaml_dict)
    config = {
        "experiment_args": experiment(yaml_dict),
        "chunk_args": chunk(yaml_dict),
        "remove_plate_args": remove_plate(yaml_dict),
        "add_plate_args": add_plate(yaml_dict),
        "create_command_args": create_commands(yaml_dict),
        "is_new_ix": is_new_ix(yaml_dict),
        "channels": yaml_dict.get("channels"),
        "stages": yaml_dict.get("stages"),
        "segmentation": yaml_dict.get("segmentation"),
        "feature_extraction": yaml_dict.get("feature_extraction"),
        "feature_export": yaml_dict.get("feature_export"),
        "containers": yaml_dict.get("containers"),
        "join_files_patterns": join_files(yaml_dict),
        "data_destination_path": data_destination(yaml_dict),
        "plates": plates(yaml_dict),
        "stage_data": stage_data(yaml_dict),
        "expected_channels": yaml_dict.get("expected_channels"),
        "max_chunks": yaml_dict.get("max_chunks"),
        "scratch_quota_gb": yaml_dict.get("scratch_quota_gb"),
        "plate_sizes_gb": yaml_dict.get("plate_sizes_gb"),
        "scratch_utilisation_fraction": yaml_dict.get("scratch_utilisation_fraction"),
        "scratch_work_factor": yaml_dict.get("scratch_work_factor"),
        "nextflow_diagnostics": yaml_dict.get("nextflow_diagnostics"),
        "nextflow_pipeline_paths": nextflow_pipeline_paths(yaml_dict),
    }
    # Resolve container .sif paths from manifest if available
    from cptools2 import containers as _containers

    container_dir = _containers.resolve_container_dir(yaml_dict)
    if container_dir is not None:
        resolved_containers = {}
        for role in ["cellprofiler", "deepprofiler", "cellpose_sam"]:
            path = _containers.resolve_container_path(yaml_dict, role=role)
            if path:
                resolved_containers[role] = path
        if resolved_containers:
            config["resolved_containers"] = resolved_containers
    return config


# All valid stage names
VALID_STAGES = [
    "illum_calculate",
    "illum_apply",
    "segmentation",
    "feature_extract",
]

# Stage alias expansion table
STAGE_ALIASES = {
    "illum": ["illum_calculate", "illum_apply"],
    "segment": ["segmentation"],
    "extract": ["feature_extract"],
}


def expand_stage_aliases(stages):
    """
    Expand stage aliases to their full names.

    Deprecated: use resolve_stages() instead.

    Parameters:
    -----------
    stages: list of str
        stage names, possibly including aliases

    Returns:
    --------
    list of str with aliases expanded
    """
    if stages is None:
        return None
    expanded = []
    for stage in stages:
        if stage in STAGE_ALIASES:
            expanded.extend(STAGE_ALIASES[stage])
        else:
            expanded.append(stage)
    return expanded


def resolve_stages(stages):
    """
    Resolve stage aliases and validate stage names.

    Parameters:
    -----------
    stages: list of str or None
        stage names or aliases from the config file.

    Returns:
    --------
    list of str or None
        Expanded, deduplicated list of stage names with aliases resolved.
        Returns None if stages is None.

    Raises:
    -------
    ValueError
        If any stage name is not recognized.
    """
    if stages is None:
        return None
    resolved = []
    for stage in stages:
        if stage in STAGE_ALIASES:
            resolved.extend(STAGE_ALIASES[stage])
        elif stage in VALID_STAGES:
            resolved.append(stage)
        else:
            raise ValueError(
                "Unrecognized stage '{}'. "
                "Valid stages: {}. "
                "Valid aliases: {}.".format(
                    stage, VALID_STAGES, list(STAGE_ALIASES.keys())
                )
            )
    # Deduplicate while preserving order
    seen = set()
    deduped = []
    for s in resolved:
        if s not in seen:
            seen.add(s)
            deduped.append(s)
    return deduped


def generate_params_json(config_dict, output_path):
    """
    Write a Nextflow params.json file from a parsed config dict.

    Parameters:
    -----------
    config_dict: dict
        parsed config dict from parse_config_file()
    output_path: str
        path to write the JSON file

    Returns:
    --------
    str: the output_path written to
    """
    params = {}

    # experiment / location
    exp_args = config_dict.get("experiment_args")
    if exp_args is not None:
        params["experiment_dir"] = exp_args["exp_dir"]
        params["input_dir"] = exp_args["exp_dir"]

    cmd_args = config_dict.get("create_command_args")
    if cmd_args is not None:
        if "location" in cmd_args:
            params["location"] = cmd_args["location"]
            params["output_dir"] = cmd_args["location"]
        if "pipeline" in cmd_args:
            params["pipeline"] = cmd_args["pipeline"]

    # chunk
    chunk_args = config_dict.get("chunk_args")
    if chunk_args is not None:
        params["chunk_size"] = chunk_args["job_size"]
    else:
        params["chunk_size"] = 96

    if config_dict.get("max_chunks") is not None:
        params["max_chunks"] = config_dict["max_chunks"]

    if config_dict.get("expected_channels") is not None:
        params["expected_channels"] = config_dict["expected_channels"]
    else:
        params["expected_channels"] = [1, 2, 3, 4, 5]

    # stages pass through as-is (CLI expands aliases before calling this)
    stages = config_dict.get("stages")
    if stages is not None:
        params["stages"] = stages

    if config_dict.get("plates") is not None:
        params["plates"] = config_dict["plates"]

    if config_dict.get("data_destination_path") is not None:
        params["data_destination"] = config_dict["data_destination_path"]

    params["stage_data"] = bool(config_dict.get("stage_data", False))

    for key, value in config_dict.get("nextflow_pipeline_paths", {}).items():
        params[key] = value

    # channels
    if config_dict.get("channels") is not None:
        params["channels"] = config_dict["channels"]

    params.update(
        diagnostics_flags_for_mode(config_dict.get("nextflow_diagnostics"))
    )

    # segmentation
    if config_dict.get("segmentation") is not None:
        params["segmentation"] = config_dict["segmentation"]
        segmentation = config_dict["segmentation"]
        if isinstance(segmentation, dict):
            if "diameter" in segmentation:
                params["seg_diameter_min"] = segmentation["diameter"]
                params["seg_diameter_max"] = segmentation["diameter"]
                params["cellpose_diameter"] = segmentation["diameter"]
            if "cellpose_channel" in segmentation:
                params["cellpose_channel"] = segmentation["cellpose_channel"]
            if "channel" in segmentation:
                params["cellpose_channel"] = segmentation["channel"]
            if "model" in segmentation:
                params["cellpose_model"] = segmentation["model"]
            if "batch_size" in segmentation:
                params["cellpose_batch_size"] = segmentation["batch_size"]

    # feature_extraction
    if config_dict.get("feature_extraction") is not None:
        params["feature_extraction"] = config_dict["feature_extraction"]
        feature_extraction = config_dict["feature_extraction"]
        if isinstance(feature_extraction, dict):
            if "tool" in feature_extraction:
                params["feature_extraction_tool"] = feature_extraction["tool"]
            if "config" in feature_extraction:
                params["feature_extraction_config"] = _path_arg(
                    feature_extraction["config"]
                )
            if "weights" in feature_extraction:
                params["feature_extraction_weights"] = _path_arg(
                    feature_extraction["weights"]
                )
            if "batch_size" in feature_extraction:
                params["feature_extraction_batch_size"] = feature_extraction[
                    "batch_size"
                ]

    feature_export = config_dict.get("feature_export") or {}
    export_enabled = bool(config_dict.get("feature_extraction")) and feature_export.get(
        "enabled", True
    )
    params["feature_export_enabled"] = export_enabled
    feature_export_formats = feature_export.get("formats", ["csv"])
    if isinstance(feature_export_formats, str):
        feature_export_formats = [feature_export_formats]
    params["feature_export_formats"] = (
        ",".join(
            str(value).strip().lower()
            for value in feature_export_formats
            if str(value).strip()
        )
        or "csv"
    )
    params["feature_export_nan_object_fail_fraction"] = float(
        feature_export.get("nan_object_fail_fraction", 0.05)
    )
    if "run_label" in feature_export:
        params["feature_export_run_label"] = str(feature_export["run_label"])

    # containers
    if config_dict.get("containers") is not None:
        params["containers"] = config_dict["containers"]

    # resolved container .sif paths take precedence if available
    if config_dict.get("resolved_containers"):
        params["containers"] = config_dict["resolved_containers"]

    output_dir = os.path.dirname(os.path.abspath(output_path))
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(params, f, indent=2)

    return output_path
