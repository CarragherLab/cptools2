import argparse
import json
import os
import shutil
import subprocess
import sys

from cptools2 import batch as batch_module
from cptools2 import parse_yaml
from cptools2.colours import pretty_print


__version__ = "0.3.0"


NEXTFLOW_INSTALL_MSG = """\
Nextflow is not installed or not on PATH.

Install Nextflow:
  curl -s https://get.nextflow.io | bash
  # or via conda:
  conda install -c bioconda nextflow

Then ensure 'nextflow' is on your PATH and try again.
"""


def _check_config_file(path):
    """Validate that the config file exists."""
    if not os.path.isfile(path):
        raise FileNotFoundError("Config file not found: '{}'".format(path))
    return path


def _find_nextflow():
    """Check if nextflow is available on PATH."""
    return shutil.which("nextflow") is not None


def _prepare_config(config_file, stages_override=None):
    """Shared config preparation for pipeline and prepare commands."""
    _check_config_file(config_file)
    pretty_print("parsing config file: {}".format(config_file))
    config = parse_yaml.parse_config_file(config_file)
    if stages_override:
        config["stages"] = stages_override
    if config.get("stages"):
        config["stages"] = parse_yaml.resolve_stages(config["stages"])
    cmd_args = config.get("create_command_args") or {}
    location = cmd_args.get("location", os.path.dirname(os.path.abspath(config_file)))
    params_path = os.path.join(location, "params.json")
    parse_yaml.generate_params_json(config, params_path)
    pretty_print("wrote params.json: {}".format(params_path))
    return config, params_path


def _is_datastore_path(path):
    """Return True for Eddie DataStore-style paths."""
    return "/datastore/" in str(path or "").replace("\\", "/")


def _configured_plate_sizes(config):
    """Return configured plate sizes as bytes, or None if not supplied."""
    configured_sizes = config.get("plate_sizes_gb")
    if configured_sizes is None:
        return None
    if not isinstance(configured_sizes, dict):
        raise ValueError("plate_sizes_gb must be a mapping of plate name to size in GB")
    return {
        str(plate): int(float(size_gb) * 1024**3)
        for plate, size_gb in configured_sizes.items()
    }


def _filter_plate_sizes(plate_sizes, configured_plates):
    """Apply an explicit plate list to measured/configured plate sizes."""
    if not configured_plates:
        return plate_sizes
    filtered = {
        plate: size
        for plate, size in plate_sizes.items()
        if plate in configured_plates
    }
    missing = sorted(set(configured_plates) - set(filtered))
    if missing:
        raise ValueError(
            "No plate size available for configured plate(s): {}".format(
                ", ".join(missing)
            )
        )
    return filtered


def _build_scratch_batches(config, quota):
    """Compute scratch-safe plate batches for the pipeline run."""
    exp_args = config.get("experiment_args") or {}
    input_dir = exp_args.get("exp_dir")
    configured_plates = config.get("plates")
    configured_sizes = _configured_plate_sizes(config)

    if configured_sizes is not None:
        plate_sizes = _filter_plate_sizes(configured_sizes, configured_plates)
    elif input_dir and os.path.isdir(input_dir):
        plate_sizes = batch_module.compute_plate_sizes(input_dir)
        plate_sizes = _filter_plate_sizes(plate_sizes, configured_plates)
    else:
        if input_dir and config.get("stage_data") and _is_datastore_path(input_dir):
            raise RuntimeError(
                "Cannot compute scratch-safe plate batches because the DataStore "
                "input directory is not accessible: {}. Run the preflight on a "
                "staging node that can see the path, or add plate_sizes_gb to the "
                "config with measured per-plate sizes.".format(input_dir)
            )
        return [{"batch_id": 1, "plates": None, "total_size_gb": 0, "plate_count": 0}]

    if not plate_sizes:
        raise RuntimeError(
            "No plate sizes were available after applying the configured plate list."
        )

    batches = batch_module.create_batches(plate_sizes, quota.available)
    pretty_print(
        "Computed {} batch(es) for {} plates".format(len(batches), len(plate_sizes))
    )
    return batches


def cmd_pipeline(args):
    """Handle the 'pipeline' subcommand."""
    config, params_path = _prepare_config(args.config, stages_override=args.stages)

    # Scratch pre-flight check
    config_quota = config.get("scratch_quota_gb")
    quota = batch_module.get_scratch_quota(config_quota)
    if quota.used_pct > 80:
        pretty_print(
            "Warning: scratch is {:.0f}% full ({:.1f}GB / {:.1f}GB)".format(
                quota.used_pct,
                quota.used / 1024**3,
                quota.total / 1024**3,
            )
        )

    batches = _build_scratch_batches(config, quota)

    cmd_args = config.get("create_command_args") or {}
    location = cmd_args.get("location", os.path.dirname(os.path.abspath(args.config)))
    batch_params = [
        _write_batch_params(params_path, batch_info, location)
        for batch_info in batches
    ]

    if args.dry_run:
        pretty_print("dry-run mode: params.json generated, skipping Nextflow")
        for batch_info, batch_params_path in zip(batches, batch_params):
            pretty_print(
                "Batch {} plates: {} params: {}".format(
                    batch_info["batch_id"],
                    batch_info["plates"] or "all configured plates",
                    batch_params_path,
                )
            )
        return

    if not _find_nextflow():
        print(NEXTFLOW_INSTALL_MSG, file=sys.stderr)
        sys.exit(1)

    # Resolve main.nf path relative to the package root
    nf_main = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "nextflow", "main.nf"
    )

    for batch_info, batch_params_path in zip(batches, batch_params):
        nf_cmd = ["nextflow", "run", nf_main, "-params-file", batch_params_path,
                  "-profile", "eddie"]
        if args.resume:
            nf_cmd.append("-resume")
        pretty_print(
            "Running batch {}: {}".format(
                batch_info["batch_id"], " ".join(nf_cmd)
            )
        )
        result = subprocess.run(nf_cmd)
        if result.returncode != 0:
            pretty_print(
                "Batch {} failed (exit {}). Fix and re-run with --resume.".format(
                    batch_info["batch_id"], result.returncode
                )
            )
            sys.exit(result.returncode)

    pretty_print("All batches complete!")


def _write_batch_params(base_params_path, batch_info, location):
    """Write params for one scratch-safe plate batch."""
    with open(base_params_path) as f:
        params = json.load(f)
    if batch_info.get("plates") is not None:
        params["plates"] = batch_info["plates"]
    batch_id = batch_info["batch_id"]
    params["batch_id"] = batch_id
    params["batch_total_size_gb"] = batch_info.get("total_size_gb", 0)
    params["batch_plate_count"] = batch_info.get("plate_count", 0)
    batch_params_path = os.path.join(location, f"params.batch_{batch_id}.json")
    with open(batch_params_path, "w") as f:
        json.dump(params, f, indent=2)
    return batch_params_path


def cmd_prepare(args):
    """Handle the 'prepare' subcommand: data preparation only (no Nextflow)."""
    _prepare_config(args.config, stages_override=args.stages)
    pretty_print("data preparation complete (Nextflow not invoked)")


def cmd_join(args):
    """Handle the 'join' subcommand: join result CSVs after analysis."""
    # Lazy import to avoid pulling in pandas at CLI startup
    from cptools2 import file_tools

    pretty_print("joining files in: {}".format(args.location))
    raw_data_location = os.path.join(args.location, "raw_data")
    file_tools.join_plate_files(
        plate_store=None,
        raw_data_location=raw_data_location,
        patterns=args.patterns,
    )
    pretty_print("DONE!")


def cmd_generate(args):
    """Deprecated 'generate' subcommand."""
    print(
        "Error: 'generate' has been removed.\n"
        "Use 'cptools2 pipeline <config.yml>' instead.\n"
        "Run 'cptools2 pipeline --help' for details.",
        file=sys.stderr,
    )
    sys.exit(1)


def build_parser():
    """Build the argparse parser with subcommands."""
    parser = argparse.ArgumentParser(
        prog="cptools2",
        description="CLI for orchestrating CellProfiler image-analysis workflows",
    )
    parser.add_argument(
        "--version", action="version", version="%(prog)s {}".format(__version__)
    )

    subparsers = parser.add_subparsers(dest="command", help="available commands")

    # --- pipeline ---
    p_pipeline = subparsers.add_parser(
        "pipeline",
        help="parse config, generate params.json, invoke Nextflow",
    )
    p_pipeline.add_argument("config", help="path to YAML config file")
    p_pipeline.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="generate params.json without invoking Nextflow",
    )
    p_pipeline.add_argument(
        "--resume",
        action="store_true",
        default=False,
        help="pass -resume to Nextflow to continue from last checkpoint",
    )
    p_pipeline.add_argument(
        "--stages",
        nargs="+",
        default=None,
        help="stages to run (aliases: illum, segment, extract)",
    )
    p_pipeline.set_defaults(func=cmd_pipeline)

    # --- prepare ---
    p_prepare = subparsers.add_parser(
        "prepare",
        help="data preparation only (generate params.json, no Nextflow)",
    )
    p_prepare.add_argument("config", help="path to YAML config file")
    p_prepare.add_argument(
        "--stages",
        nargs="+",
        default=None,
        help="stages to run (aliases: illum, segment, extract)",
    )
    p_prepare.set_defaults(func=cmd_prepare)

    # --- join ---
    p_join = subparsers.add_parser(
        "join",
        help="join result CSVs after analysis",
    )
    p_join.add_argument(
        "--location", required=True, help="path to results directory"
    )
    p_join.add_argument(
        "--patterns",
        nargs="+",
        required=True,
        help="filename patterns to match (e.g. Image.csv Cells.csv)",
    )
    p_join.set_defaults(func=cmd_join)

    # --- generate (deprecated) ---
    p_generate = subparsers.add_parser(
        "generate",
        help="[DEPRECATED] use 'pipeline' instead",
    )
    p_generate.add_argument("config", nargs="?", help="config file (ignored)")
    p_generate.set_defaults(func=cmd_generate)

    return parser


def main():
    """Entry point for cptools2 CLI."""
    parser = build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    args.func(args)


if __name__ == "__main__":
    main()
