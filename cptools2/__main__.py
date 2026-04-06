import argparse
import os
import shutil
import sys

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


def cmd_pipeline(args):
    """Handle the 'pipeline' subcommand."""
    config_file = _check_config_file(args.config)
    pretty_print("parsing config file: {}".format(config_file))

    config = parse_yaml.parse_config_file(config_file)

    # Override stages if --stages flag provided
    if args.stages:
        config["stages"] = args.stages

    # Expand stage aliases
    if config.get("stages"):
        config["stages"] = parse_yaml.expand_stage_aliases(config["stages"])

    # Determine output path for params.json
    cmd_args = config.get("create_command_args") or {}
    location = cmd_args.get("location", os.path.dirname(os.path.abspath(config_file)))
    params_path = os.path.join(location, "params.json")

    # Generate params.json
    parse_yaml.generate_params_json(config, params_path)
    pretty_print("wrote params.json: {}".format(params_path))

    if args.dry_run:
        pretty_print("dry-run mode: params.json generated, skipping Nextflow")
        return

    # Check for nextflow
    if not _find_nextflow():
        print(NEXTFLOW_INSTALL_MSG, file=sys.stderr)
        sys.exit(1)

    # Build nextflow command
    nf_cmd = ["nextflow", "run", "main.nf", "-params-file", params_path]
    if args.resume:
        nf_cmd.append("-resume")

    pretty_print("running: {}".format(" ".join(nf_cmd)))
    os.execvp("nextflow", nf_cmd)


def cmd_prepare(args):
    """Handle the 'prepare' subcommand: data preparation only (no Nextflow)."""
    config_file = _check_config_file(args.config)
    pretty_print("parsing config file: {}".format(config_file))

    config = parse_yaml.parse_config_file(config_file)

    # Override stages if --stages flag provided
    if args.stages:
        config["stages"] = args.stages

    # Expand stage aliases
    if config.get("stages"):
        config["stages"] = parse_yaml.expand_stage_aliases(config["stages"])

    # Determine output path for params.json
    cmd_args = config.get("create_command_args") or {}
    location = cmd_args.get("location", os.path.dirname(os.path.abspath(config_file)))
    params_path = os.path.join(location, "params.json")

    # Generate params.json
    parse_yaml.generate_params_json(config, params_path)
    pretty_print("wrote params.json: {}".format(params_path))
    pretty_print("data preparation complete (Nextflow not invoked)")


def cmd_join(args):
    """Handle the 'join' subcommand: join result CSVs after analysis."""
    # Lazy import to avoid pulling in pandas at CLI startup
    from cptools2 import file_tools

    pretty_print("joining files in: {}".format(args.location))
    file_tools.join_output_files(args.location, args.patterns)
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


class EddieNodeError(Exception):
    pass


if __name__ == "__main__":
    main()
