import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from cptools2 import batch as batch_module
from cptools2.colours import pretty_print
from cptools2.nextflow_diagnostics import (
    diagnostics_flags_for_mode,
    normalise_nextflow_diagnostics_mode,
)

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
    from cptools2 import parse_yaml

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
        plate: size for plate, size in plate_sizes.items() if plate in configured_plates
    }
    missing = sorted(set(configured_plates) - set(filtered))
    if missing:
        raise ValueError(
            "No plate size available for configured plate(s): {}".format(
                ", ".join(missing)
            )
        )
    return filtered


def _scratch_sizing_from_config(config):
    """Return validated scratch sizing values from config or defaults."""
    utilisation_fraction = config.get("scratch_utilisation_fraction")
    work_factor = config.get("scratch_work_factor")
    if utilisation_fraction is None:
        utilisation_fraction = 0.75
    if work_factor is None:
        work_factor = 1.3
    try:
        utilisation_fraction = float(utilisation_fraction)
        work_factor = float(work_factor)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "scratch_utilisation_fraction and scratch_work_factor must be numeric"
        ) from exc
    if utilisation_fraction <= 0 or utilisation_fraction > 1:
        raise ValueError(
            "scratch_utilisation_fraction must be > 0 and <= 1; got {}".format(
                utilisation_fraction
            )
        )
    if work_factor <= 0:
        raise ValueError("scratch_work_factor must be > 0; got {}".format(work_factor))
    return utilisation_fraction, work_factor


def _build_scratch_batches(
    config,
    quota,
    utilisation_fraction=None,
    work_factor=None,
):
    """Compute scratch-safe plate batches for the pipeline run."""
    exp_args = config.get("experiment_args") or {}
    input_dir = exp_args.get("exp_dir")
    configured_plates = config.get("plates")
    if utilisation_fraction is None or work_factor is None:
        utilisation_fraction, work_factor = _scratch_sizing_from_config(config)
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

    batches = batch_module.create_batches(
        plate_sizes,
        quota.available,
        utilisation_fraction=utilisation_fraction,
        work_factor=work_factor,
    )
    pretty_print(
        "Computed {} batch(es) for {} plates".format(len(batches), len(plate_sizes))
    )
    return batches


def _batch_name(batch_info):
    """Return the stable display/storage name for a batch."""
    return "batch_{:03d}".format(batch_info["batch_id"])


def _annotate_batch_paths(batch_info, location):
    """Add batch-scoped work and provenance paths without changing output_dir."""
    annotated = dict(batch_info)
    batch_name = annotated.get("batch_name") or _batch_name(annotated)
    annotated["batch_name"] = batch_name
    annotated["batch_work_dir"] = os.path.join(location, "work", batch_name)
    annotated["trace_path"] = os.path.join(
        location, "traces", "trace.{}.txt".format(batch_name)
    )
    annotated["report_path"] = os.path.join(
        location, "traces", "report.{}.html".format(batch_name)
    )
    annotated["timeline_path"] = os.path.join(
        location, "traces", "timeline.{}.html".format(batch_name)
    )
    return annotated


def _resolve_nextflow_diagnostics_mode(config, args):
    """Return the requested Nextflow diagnostics mode."""
    mode = getattr(args, "nextflow_diagnostics", None)
    if mode is None:
        mode = config.get("nextflow_diagnostics")
    return normalise_nextflow_diagnostics_mode(mode)


def _build_nextflow_command(
    nf_main, batch_params_path, batch_info, resume=False, diagnostics_mode="full"
):
    """Build the Nextflow command for one scratch-safe batch."""
    diagnostics_mode = str(diagnostics_mode).strip().lower()
    if diagnostics_mode not in {"full", "minimal", "off"}:
        raise ValueError(
            "diagnostics_mode must be one of full, minimal, or off; got {}".format(
                diagnostics_mode
            )
        )
    nf_cmd = [
        "nextflow",
        "run",
        nf_main,
        "-params-file",
        batch_params_path,
        "-profile",
        "eddie",
        "-work-dir",
        batch_info["batch_work_dir"],
    ]
    if diagnostics_mode in {"full", "minimal"}:
        nf_cmd.extend(["-with-trace", batch_info["trace_path"]])
    if diagnostics_mode == "full":
        nf_cmd.extend(["-with-report", batch_info["report_path"]])
        nf_cmd.extend(["-with-timeline", batch_info["timeline_path"]])
    if resume:
        nf_cmd.append("-resume")
    return nf_cmd


def _validate_batch_cleanup_target(batch_work_dir, work_root):
    """Return a validated exact batch work dir under the configured work root."""
    resolved_root = Path(work_root).resolve()
    resolved_target = Path(batch_work_dir).resolve()
    try:
        relative_target = resolved_target.relative_to(resolved_root)
    except ValueError as exc:
        raise ValueError(
            "Cleanup target is outside the configured work root: {}".format(
                resolved_root
            )
        ) from exc

    if len(relative_target.parts) != 1:
        raise ValueError(
            "Cleanup target must be the exact batch work dir under {}: {}".format(
                resolved_root, resolved_target
            )
        )

    batch_name = relative_target.parts[0]
    if not re.fullmatch(r"batch_\d{3}", batch_name):
        raise ValueError(
            "Cleanup target must include an expected batch_### component: {}".format(
                resolved_target
            )
        )

    return resolved_target


def _utc_now():
    """Return an ISO-8601 UTC timestamp."""
    return datetime.now(timezone.utc).isoformat()


def _batch_ledger_path(output_dir):
    """Return the batch lifecycle ledger path."""
    return os.path.join(output_dir, "batch_status.csv")


def _run_report_path(output_dir):
    """Return the user-facing pipeline run report path."""
    return os.path.join(output_dir, "run_report.md")


def _batch_ledger_columns():
    """Return the batch lifecycle CSV columns."""
    return [
        "batch_id",
        "batch_name",
        "plates",
        "nextflow_status",
        "stage_out_status",
        "cleanup_policy",
        "cleanup_status",
        "work_dir",
        "scratch_before_bytes",
        "scratch_after_bytes",
        "message",
        "params_path",
        "trace_path",
        "attempt",
        "started_at",
        "finished_at",
        "updated_at",
    ]


def _load_batch_status_rows(ledger_path):
    """Load existing batch status rows."""
    if not os.path.exists(ledger_path):
        return []
    with open(ledger_path, newline="") as f:
        return list(csv.DictReader(f))


def _write_batch_status_rows(ledger_path, rows):
    """Rewrite the batch status ledger."""
    ledger_dir = os.path.dirname(ledger_path)
    if ledger_dir:
        os.makedirs(ledger_dir, exist_ok=True)
    with open(ledger_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=_batch_ledger_columns())
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {column: row.get(column, "") for column in writer.fieldnames}
            )


def _append_batch_status_row(ledger_path, row):
    """Append one batch lifecycle row to the ledger."""
    rows = _load_batch_status_rows(ledger_path)
    rows.append(row)
    _write_batch_status_rows(ledger_path, rows)


def _batch_status_counts(rows, column):
    """Count batch ledger values for a status column."""
    counts = {}
    for row in rows:
        value = row.get(column) or "unknown"
        counts[value] = counts.get(value, 0) + 1
    return counts


def _batch_row_has_problem(row):
    """Return whether a ledger row should be highlighted in the run report."""
    return (
        row.get("nextflow_status") not in {"success"}
        or row.get("stage_out_status") in {"failed", "unverified"}
        or row.get("cleanup_status") in {"failed", "not_eligible"}
    )


def _write_run_report(output_dir, ledger_path):
    """Write a concise Markdown summary of the batch run state."""
    rows = _load_batch_status_rows(ledger_path)
    report_path = _run_report_path(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    problem_rows = [row for row in rows if _batch_row_has_problem(row)]
    nextflow_counts = _batch_status_counts(rows, "nextflow_status")
    stage_out_counts = _batch_status_counts(rows, "stage_out_status")
    cleanup_counts = _batch_status_counts(rows, "cleanup_status")

    lines = [
        "# cptools2 pipeline run report",
        "",
        "- Generated: {}".format(_utc_now()),
        "- Batch ledger: {}".format(ledger_path),
        "- Stage-out evidence: {}".format(
            os.path.join(output_dir, "stage_out_evidence")
        ),
        "- Total batch records: {}".format(len(rows)),
        "- Problem batch records: {}".format(len(problem_rows)),
        "",
        "## Summary",
        "",
        "- Nextflow status: {}".format(json.dumps(nextflow_counts, sort_keys=True)),
        "- Stage-out status: {}".format(json.dumps(stage_out_counts, sort_keys=True)),
        "- Cleanup status: {}".format(json.dumps(cleanup_counts, sort_keys=True)),
        "",
    ]

    if problem_rows:
        lines.extend(["## Problems", ""])
        lines.append(
            "| batch | nextflow | stage-out | cleanup | work dir | message |"
        )
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for row in problem_rows:
            message = (row.get("message") or "").replace("|", "\\|")
            lines.append(
                "| {batch} | {nextflow} | {stage_out} | {cleanup} | {work_dir} | {message} |".format(
                    batch=row.get("batch_name", ""),
                    nextflow=row.get("nextflow_status", ""),
                    stage_out=row.get("stage_out_status", ""),
                    cleanup=row.get("cleanup_status", ""),
                    work_dir=row.get("work_dir", ""),
                    message=message,
                )
            )
        lines.append("")
    else:
        lines.extend(["## Problems", "", "No problem batch records found.", ""])

    lines.extend(["## Batch Records", ""])
    lines.append(
        "| batch | plates | nextflow | stage-out | cleanup | trace |"
    )
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for row in rows:
        lines.append(
            "| {batch} | {plates} | {nextflow} | {stage_out} | {cleanup} | {trace} |".format(
                batch=row.get("batch_name", ""),
                plates=(row.get("plates") or "").replace("|", "\\|"),
                nextflow=row.get("nextflow_status", ""),
                stage_out=row.get("stage_out_status", ""),
                cleanup=row.get("cleanup_status", ""),
                trace=row.get("trace_path", ""),
            )
        )
    lines.append("")

    with open(report_path, "w") as f:
        f.write("\n".join(lines))
    return report_path


def _batch_attempt_number(rows, batch_name):
    """Return the next attempt number for a batch."""
    attempts = 0
    for row in rows:
        if row.get("batch_name") != batch_name:
            continue
        try:
            attempts = max(attempts, int(row.get("attempt") or 0))
        except (TypeError, ValueError):
            continue
    return attempts + 1


def _batch_required_scratch_bytes(batch_info):
    """Return the estimated scratch requirement for a batch in bytes."""
    total_size_gb = batch_info.get("total_size_gb") or 0
    return int(float(total_size_gb) * 1024**3)


def _durable_stage_out_evidence_root(batch_work_dir):
    """Return the durable stage-out evidence root for a batch."""
    batch_work_dir = Path(batch_work_dir)
    return batch_work_dir.parent.parent / "stage_out_evidence" / batch_work_dir.name


def _stage_out_verification_paths(batch_work_dir):
    """Return stage-out verification JSON files for a batch."""
    durable_root = _durable_stage_out_evidence_root(batch_work_dir)
    if durable_root.exists():
        durable_paths = sorted(durable_root.rglob("verification.json"))
        if durable_paths:
            return durable_paths

    batch_work_dir = Path(batch_work_dir)
    candidates = []
    for path in batch_work_dir.rglob("verification.json"):
        if any(parent.name == "stage_out_evidence" for parent in path.parents):
            candidates.append(path)
    return sorted(candidates)


def _read_stage_out_verification(batch_work_dir):
    """
    Parse stage-out verification artifacts for a batch.

    Returns a tuple of (status, message, evidence_paths).
    """
    evidence_paths = _stage_out_verification_paths(batch_work_dir)
    if not evidence_paths:
        return "not_required", "No stage_out_evidence/verification.json found", []

    verification_rows = []
    for path in evidence_paths:
        try:
            with open(path) as f:
                verification_rows.append((path, json.load(f)))
        except (OSError, json.JSONDecodeError) as exc:
            return "failed", "Failed to parse {}: {}".format(path, exc), evidence_paths

    statuses = []
    for path, payload in verification_rows:
        status = str(payload.get("verification_status", "")).strip().lower()
        if status not in {"verified", "unverified", "failed"}:
            return (
                "unverified",
                "Verification artifact {} missing verification_status".format(path),
                evidence_paths,
            )
        statuses.append(status)

    if all(status == "verified" for status in statuses):
        return (
            "verified",
            "Parsed {} verified stage-out artifact(s)".format(len(evidence_paths)),
            evidence_paths,
        )
    if any(status == "failed" for status in statuses):
        return (
            "failed",
            "Stage-out verification failed in {}".format(
                ", ".join(str(path) for path, _ in verification_rows)
            ),
            evidence_paths,
        )
    return (
        "unverified",
        "Stage-out verification incomplete in {}".format(
            ", ".join(str(path) for path in evidence_paths)
        ),
        evidence_paths,
    )


def _build_batch_status_row(
    batch_info,
    *,
    cleanup_policy,
    nextflow_status,
    stage_out_status,
    cleanup_status,
    scratch_before_bytes,
    scratch_after_bytes,
    message,
    params_path,
    attempt,
    started_at,
    finished_at,
):
    """Build a single batch lifecycle record."""
    return {
        "batch_id": batch_info["batch_id"],
        "batch_name": batch_info["batch_name"],
        "plates": json.dumps(batch_info.get("plates") or []),
        "nextflow_status": nextflow_status,
        "stage_out_status": stage_out_status,
        "cleanup_policy": cleanup_policy,
        "cleanup_status": cleanup_status,
        "work_dir": batch_info["batch_work_dir"],
        "scratch_before_bytes": scratch_before_bytes,
        "scratch_after_bytes": scratch_after_bytes,
        "message": message,
        "params_path": params_path,
        "trace_path": batch_info.get("trace_path", ""),
        "attempt": attempt,
        "started_at": started_at,
        "finished_at": finished_at,
        "updated_at": finished_at,
    }


def _cleanup_batch_work_dir(batch_work_dir, work_root):
    """Clean a validated batch work dir, preferring nextflow clean first."""
    validated_target = _validate_batch_cleanup_target(batch_work_dir, work_root)
    clean_cmd = [
        "nextflow",
        "clean",
        "-f",
        "-work-dir",
        str(validated_target),
    ]
    try:
        result = subprocess.run(clean_cmd)
    except OSError:
        result = None
    if result is not None and result.returncode == 0:
        return
    shutil.rmtree(str(validated_target))


def _resolve_cleanup_policy(args):
    """Resolve the batch cleanup policy from current and legacy CLI flags."""
    if getattr(args, "clean_work", False):
        return "success"
    return getattr(args, "cleanup_policy", "verified")


def cmd_pipeline(args):  # noqa: C901
    """Handle the 'pipeline' subcommand."""
    config, params_path = _prepare_config(args.config, stages_override=args.stages)
    diagnostics_mode = _resolve_nextflow_diagnostics_mode(config, args)
    cleanup_policy = _resolve_cleanup_policy(args)
    continue_on_batch_failure = getattr(args, "continue_on_batch_failure", False)
    final_exit_code = 0

    # Scratch pre-flight check
    config_quota = config.get("scratch_quota_gb")
    quota = batch_module.get_scratch_quota(config_quota)
    utilisation_fraction, work_factor = _scratch_sizing_from_config(config)
    if quota.used_pct > 80:
        pretty_print(
            "Warning: scratch is {:.0f}% full ({:.1f}GB / {:.1f}GB)".format(
                quota.used_pct,
                quota.used / 1024**3,
                quota.total / 1024**3,
            )
        )

    batches = _build_scratch_batches(
        config,
        quota,
        utilisation_fraction=utilisation_fraction,
        work_factor=work_factor,
    )
    batches = [dict(batch_info, diagnostics_mode=diagnostics_mode) for batch_info in batches]

    cmd_args = config.get("create_command_args") or {}
    location = cmd_args.get("location", os.path.dirname(os.path.abspath(args.config)))
    batches = [_annotate_batch_paths(batch_info, location) for batch_info in batches]
    batch_params = [
        _write_batch_params(params_path, batch_info, location)
        for batch_info in batches
    ]
    ledger_path = _batch_ledger_path(location)
    existing_rows = _load_batch_status_rows(ledger_path)

    if args.dry_run:
        pretty_print("dry-run mode: params.json generated, skipping Nextflow")
        for batch_info, batch_params_path in zip(batches, batch_params):
            pretty_print(
                "Batch {} ({}) plates: {} scratch: {:.2f}GB "
                "scratch_utilisation_fraction: {:.2f} scratch_work_factor: {:.2f} "
                "work: {} output_dir: {} trace: {} report: {} timeline: {} "
                "params: {}".format(
                    batch_info["batch_id"],
                    batch_info["batch_name"],
                    batch_info["plates"] or "all configured plates",
                    batch_info.get("total_size_gb", 0),
                    utilisation_fraction,
                    work_factor,
                    batch_info["batch_work_dir"],
                    location,
                    batch_info["trace_path"],
                    batch_info["report_path"],
                    batch_info["timeline_path"],
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
        "nextflow",
        "main.nf",
    )

    for batch_info, batch_params_path in zip(batches, batch_params):
        batch_started_at = _utc_now()
        batch_attempt = _batch_attempt_number(existing_rows, batch_info["batch_name"])
        current_quota = batch_module.get_scratch_quota(config_quota)
        required_scratch_bytes = _batch_required_scratch_bytes(batch_info)
        if current_quota.available < required_scratch_bytes:
            message = (
                "Batch {} requires {} bytes but only {} bytes are available; "
                "stopping before submission".format(
                    batch_info["batch_name"],
                    required_scratch_bytes,
                    current_quota.available,
                )
            )
            pretty_print(message)
            row = _build_batch_status_row(
                batch_info,
                cleanup_policy=cleanup_policy,
                nextflow_status="not_started",
                stage_out_status="not_required",
                cleanup_status="not_requested",
                scratch_before_bytes=current_quota.available,
                scratch_after_bytes=current_quota.available,
                message=message,
                params_path=batch_params_path,
                attempt=batch_attempt,
                started_at=batch_started_at,
                finished_at=_utc_now(),
            )
            _append_batch_status_row(ledger_path, row)
            _write_run_report(location, ledger_path)
            sys.exit(1)

        nf_cmd = _build_nextflow_command(
            nf_main=nf_main,
            batch_params_path=batch_params_path,
            batch_info=batch_info,
            resume=args.resume,
            diagnostics_mode=diagnostics_mode,
        )
        pretty_print(
            "Running batch {}: {}".format(batch_info["batch_id"], " ".join(nf_cmd))
        )
        result = subprocess.run(nf_cmd)
        if result.returncode != 0:
            message = "Batch {} failed (exit {}). Fix and re-run with --resume.".format(
                batch_info["batch_id"], result.returncode
            )
            pretty_print(message)
            row = _build_batch_status_row(
                batch_info,
                cleanup_policy=cleanup_policy,
                nextflow_status="failed",
                stage_out_status="failed",
                cleanup_status="not_requested",
                scratch_before_bytes=current_quota.available,
                scratch_after_bytes=current_quota.available,
                message=message,
                params_path=batch_params_path,
                attempt=batch_attempt,
                started_at=batch_started_at,
                finished_at=_utc_now(),
            )
            _append_batch_status_row(ledger_path, row)
            if continue_on_batch_failure:
                final_exit_code = final_exit_code or result.returncode or 1
                continue
            _write_run_report(location, ledger_path)
            sys.exit(result.returncode)

        (
            stage_out_status,
            stage_out_message,
            evidence_paths,
        ) = _read_stage_out_verification(batch_info["batch_work_dir"])
        cleanup_status = "not_requested"
        should_cleanup = False

        if cleanup_policy == "keep":
            cleanup_status = "kept"
        elif cleanup_policy == "success":
            if stage_out_status in {"failed", "unverified"}:
                cleanup_status = "not_eligible"
            else:
                should_cleanup = True
        elif cleanup_policy == "verified":
            if stage_out_status == "verified":
                should_cleanup = True
            else:
                cleanup_status = "not_eligible"
        else:
            raise ValueError("Unsupported cleanup policy: {}".format(cleanup_policy))

        if should_cleanup:
            try:
                pretty_print(
                    "Cleaning batch {} work dir".format(batch_info["batch_id"])
                )
                _cleanup_batch_work_dir(
                    batch_info["batch_work_dir"], os.path.join(location, "work")
                )
                cleanup_status = "cleaned"
            except Exception as exc:
                cleanup_status = "failed"
                message = "{}; cleanup failed: {}".format(stage_out_message, exc)
                row = _build_batch_status_row(
                    batch_info,
                    cleanup_policy=cleanup_policy,
                    nextflow_status="success",
                    stage_out_status=stage_out_status,
                    cleanup_status=cleanup_status,
                    scratch_before_bytes=current_quota.available,
                    scratch_after_bytes=current_quota.available,
                    message=message,
                    params_path=batch_params_path,
                    attempt=batch_attempt,
                    started_at=batch_started_at,
                    finished_at=_utc_now(),
                )
                _append_batch_status_row(ledger_path, row)
                if continue_on_batch_failure:
                    final_exit_code = final_exit_code or 1
                    continue
                _write_run_report(location, ledger_path)
                sys.exit(1)

        if cleanup_policy == "verified" and stage_out_status != "verified":
            message = stage_out_message
            if evidence_paths:
                message = "{}; evidence={}".format(
                    stage_out_message, ", ".join(str(path) for path in evidence_paths)
                )
            pretty_print(
                (
                    "Batch {} is not verified; preserving work and stopping "
                    "before the next batch"
                ).format(batch_info["batch_name"])
            )
            row = _build_batch_status_row(
                batch_info,
                cleanup_policy=cleanup_policy,
                nextflow_status="success",
                stage_out_status=stage_out_status,
                cleanup_status=cleanup_status,
                scratch_before_bytes=current_quota.available,
                scratch_after_bytes=batch_module.get_scratch_quota(config_quota).available,
                message=message,
                params_path=batch_params_path,
                attempt=batch_attempt,
                started_at=batch_started_at,
                finished_at=_utc_now(),
            )
            _append_batch_status_row(ledger_path, row)
            if continue_on_batch_failure:
                final_exit_code = final_exit_code or 1
                continue
            _write_run_report(location, ledger_path)
            sys.exit(1)

        if cleanup_policy == "success" and stage_out_status in {"failed", "unverified"}:
            message = stage_out_message
            pretty_print(
                (
                    "Batch {} is not cleanable because stage-out is {}; "
                    "stopping before the next batch"
                ).format(batch_info["batch_name"], stage_out_status)
            )
            row = _build_batch_status_row(
                batch_info,
                cleanup_policy=cleanup_policy,
                nextflow_status="success",
                stage_out_status=stage_out_status,
                cleanup_status=cleanup_status,
                scratch_before_bytes=current_quota.available,
                scratch_after_bytes=batch_module.get_scratch_quota(config_quota).available,
                message=message,
                params_path=batch_params_path,
                attempt=batch_attempt,
                started_at=batch_started_at,
                finished_at=_utc_now(),
            )
            _append_batch_status_row(ledger_path, row)
            if continue_on_batch_failure:
                final_exit_code = final_exit_code or 1
                continue
            _write_run_report(location, ledger_path)
            sys.exit(1)

        if cleanup_policy == "keep" and stage_out_status in {"failed", "unverified"}:
            message = stage_out_message
            pretty_print(
                (
                    "Batch {} is not verified; preserving work and stopping "
                    "before the next batch"
                ).format(batch_info["batch_name"])
            )
            row = _build_batch_status_row(
                batch_info,
                cleanup_policy=cleanup_policy,
                nextflow_status="success",
                stage_out_status=stage_out_status,
                cleanup_status=cleanup_status,
                scratch_before_bytes=current_quota.available,
                scratch_after_bytes=batch_module.get_scratch_quota(config_quota).available,
                message=message,
                params_path=batch_params_path,
                attempt=batch_attempt,
                started_at=batch_started_at,
                finished_at=_utc_now(),
            )
            _append_batch_status_row(ledger_path, row)
            if continue_on_batch_failure:
                final_exit_code = final_exit_code or 1
                continue
            _write_run_report(location, ledger_path)
            sys.exit(1)

        message = stage_out_message
        if evidence_paths:
            message = "{}; evidence={}".format(
                stage_out_message, ", ".join(str(path) for path in evidence_paths)
            )
        row = _build_batch_status_row(
            batch_info,
            cleanup_policy=cleanup_policy,
            nextflow_status="success",
            stage_out_status=stage_out_status,
            cleanup_status=cleanup_status,
            scratch_before_bytes=current_quota.available,
            scratch_after_bytes=batch_module.get_scratch_quota(config_quota).available,
            message=message,
            params_path=batch_params_path,
            attempt=batch_attempt,
            started_at=batch_started_at,
            finished_at=_utc_now(),
        )
        _append_batch_status_row(ledger_path, row)

    report_path = _write_run_report(location, ledger_path)
    pretty_print("Run report: {}".format(report_path))
    if final_exit_code:
        sys.exit(final_exit_code)
    pretty_print("All batches complete!")


def _write_batch_params(base_params_path, batch_info, location):
    """Write params for one scratch-safe plate batch."""
    with open(base_params_path) as f:
        params = json.load(f)
    params.update(
        diagnostics_flags_for_mode(batch_info.get("diagnostics_mode", "full"))
    )
    if batch_info.get("plates") is not None:
        params["plates"] = batch_info["plates"]
    batch_id = batch_info["batch_id"]
    params["batch_id"] = batch_id
    params["batch_name"] = batch_info["batch_name"]
    params["batch_work_dir"] = batch_info["batch_work_dir"]
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


def cmd_deepprofiler_package(args):
    """Build a DeepProfiler input package from chunk and location CSVs."""
    from cptools2 import nextflow_chunking

    nextflow_chunking.build_deepprofiler_input_package(
        chunk_manifest=args.chunk_manifest,
        locations_dir=args.locations_dir,
        output_root=args.output_root,
        config_path=args.config_path,
    )


def cmd_export_features(args):
    """Export native feature outputs to measurement tables."""
    from cptools2.feature_export.contract import (
        FeatureExportRequest,
        normalise_formats,
    )
    from cptools2.feature_export.deepprofiler import export_deepprofiler

    if args.extractor != "deepprofiler":
        raise ValueError("Unsupported feature extractor: {}".format(args.extractor))

    request = FeatureExportRequest(
        extractor=args.extractor,
        features_dir=Path(args.features_dir),
        output_dir=Path(args.output_dir),
        formats=normalise_formats(args.formats),
        nan_object_fail_fraction=args.nan_object_fail_fraction,
        plate_id=args.plate_id,
        run_label=args.run_label,
    )
    result = export_deepprofiler(request)
    print(
        (
            "Exported {extractor}: manifest={manifest} sites={sites} "
            "cells={cells} quality={quality}"
        ).format(
            extractor=result.extractor,
            manifest=result.manifest_rows,
            sites=result.site_rows,
            cells=result.cell_rows,
            quality=result.quality_rows,
        )
    )


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
        "--nextflow-diagnostics",
        choices=["full", "minimal", "off"],
        default=None,
        help="Nextflow observer mode: full, minimal, or off",
    )
    p_pipeline.add_argument(
        "--cleanup-policy",
        choices=["keep", "success", "verified"],
        default="verified",
        help="batch cleanup policy: keep, success, or verified",
    )
    p_pipeline.add_argument(
        "--clean-work",
        action="store_true",
        default=False,
        help="remove batch work dirs after successful Nextflow batches",
    )
    p_pipeline.add_argument(
        "--continue-on-batch-failure",
        action="store_true",
        default=False,
        help="record batch failures and continue independent later batches",
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
    p_join.add_argument("--location", required=True, help="path to results directory")
    p_join.add_argument(
        "--patterns",
        nargs="+",
        required=True,
        help="filename patterns to match (e.g. Image.csv Cells.csv)",
    )
    p_join.set_defaults(func=cmd_join)

    # --- deepprofiler-package ---
    p_deepprofiler = subparsers.add_parser(
        "deepprofiler-package",
        help="build DeepProfiler inputs from Nextflow chunk and location CSVs",
    )
    p_deepprofiler.add_argument("--chunk-manifest", required=True)
    p_deepprofiler.add_argument("--locations-dir", required=True)
    p_deepprofiler.add_argument("--output-root", required=True)
    p_deepprofiler.add_argument("--config-path", required=True)
    p_deepprofiler.set_defaults(func=cmd_deepprofiler_package)

    # --- export-features ---
    p_export_features = subparsers.add_parser(
        "export-features",
        help="export native feature outputs to measurement tables",
    )
    p_export_features.add_argument(
        "features_dir",
        help="directory containing native feature outputs",
    )
    p_export_features.add_argument(
        "--extractor",
        choices=["deepprofiler"],
        required=True,
        help="feature extractor that produced the native outputs",
    )
    p_export_features.add_argument(
        "--output-dir",
        required=True,
        help="directory where feature tables should be written",
    )
    p_export_features.add_argument(
        "--formats",
        nargs="+",
        default=["csv"],
        choices=["csv", "parquet"],
        help="output formats: csv or csv parquet",
    )
    p_export_features.add_argument(
        "--nan-object-fail-fraction",
        type=float,
        default=0.05,
        help=(
            "site-level quality gate fail threshold for fraction of objects "
            "with any NaN feature values"
        ),
    )
    p_export_features.add_argument(
        "--plate-id",
        default=None,
        help="plate identifier used for published aliases",
    )
    p_export_features.add_argument(
        "--run-label",
        default=None,
        help="optional run label appended to published aliases",
    )
    p_export_features.set_defaults(func=cmd_export_features)

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
