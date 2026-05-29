from pathlib import Path

from scripts import collect_deepprofiler_evidence


def test_collect_trace_summary_counts_feature_statuses(tmp_path):
    trace = tmp_path / "trace.txt"
    trace.write_text(
        "\t".join(["task_id", "process", "status", "exit", "realtime", "peak_rss"]) + "\n"
        + "\t".join(["1", "FEATURE_EXTRACT (chunk_0001)", "COMPLETED", "0", "1m 2s", "160 GB"]) + "\n"
        + "\t".join(["2", "FEATURE_EXTRACT (chunk_0002)", "FAILED", "1", "10s", "4 GB"]) + "\n"
        + "\t".join(["3", "CELLPOSE_SEGMENT (chunk_0001)", "COMPLETED", "0", "9s", "2 GB"]) + "\n"
        + "\t".join(["4", "CELLPOSE_SEGMENT (chunk_0002)", "FAILED", "1", "8s", "2 GB"]) + "\n",
        encoding="utf-8",
    )

    summary = collect_deepprofiler_evidence.collect_trace_summary(trace)

    assert summary["feature_total"] == 2
    assert summary["feature_completed"] == 1
    assert summary["feature_failed"] == 1
    assert summary["feature_failed_attempts"] == 1
    assert summary["cellpose_total"] == 2
    assert summary["cellpose_completed"] == 1
    assert summary["cellpose_failed"] == 1
    assert summary["cellpose_failed_attempts"] == 1


def test_first_error_line_finds_cudnn_signature(tmp_path):
    log = tmp_path / "nested" / ".command.err"
    log.parent.mkdir()
    log.write_text(
        "noise\n"
        "Could not create cudnn handle: CUDNN_STATUS_INTERNAL_ERROR\n"
        "more noise\n",
        encoding="utf-8",
    )

    assert collect_deepprofiler_evidence.first_error_line(tmp_path) == (
        "Could not create cudnn handle: CUDNN_STATUS_INTERNAL_ERROR"
    )


def test_first_error_line_finds_bus_error_case_insensitively(tmp_path):
    log = tmp_path / ".command.err"
    log.write_text(
        ".command.sh: line 43: 102 Bus error (core dumped) python -m cellpose\n",
        encoding="utf-8",
    )

    assert "Bus error" in collect_deepprofiler_evidence.first_error_line(tmp_path)


def test_output_counts_detect_npz_and_masks(tmp_path):
    (tmp_path / "features").mkdir()
    (tmp_path / "features" / "a.npz").write_text("x", encoding="utf-8")
    (tmp_path / "masks").mkdir()
    (tmp_path / "masks" / "mask.tiff").write_text("x", encoding="utf-8")
    (tmp_path / "masks" / "another_mask.TIF").write_text("x", encoding="utf-8")
    (tmp_path / "masks" / "no_cells.tsv").write_text("x", encoding="utf-8")

    counts = collect_deepprofiler_evidence.output_counts(tmp_path)

    assert counts == {"npz": 1, "masks": 2, "no_cells": 1}


def test_missing_trace_returns_zeros(tmp_path):
    summary = collect_deepprofiler_evidence.collect_trace_summary(tmp_path / "trace.txt")

    assert summary == {
        "feature_total": 0,
        "feature_completed": 0,
        "feature_failed": 0,
        "feature_failed_attempts": 0,
        "cellpose_total": 0,
        "cellpose_completed": 0,
        "cellpose_failed": 0,
        "cellpose_failed_attempts": 0,
    }


def test_trace_summary_matches_qualified_and_suffixed_process_names(tmp_path):
    trace = tmp_path / "trace.txt"
    trace.write_text(
        "\t".join(["task_id", "process", "status"]) + "\n"
        + "\t".join(["1", "workflow:FEATURE_EXTRACT", "COMPLETED"]) + "\n"
        + "\t".join(["2", "CELLPOSE_SEGMENT (1)", "FAILED"]) + "\n",
        encoding="utf-8",
    )

    summary = collect_deepprofiler_evidence.collect_trace_summary(trace)

    assert summary["feature_total"] == 1
    assert summary["feature_completed"] == 1
    assert summary["cellpose_total"] == 1
    assert summary["cellpose_failed"] == 1


def test_trace_summary_accepts_nextflow_name_column(tmp_path):
    trace = tmp_path / "trace.txt"
    trace.write_text(
        "\t".join(["task_id", "name", "status"]) + "\n"
        + "\t".join(["1", "FEATURE_EXTRACT (plate:chunk_0001)", "COMPLETED"]) + "\n"
        + "\t".join(["2", "CELLPOSE_SEGMENT (plate:chunk_0001)", "COMPLETED"]) + "\n",
        encoding="utf-8",
    )

    summary = collect_deepprofiler_evidence.collect_trace_summary(trace)

    assert summary["feature_total"] == 1
    assert summary["feature_completed"] == 1
    assert summary["cellpose_total"] == 1
    assert summary["cellpose_completed"] == 1


def test_trace_summary_counts_cached_and_retried_tasks_as_final_success(tmp_path):
    trace = tmp_path / "trace.txt"
    trace.write_text(
        "\t".join(["task_id", "name", "status"]) + "\n"
        + "\t".join(["1", "FEATURE_EXTRACT (plate:chunk_0001)", "FAILED"]) + "\n"
        + "\t".join(["2", "FEATURE_EXTRACT (plate:chunk_0001)", "COMPLETED"]) + "\n"
        + "\t".join(["3", "CELLPOSE_SEGMENT (plate:chunk_0001)", "CACHED"]) + "\n",
        encoding="utf-8",
    )

    summary = collect_deepprofiler_evidence.collect_trace_summary(trace)

    assert summary["feature_total"] == 1
    assert summary["feature_completed"] == 1
    assert summary["feature_failed"] == 0
    assert summary["feature_failed_attempts"] == 1
    assert summary["cellpose_total"] == 1
    assert summary["cellpose_completed"] == 1


def test_render_markdown_includes_key_sections(tmp_path):
    summary = collect_deepprofiler_evidence.collect(tmp_path)

    markdown = collect_deepprofiler_evidence.render_markdown(summary)

    assert "## Trace" in markdown
    assert "## Outputs" in markdown
    assert "## First Error" in markdown


def test_cli_writes_json_and_markdown(tmp_path):
    run_root = tmp_path / "run"
    run_root.mkdir()
    (run_root / "trace.txt").write_text(
        "\t".join(["task_id", "process", "status"]) + "\n"
        + "\t".join(["1", "FEATURE_EXTRACT", "COMPLETED"]) + "\n",
        encoding="utf-8",
    )

    json_out = tmp_path / "out" / "summary.json"
    md_out = tmp_path / "out" / "summary.md"

    exit_code = collect_deepprofiler_evidence.main(
        [str(run_root), "--json-out", str(json_out), "--md-out", str(md_out)]
    )

    assert exit_code == 0
    assert json_out.exists()
    assert md_out.exists()


def test_collect_finds_cptools_trace_under_outputs_traces(tmp_path):
    trace = tmp_path / "outputs" / "traces" / "trace.batch_001.txt"
    trace.parent.mkdir(parents=True)
    trace.write_text(
        "\t".join(["task_id", "process", "status"]) + "\n"
        + "\t".join(["1", "FEATURE_EXTRACT", "COMPLETED"]) + "\n",
        encoding="utf-8",
    )

    summary = collect_deepprofiler_evidence.collect(tmp_path)

    assert summary["trace"]["feature_total"] == 1
    assert summary["trace"]["feature_completed"] == 1
