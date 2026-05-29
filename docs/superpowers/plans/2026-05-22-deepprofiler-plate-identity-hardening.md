# DeepProfiler Plate Identity Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent DeepProfiler export from silently relabeling wrong-plate payloads, record staged chunk routing, and make multi-plate export failures visible without stopping unrelated plate exports.

**Architecture:** Keep DeepProfiler payload paths as the observed measurement identity, treat caller `plate_id` as an assertion checked before publication, and extend the existing export staging helper to write a compact join manifest for traceability. Plate-local export validation failures become explicit status and README artifacts instead of silent missing tables, while a run-level summary compares expected plate ids with observed export statuses so the pipeline can complete successfully with visible partial exports.

**Tech Stack:** Python, pytest, Nextflow architecture smoke tests, CSV export contracts.

---

## File Structure

- Modify `cptools2/feature_export/join.py` to record staged chunk routing in an export input manifest while preserving collision-proof staging behavior.
- Modify `nextflow/modules/export_features.nf` to pass plate identity into
  staging and preserve the staged manifest with exported feature artifacts.
- Modify `cptools2/feature_export/deepprofiler.py` to separate payload-derived identity from requested plate validation.
- Create `cptools2/feature_export/reports.py` to write per-plate status and
  README artifacts plus a run-level summary for success, recorded failure, and
  missing-status plates.
- Modify `tests/test_feature_export_join.py` for manifest behavior.
- Modify `tests/test_feature_export_deepprofiler.py` for plate identity validation and alias preservation.
- Create `tests/test_feature_export_reports.py` for per-plate and run summary
  artifacts.
- Create `nextflow/modules/summarise_feature_exports.nf` for the run-level
  summary task.
- Modify `tests/test_nextflow_architecture_smoke.py` for the updated staging handoff.
- Update documentation only if implementation changes the payload contract wording beyond the design in `docs/superpowers/specs/2026-05-22-deepprofiler-plate-identity-hardening-design.md`.

### Task 1: Record Staged Plate And Chunk Routing

**Files:**
- Modify: `cptools2/feature_export/join.py`
- Modify: `nextflow/modules/export_features.nf`
- Test: `tests/test_feature_export_join.py`
- Test: `tests/test_nextflow_architecture_smoke.py`

- [ ] **Step 1: Write failing staging tests for the manifest**

Add tests proving the join manifest is written alongside staged payloads and
keeps chunk-scoped paths:

```python
def test_stage_feature_payloads_writes_plate_join_manifest():
    staged = stage_feature_payloads(
        plate_id="plate-001",
        chunk_ids=["plate-001_chunk_0001"],
        feature_dirs=[payload],
        export_input_dir=export_input_dir,
    )

    rows = list(csv.DictReader((export_input_dir / "export_input_manifest.csv").open()))
    assert rows == [
        {
            "plate_id": "plate-001",
            "chunk_id": "plate-001_chunk_0001",
            "source_feature_dir": payload.as_posix(),
            "staged_feature_dir": staged[0].as_posix(),
        }
    ]
```

Add a guard test so the staged join cannot emit an unauditable manifest:

```python
def test_stage_feature_payloads_rejects_empty_plate_id_before_copy():
    with pytest.raises(ValueError, match="plate_id is required"):
        stage_feature_payloads(
            plate_id="",
            chunk_ids=["plate-001_chunk_0001"],
            feature_dirs=[payload],
            export_input_dir=export_input_dir,
        )

    assert not export_input_dir.exists()
```

Update existing staging calls in `tests/test_feature_export_join.py` with the
new `plate_id` argument and preserve duplicate, missing payload, and stale rerun
assertions.

- [ ] **Step 2: Run the staging test subset and confirm failure**

Run:

```bash
pytest tests/test_feature_export_join.py tests/test_nextflow_architecture_smoke.py::test_nextflow_feature_export_is_cpu_only_and_plate_grouped -q
```

Expected: fail because `stage_feature_payloads()` does not accept `plate_id`,
does not write `export_input_manifest.csv`, and the Nextflow module does not
pass plate identity into staging yet.

- [ ] **Step 3: Implement manifest staging**

Extend the staging helper signature and write the manifest after payload copies
are staged:

```python
def stage_feature_payloads(
    *,
    plate_id: object,
    chunk_ids: Sequence[object],
    feature_dirs: Sequence[object],
    export_input_dir: Path,
) -> Tuple[Path, ...]:
    ...
```

Use `csv.DictWriter` with stable columns:

```python
MANIFEST_COLUMNS = (
    "plate_id",
    "chunk_id",
    "source_feature_dir",
    "staged_feature_dir",
)
```

Reject a normalized empty `plate_id` before any copy happens. The manifest rows
should use the normalized `plate_id`, normalized chunk id, source payload path,
and staged payload path. Keep the existing validation order so empty plate ids
and duplicate chunk ids fail before copies occur.

- [ ] **Step 4: Pass the plate group into staging from Nextflow**

Update the inline Python call in `nextflow/modules/export_features.nf`:

```python
stage_feature_payloads(
    plate_id='''${plate_id}''',
    chunk_ids=chunk_ids,
    feature_dirs=feature_dirs,
    export_input_dir=Path("export_input"),
)
```

After the exporter succeeds, preserve the join evidence in the published
feature artifact tree:

```bash
cp export_input/export_input_manifest.csv features/export_input_manifest.csv
```

Extend `tests/test_nextflow_architecture_smoke.py` with assertions that the
staging call includes `plate_id=`, the staged manifest is copied into the
published feature export artifact tree, and the exporter still receives:

```text
--plate-id ${plate_id}
```

- [ ] **Step 5: Re-run the staging test subset**

Run:

```bash
pytest tests/test_feature_export_join.py tests/test_nextflow_architecture_smoke.py::test_nextflow_feature_export_is_cpu_only_and_plate_grouped -q
```

Expected: pass.

### Task 2: Validate Payload Identity Before Publication

**Files:**
- Modify: `cptools2/feature_export/deepprofiler.py`
- Test: `tests/test_feature_export_deepprofiler.py`

- [ ] **Step 1: Write failing exporter tests for wrong and mixed plate inputs**

Add focused tests:

```python
def test_export_deepprofiler_rejects_requested_plate_id_mismatch():
    path = features_dir / "plate-B" / "A01" / "1.npz"
    path.parent.mkdir(parents=True)
    np.savez(path, features=np.array([1.0, 2.0], dtype="float32"))

    with pytest.raises(
        ValueError,
        match="Requested plate_id plate-A does not match exported plate",
    ):
        export_deepprofiler(
            FeatureExportRequest(
                extractor="deepprofiler",
                features_dir=features_dir,
                output_dir=output_dir,
                formats=("csv",),
                plate_id="plate-A",
            )
        )
```

```python
def test_export_deepprofiler_rejects_mixed_observed_plates_with_request():
    for plate in ["plate-A", "plate-B"]:
        path = features_dir / plate / "A01" / "1.npz"
        path.parent.mkdir(parents=True)
        np.savez(path, features=np.array([1.0, 2.0], dtype="float32"))

    with pytest.raises(ValueError, match="Expected one plate in feature export input"):
        export_deepprofiler(
            FeatureExportRequest(
                extractor="deepprofiler",
                features_dir=features_dir,
                output_dir=output_dir,
                formats=("csv",),
                plate_id="plate-A",
            )
        )
```

Add a malformed path-contract test so flattened inputs fail explicitly instead
of producing a misleading inferred plate id from surrounding staging folders:

```python
def test_export_deepprofiler_rejects_payload_without_plate_well_site_shape():
    path = features_dir / "1.npz"
    path.parent.mkdir(parents=True, exist_ok=True)
    np.savez(path, features=np.array([1.0, 2.0], dtype="float32"))

    with pytest.raises(
        ValueError,
        match="Expected DeepProfiler payload path features/<plate>/<well>/<site>.npz",
    ):
        export_deepprofiler(
            FeatureExportRequest(
                extractor="deepprofiler",
                features_dir=features_dir,
                output_dir=output_dir,
                formats=("csv",),
                plate_id="plate-A",
            )
        )
```

Retain or extend the matching alias test so it proves canonical cell rows keep
the observed `Metadata_Plate` when `plate_id="plate-001"` matches.

- [ ] **Step 2: Run the exporter subset and confirm failure**

Run:

```bash
pytest tests/test_feature_export_deepprofiler.py -q
```

Expected: the wrong-plate test fails because requested `plate_id` is injected
into metadata before `_resolve_plate_id()` validates observed payload plates.

- [ ] **Step 3: Remove the pre-validation plate override**

Change the exporter scan so payload metadata is always inferred from the NPZ
path:

```python
metadata = _metadata_from_npz_path(path)
```

Keep `_metadata_from_npz_path()` focused on the native DeepProfiler path shape,
reject paths that do not have the expected `plate/well/site.npz` suffix under
the feature tree, and remove the optional override argument if it no longer has
a caller.

- [ ] **Step 4: Keep `_resolve_plate_id()` as the validation gate**

Ensure `_resolve_plate_id()` receives payload-derived inspected metadata,
rejects mixed observed plates before returning any requested plate id, and
raises a clear requested mismatch error when observed and requested identities
disagree.

The validation rule remains:

```python
if requested_plate_id and inferred and inferred != [requested]:
    raise ValueError(...)
```

but now `inferred` must be populated only from observed payload metadata.

- [ ] **Step 5: Re-run the exporter subset**

Run:

```bash
pytest tests/test_feature_export_deepprofiler.py -q
```

Expected: pass, including matching alias behavior and new mismatch rejection.

### Task 3: Turn Plate Export Outcomes Into User-Facing Artifacts

**Files:**
- Modify: `cptools2/feature_export/deepprofiler.py`
- Create: `cptools2/feature_export/reports.py`
- Modify: `nextflow/modules/export_features.nf`
- Modify: `nextflow/main.nf`
- Create: `nextflow/modules/summarise_feature_exports.nf`
- Test: `tests/test_feature_export_reports.py`
- Test: `tests/test_nextflow_architecture_smoke.py`

- [ ] **Step 1: Write failing per-plate status tests**

Add tests for a successful export report and a strict failed export report. The
success case must assert that status and README point to available tables. The
failure case must assert that wrong-plate validation produces:

```text
export_status = failed
tables_present = false
failure_stage = plate_identity_validation
```

and that no measurement table aliases are published for that failed plate.

Use concrete helper calls in `tests/test_feature_export_reports.py`, for example:

```python
status_path, readme_path = write_plate_export_report(
    output_dir=plate_output,
    plate_id="plate-001",
    export_status="failed",
    failure_stage="plate_identity_validation",
    failure_reason="Requested plate_id plate-001 does not match exported plate(s): plate-002",
    tables_present=False,
    manifest_path="export_input_manifest.csv",
)
```

- [ ] **Step 2: Write failing run summary tests**

Add a report helper test that passes expected plate ids plus observed statuses
and writes:

```text
feature_export_summary.csv
README.md
```

Cover three rows:

```text
plate-success -> exported
plate-failed -> failed with recorded reason
plate-missing -> missing_status
```

The README assertion should check that failed and missing-status plate ids are
named explicitly.

Use `write_feature_export_summary()` in
`tests/test_feature_export_reports.py` so the helper boundary is testable
without Nextflow.

- [ ] **Step 3: Run report tests and confirm failure**

Run:

```bash
pytest tests/test_feature_export_reports.py tests/test_nextflow_architecture_smoke.py::test_nextflow_feature_export_is_cpu_only_and_plate_grouped -q
```

Expected: fail because the exporter does not yet preserve a per-plate export
status and the workflow has no run-level export summary process.

- [ ] **Step 4: Implement per-plate status and README artifacts**

Add `cptools2/feature_export/reports.py` with small feature-export report
helpers that write a machine-readable status row and README for each plate
export. Expected validation failures must produce a failed plate artifact and
return control to the plate task without publishing measurement tables.

Per-plate status columns:

```python
PLATE_EXPORT_STATUS_COLUMNS = (
    "plate_id",
    "export_status",
    "failure_stage",
    "failure_reason",
    "tables_present",
    "manifest_path",
)
```

Keep the strict exporter validation intact. The report wrapper records the
failure; it does not trust `plate_id` to manufacture rows. Update
`nextflow/modules/export_features.nf` so its plate export orchestration writes a
success report after tables validate and a failed report when exporter
validation raises a plate-local identity or payload error.

- [ ] **Step 5: Add a run-level summary process**

Create `nextflow/modules/summarise_feature_exports.nf` and wire it from
`nextflow/main.nf` so expected plate ids and observed export status artifacts
produce a run summary for all plates. The summary must preserve one row per
expected plate and mark plates without an observed status as `missing_status`.

The summary output should be available in ordinary non-Eddie results and stage
out when DataStore staging is enabled:

```text
feature_export_summary/feature_export_summary.csv
feature_export_summary/README.md
```

- [ ] **Step 6: Keep unrelated plate exports alive**

Plan the Nextflow failure policy explicitly:

- expected validation errors should be caught inside plate export reporting so
  each plate emits a failed status artifact;
- unexpected `EXPORT_FEATURES` task crashes should not prevent other plate
  export tasks from finishing;
- the summary must show `missing_status` for any expected plate whose task
  produced no status.

Use an `EXPORT_FEATURES`-specific Nextflow failure policy and architecture tests
to keep the whole run successful for ignored export-task crashes while
preserving explicit status data for the normal validation-failure path. Keep
the summary process aware of the expected plate roster so missing status
artifacts remain visible.

- [ ] **Step 7: Re-run report and architecture tests**

Run:

```bash
pytest tests/test_feature_export_reports.py tests/test_nextflow_architecture_smoke.py -q
```

Expected: pass.

### Task 4: Verify The Hardened Export Boundary

**Files:**
- Verify: `cptools2/feature_export/join.py`
- Verify: `cptools2/feature_export/deepprofiler.py`
- Verify: `nextflow/modules/export_features.nf`
- Verify: touched test files

- [ ] **Step 1: Run the focused export and architecture suites**

Run:

```bash
pytest tests/test_feature_export_contract.py tests/test_feature_export_join.py tests/test_feature_export_deepprofiler.py tests/test_feature_export_reports.py tests/test_nextflow_architecture_smoke.py -q
```

Expected: pass.

- [ ] **Step 2: Run CLI coverage for the preserved `plate_id` interface**

Run:

```bash
pytest tests/test_cli.py -k test_export_features_dispatches_to_deepprofiler_exporter -q
```

Expected: pass.

- [ ] **Step 3: Check the diff for contract drift**

Inspect the diff and confirm:

- canonical table names remain unchanged;
- plate-aware aliases remain unchanged after validation;
- the manifest is not used to overwrite `Metadata_Plate`;
- the manifest is preserved with exported feature artifacts for later evidence;
- failed plate exports have status/readme artifacts but no measurement tables;
- the run summary explains failed and missing-status plates;
- no recovery or trust fallback slipped into the normal exporter path.

- [ ] **Step 4: Update reference docs only if implementation changes documented behavior**

If user-facing exporter behavior or the expected DeepProfiler payload shape
needs an explicit reference note, add a concise note under the relevant feature
export or DeepProfiler reference docs. Do not rewrite Phase 2.9 evidence.

- [ ] **Step 5: Commit the hardened boundary**

Stage the touched implementation, tests, and any documentation changes:

```bash
git add cptools2/feature_export/join.py cptools2/feature_export/deepprofiler.py cptools2/feature_export/reports.py nextflow/main.nf nextflow/modules/export_features.nf nextflow/modules/summarise_feature_exports.nf tests/test_feature_export_join.py tests/test_feature_export_deepprofiler.py tests/test_feature_export_reports.py tests/test_nextflow_architecture_smoke.py docs
git commit -m "fix: harden deepprofiler plate export identity"
```

## Execution Notes

- Keep this repair local-first. The clean Eddie Phase 2.9 full-plate evidence
  already proves the full route after the join collision repair.
- Re-run Eddie only if implementation changes the runtime payload shape or
  Nextflow grouping contract in a way local architecture and exporter tests do
  not cover.
- Keep the separate Parquet memory follow-up out of this repair.
