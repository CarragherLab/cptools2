# Seqera Eddie Scratch Batching Plan

Status: proposed
Date: 2026-04-29
Scope: cptools2 Nextflow Eddie execution, scratch-safe batching, reproducibility

## Why This Matters

Seqera's nf-core guidance describes the same operational problem cptools2 is solving:
Eddie scratch is finite, DataStore access is constrained, and long-running analysis
needs to be recoverable without hand-cleaning a half-failed run.

cptools2 already has the right broad direction. The CLI prepares batch-specific
params, Nextflow runs through SGE, and DataStore movement is isolated in staging
processes. The missing piece is making each batch a fully isolated, auditable run
unit with its own work directory, reports, cleanup policy, and scratch-size
calibration while keeping user-facing outputs flat and simple.

## Premise Challenge

The tempting interpretation is: "Seqera recommends an external wrapper, so we
should add a shell wrapper around cptools2."

That is the wrong center of gravity for this project.

cptools2 already is the wrapper. The durable product should be a single command:

```bash
cptools2 pipeline config.yml --resume
```

That command should perform the same lifecycle Seqera recommends:

1. discover or load per-plate sizes
2. create scratch-safe batches
3. run Nextflow once per batch
4. isolate each batch's work directory and provenance files
5. emit provenance artifacts
6. optionally clean finished work
7. stop cleanly on failure with enough state to resume

The lesson is not "copy the nf-core wrapper." The lesson is "make cptools2's
existing wrapper strict enough that users do not need to write one."

## Current State

Relevant files:

- `cptools2/batch.py` computes plate batches using a fixed 75% scratch utilisation
  target and 1.3x overhead factor.
- `cptools2/__main__.py` writes `params.batch_<id>.json` and invokes Nextflow once
  per batch.
- `nextflow/conf/eddie.config` uses SGE, `queueSize = 8`, staging labels, GPU queue
  configuration, and Singularity setup.
- `nextflow/modules/stage_in.nf` stages DataStore plate data to scratch through the
  staging queue.
- `nextflow/modules/stage_out.nf` copies final outputs back to the configured
  destination through the staging queue.

Main current weakness:

```python
"-work-dir",
os.path.join(location, "work"),
```

All batches currently share one Nextflow work root. This works, but it weakens the
operational model. It makes per-batch cleanup harder, muddles provenance, and makes
it less obvious which scratch usage belongs to which batch.

## Product Definition

Build "batch as reproducible run unit" into `cptools2 pipeline`.

For each batch, cptools2 should create:

```text
<run_root>/
  params.json
  params.batch_001.json
  traces/
    trace.batch_001.txt
    report.batch_001.html
    timeline.batch_001.html
  work/
    batch_001/
```

Outputs should remain flat under the configured `params.output_dir`:

```text
<output_dir>/
  plate_a/
  plate_b/
  plate_c/
```

The user experience should stay simple:

```bash
cptools2 pipeline config.yml --resume
```

Optional cleanup:

```bash
cptools2 pipeline config.yml --resume --clean-work
```

Dry run should show the complete batch plan:

```text
Batch 1
  plates: plate_a, plate_b
  estimated scratch: 842.3 GB
  work dir: /exports/eddie/scratch/$USER/cptools2/work/batch_001
  output dir: /exports/eddie/scratch/$USER/cptools2
  params: /exports/eddie/scratch/$USER/cptools2/params.batch_001.json
```

## Decisions

### 1. Keep Plate-Centric Batching

Seqera's example batches samplesheets by sample size. cptools2 should keep batching
by plate.

Reason: plate is the natural high-content imaging reproducibility unit. Plate
discovery, illumination correction, chunk manifests, stage-in, stage-out, and
downstream joins all align around plates.

### 2. Use Separate Work Directories Per Batch

Change batch invocation from:

```text
<location>/work
```

to:

```text
<location>/work/batch_001
```

This is the highest-value change. It makes scratch accounting, cleanup, and failure
recovery concrete.

### 3. Keep Outputs Flat

Do not add batch-specific output roots.

Each batch should preserve the configured `params.output_dir`. User-facing result
directories should stay plate-centric:

```text
<output_dir>/
  plate_a/
  plate_b/
  plate_c/
```

Reason: batched outputs confuse downstream joining, manual inspection, and
`STAGE_OUT`. Batch isolation belongs in `work/batch_###`, `params.batch_###.json`,
and `traces/*batch_###*`, not in the final result layout.

### 4. Emit Nextflow Provenance Per Batch

Add these flags to each Nextflow invocation:

```bash
-with-trace <location>/traces/trace.batch_001.txt
-with-report <location>/traces/report.batch_001.html
-with-timeline <location>/traces/timeline.batch_001.html
```

This gives us evidence for tuning memory, runtime, concurrency, and scratch factors.

### 5. Make Scratch Model Configurable

Current hard-coded model:

```text
utilisation_fraction = 0.75
overhead_factor = 1.3
```

Keep these as defaults, but expose them in config:

```yaml
scratch_utilisation_fraction: 0.75
scratch_work_factor: 1.3
```

Use `scratch_work_factor` rather than `overhead_factor` in user-facing config. The
Seqera concept is easier to understand: raw plate size multiplied by expected work
expansion.

Do not import nf-core's 3.5x blindly. Imaging may be lower or higher depending on
whether corrected images, masks, feature tensors, and published copies are retained.
Measure on Loop 230.

### 6. Add Optional Work Cleanup

Add:

```bash
--clean-work
```

Behavior:

- only runs after a batch exits successfully
- first tries `nextflow clean -f -work-dir <batch_work_dir>`
- if falling back to file removal, only removes a resolved path inside the configured
  work root
- never cleans failed batches

Default should be keep-work for now. Debuggability wins until the Eddie path is
validated end to end.

### 7. Throttle Disk-Heavy Stages Explicitly

Current global `queueSize = 8` is useful but blunt. Add process or label-level caps:

```groovy
withLabel: 'illum_apply' {
    maxForks = 4
}

withLabel: 'segmentation' {
    maxForks = 4
}

withLabel: 'feature_extract' {
    maxForks = 2
}

withLabel: 'staging' {
    maxForks = 2
}
```

Initial values should be conservative for Eddie validation. Tune from trace files
and `qacct`, not guesses.

### 8. Do Not Use `publishDir mode: 'move'` Yet

Seqera's `move` advice is correct for terminal outputs. It is risky in cptools2
because several published outputs are still consumed downstream.

Near-term rule:

- keep `copy` for intermediate products
- consider `move` only for terminal outputs after graph review
- prefer explicit stage-out plus per-batch work cleanup for now

## Alternatives Considered

### Alternative A: External Shell Wrapper

Build `run_batches.sh` like the Seqera example.

Rejected for now. It creates another thing users must understand and keep in sync
with cptools2. Useful as documentation, not as the primary interface.

### Alternative B: One Long Nextflow Run With Internal Batch Channels

Let Nextflow process all plates and rely on `queueSize`, `maxForks`, and stage-out.

Rejected for Eddie validation. It is elegant, but cleanup boundaries are vague. A
failed run also becomes harder to reason about when scratch is near quota.

### Alternative C: One Batch Per Plate

Simplest operational model.

Rejected as the default. It is safe but can underuse Eddie badly for small plates.
Keep it as an escape hatch via very conservative scratch config or explicit plate
lists.

## Implementation Plan

### Phase 1: Batch Isolation

Files:

- `cptools2/__main__.py`
- tests covering CLI dry-run and generated params

Tasks:

1. Generate zero-padded batch names, e.g. `batch_001`.
2. Use `<location>/work/<batch_name>` for `-work-dir`.
3. Create `<location>/traces`.
4. Add `-with-trace`, `-with-report`, and `-with-timeline`.
5. Show work, unchanged output, and trace paths during `--dry-run`.

Acceptance:

- dry run prints batch-specific work and trace paths
- non-dry run invokes Nextflow with batch-specific `-work-dir`
- existing single-batch configs still work

### Phase 2: Batch Metadata Provenance

Files:

- `cptools2/__main__.py`
- tests around `params.batch_<id>.json`

Tasks:

1. Preserve `params.output_dir` unchanged for every batch.
2. Add `batch_name` and `batch_work_dir` to params.
3. Document that work and traces are batch-scoped, while outputs are plate-scoped.

Acceptance:

- params file records batch metadata
- Nextflow modules continue to use `params.output_dir`
- stage-out still writes to the intended destination

### Phase 3: Scratch Model Knobs

Files:

- `cptools2/batch.py`
- `cptools2/__main__.py`
- `cptools2/parse_yaml.py`
- config docs

Tasks:

1. Add `scratch_utilisation_fraction`.
2. Add `scratch_work_factor`.
3. Validate both values are positive and sensible.
4. Include the model values in dry-run output.

Acceptance:

- defaults preserve current behavior
- config can override both values
- tests cover default and override behavior

### Phase 4: Cleanup

Files:

- `cptools2/__main__.py`
- tests for command construction and path safety

Tasks:

1. Add `--clean-work`.
2. Run cleanup only after successful batch completion.
3. Prefer `nextflow clean -f -work-dir <batch_work_dir>`.
4. Refuse fallback removal outside the configured work root.

Acceptance:

- failed batches are not cleaned
- successful batches are cleaned only with the flag
- path safety test prevents accidental broad deletion

### Phase 5: Eddie Tuning

Files:

- `nextflow/conf/eddie.config`
- docs for Eddie validation

Tasks:

1. Add conservative `maxForks` for staging and disk-heavy labels.
2. Run Loop 230 with one small batch.
3. Record `du -sh`, trace, report, timeline, and `qacct` values.
4. Tune scratch factor and concurrency from evidence.

Acceptance:

- first Eddie validation produces trace/report/timeline
- actual peak scratch is recorded
- next run uses measured scratch factor

## Test Plan

Local tests:

```bash
pytest tests
```

Focused tests to add:

- batch directory naming
- generated Nextflow command includes batch work dir
- generated params include batch metadata
- scratch factor override changes batch grouping
- cleanup does not run on failed batch
- cleanup path safety rejects paths outside work root

Eddie validation:

```bash
cptools2 pipeline /exports/eddie/scratch/$USER/cptools2-loop230/config/loop230-sarah-screen.yaml --dry-run
cptools2 pipeline /exports/eddie/scratch/$USER/cptools2-loop230/config/loop230-sarah-screen.yaml --resume
```

During the first real batch:

```bash
watch -n 60 du -sh /exports/eddie/scratch/$USER/cptools2-loop230/work/batch_001
qacct -j <job_id>
```

## Risks

### Risk: Batched Outputs Break Existing Consumers

Mitigation: do not implement batched outputs. Keep `params.output_dir` unchanged
for every batch and use batch-scoped work dirs plus trace files for reproducibility.

### Risk: Work Cleanup Deletes Useful Debug State

Mitigation: opt-in only. Never clean failed batches.

### Risk: Scratch Factor Is Still Wrong

Mitigation: treat the first Eddie run as calibration. Emit enough trace and path
data that adjustment is easy.

### Risk: `publishDir mode: 'move'` Looks Attractive But Breaks the Graph

Mitigation: do not adopt it until terminal outputs are formally identified.

## Recommended Next Action

Run an engineering review on this plan before coding:

```text
/plan-eng-review docs/plans/seqera-eddie-scratch-batching-plan.md
```

If approved, implement Phase 1 and Phase 3 first. Those give the most leverage:
clear batch boundaries and a configurable scratch model. Cleanup can wait until
the first Eddie validation has produced enough evidence.

## Loop 380 Calibration Notes

The flat-output layout is fixed, and the batch-scoped work layout is now the
operational model:

```text
<output_dir>/
  plate_a/
  plate_b/
  plate_c/

<run_root>/
  params.json
  params.batch_001.json
  traces/
    trace.batch_001.txt
    report.batch_001.html
    timeline.batch_001.html
  work/
    batch_001/
```

Local dry-run attempt on this Windows workspace:

```bash
python -m cptools2 pipeline config/loop230-sarah-screen.yaml --dry-run
```

That command fails before Nextflow starts because
`parse_yaml.generate_params_json` tries to create `C:\exports` from the config's
`/exports/...` paths and Windows returns `PermissionError: [WinError 5] Access is
denied`. No Eddie trace, report, timeline, `qacct`, or peak scratch data were
collected here, so the `scratch_work_factor` and `maxForks` values in this plan
remain provisional until an Eddie run records real measurements.

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope & strategy | 0 | not run | Not required for this infra/reproducibility plan |
| Codex Review | `/codex review` | Independent 2nd opinion | 0 | not run | Not required before implementation, useful before landing |
| Eng Review | `/plan-eng-review` | Architecture & tests (required) | 2 | clear | Second pass confirms plan matches discussed goals; 0 critical gaps |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | not applicable | Backend/CLI/HPC workflow only |
| DX Review | `/plan-devex-review` | Developer experience gaps | 0 | optional | Could review dry-run output after implementation |

### Eng Review Verdict

Status: **CLEARED FOR IMPLEMENTATION** after second review on 2026-05-01.

The plan is directionally right: keep cptools2 as the wrapper, make each batch a
reproducible run unit, isolate work directories, emit provenance, keep outputs
flat, and tune scratch from evidence. Do not start from an external shell wrapper.

The implementation constraints are listed below. User decisions after review:
oversized single plates are extremely unlikely for this workload and do not need
to block implementation; batched outputs confuse the workflow and are out of
scope; guarded deletion after successful batches is required because large staged
image files must be removed from scratch.

### Architecture Issues

1. **Oversized single plates are not handled. Deferred by user decision.**

   Current `create_batches()` will place a plate into a batch even if
   `plate_size * scratch_work_factor` exceeds the usable scratch limit. That creates
   a batch that is known to violate quota before it starts.

   Decision: do not block implementation on this. If added later, fail with a clear
   error listing plate name, estimated size, usable scratch, and suggested actions:
   increase scratch quota, lower stages, run from pre-staged scratch, or process a
   smaller subset.

2. **Batch isolation must not change result layout. Confirmed.**

   Work directory isolation is safe and should ship first. Output directory nesting
   changes user-visible result layout, complicates downstream joins, and interacts
   with `data_destination`.

   Required implementation rule: implement work-dir isolation, traces, and batch
   metadata only. Keep `params.output_dir` unchanged for every batch.

3. **Batched outputs are out of scope. Confirmed.**

   The project does not need `batch_output_mode`, `batch_output_dir`, or nested
   result directories. Batch identity should be visible in params, work dirs, and
   traces, not in final output layout.

   Decision rationale: final outputs are what users inspect, join, and destage.
   Keeping them flat avoids confusing result paths and preserves existing behavior.

4. **Cleanup fallback requires strict path safety. Confirmed.**

   The plan allows falling back from `nextflow clean` to direct directory removal.
   This is operationally important because large staged image files must be removed
   from scratch after each batch, matching the SGE-native workflow.

   Required implementation rule: direct deletion is allowed only after all of these checks pass:
   resolved target is the exact batch work directory, target is inside the configured
   work root, target path contains the expected `batch_###` component, and the batch
   completed successfully. Never delete on failed batches.

### Code Quality Issues

5. **Batch command construction should be extracted before adding more flags.**

   `cmd_pipeline()` currently builds the Nextflow command inline. Adding work dirs,
   trace/report/timeline, resume, and future cleanup will make it harder to test.

   Required implementation rule: add a small helper such as `_build_nextflow_command(...)` and
   unit-test the command list directly.

6. **Scratch sizing config belongs in the batch API, not only CLI glue.**

   `create_batches()` currently hard-codes `0.75` and `1.3`. The plan should make
   these arguments to `create_batches()` with defaults, then have CLI/config pass
   overrides.

   Required implementation rule: keep the sizing rules testable in `tests/test_batch.py`, not
   only via CLI integration tests.

### Test Review

Target data flow:

```text
config.yml
   |
   v
parse_yaml -> params.json
   |
   v
scratch preflight -> plate sizes -> batch plan
   |
   v
params.batch_001.json + work/batch_001 + traces/*
   |
   v
nextflow run -params-file params.batch_001.json -work-dir work/batch_001
   |
   v
success? -> optional nextflow clean
failure? -> stop, keep work dir, resume later
```

Required tests:

- batch command includes `work/batch_001`
- command includes trace, report, and timeline paths
- dry-run prints batch work and trace paths
- oversized-plate behavior is deferred by user decision
- scratch factor and utilisation overrides preserve default behavior when omitted
- failed batch does not trigger cleanup
- cleanup fallback only deletes validated batch work directories after success
- `--resume` still appends `-resume` to each batch command

Critical gap: none after user decision to defer oversized-plate handling.

### Performance Review

The concurrency plan is conservative enough for validation. Keep `executor.queueSize`
at 8 initially and add label-level `maxForks` only after command isolation and trace
files exist. Otherwise there is no evidence loop.

Recommended first values after traces are available:

```groovy
withLabel: 'illum_apply' { maxForks = 4 }
withLabel: 'segmentation' { maxForks = 4 }
withLabel: 'feature_extract' { maxForks = 2 }
withLabel: 'staging' { maxForks = 2 }
```

Do not tune these from guesses. Tune from trace files, `qacct`, and peak `du -sh`
measurements.

### What Already Exists

- `cptools2/__main__.py` already computes scratch batches and invokes Nextflow once
  per batch. Reuse this; do not add a shell wrapper.
- `cptools2/batch.py` already handles plate-size scanning and scratch quota
  detection. Extend it with configurable sizing.
- `tests/test_cli.py` already covers dry-run batch params and Nextflow invocation.
  Extend these tests for batch work dirs and provenance flags.
- `tests/test_batch.py` already covers basic batch grouping. Extend it for sizing
  overrides. Oversized-plate behavior is explicitly deferred by user decision.
- `nextflow/conf/eddie.config` already has SGE, staging labels, GPU handling, and
  `queueSize`. Tune it later, with evidence.
- `nextflow/modules/stage_in.nf` and `stage_out.nf` already isolate DataStore
  movement. Do not rebuild that in Python.

### NOT In Scope

- External `run_batches.sh`: useful as documentation, not the product interface.
- `publishDir mode: 'move'`: defer until terminal outputs are formally identified.
- Node-local `process.scratch = '$TMPDIR'`: interesting, but it changes Nextflow IO
  behavior and should wait until the simpler work-dir isolation path is validated.
- Batched or nested output directories: explicitly out of scope because they confuse
  the user-facing result layout.
- Rewriting Nextflow graph internals: not needed for this feature.
- Eddie performance tuning beyond conservative caps: needs traces from real runs.

### Failure Modes

| New codepath | Realistic failure | Test? | Handling? | User-visible? |
|--------------|-------------------|-------|-----------|---------------|
| Batch sizing | One plate exceeds usable scratch | Deferred | Deferred | Accepted low-probability risk |
| Params writing | Batch params written but output dir missing | Existing partial | Existing partial | Mostly clear |
| Command building | Wrong `-work-dir` shared by all batches | Required | No | Silent until scratch cleanup |
| Trace path creation | Trace directory missing or unwritable | Required | Required | Should fail before Nextflow |
| Nextflow batch run | Batch 2 fails after batch 1 succeeds | Existing partial | Existing stop behavior | Clear enough |
| Cleanup | `nextflow clean` fails | Required | Required | Try guarded batch-dir deletion; if guard fails, report and keep work |
| Output layout | Results land somewhere user does not expect | Not needed | Avoided by design | Keep `output_dir` unchanged |

Critical gaps flagged: 0.

### Parallelization Strategy

Sequential implementation is safer for Phase 1 because the highest-risk changes all
touch `cptools2/__main__.py`.

After Phase 1 lands, split work as:

| Step | Modules touched | Depends on |
|------|-----------------|------------|
| Batch API sizing | `cptools2/`, `tests/` | Phase 1 |
| CLI cleanup flag | `cptools2/`, `tests/` | Phase 1 |
| Eddie maxForks tuning | `nextflow/`, `tests/` | Trace output from Phase 1 |

Lane A: Phase 1 command isolation and traces, sequential.
Lane B: Eddie config tuning, later and independent after traces exist.
Lane C: guarded cleanup, later and sequential because it affects scratch deletion.

### Recommended Implementation Order

1. Extract `_build_nextflow_command()` and add tests.
2. Add `batch_name`, `batch_work_dir`, and trace paths without changing
   `params.output_dir`.
3. Add configurable `scratch_utilisation_fraction` and `scratch_work_factor`.
4. Add guarded cleanup with strict batch-work-dir path checks.
5. Run local tests.
6. Run Eddie dry-run.
7. Run one small Eddie batch and collect trace/report/timeline plus peak scratch.
8. Only then add concurrency tuning.

### Completion Summary

- Step 0: Scope Challenge: scope accepted with tighter sequencing
- Architecture Review: second pass found no remaining plan mismatch
- Code Quality Review: 2 issues found
- Test Review: diagram produced, implementation test list accepted
- Performance Review: 1 issue found
- NOT in scope: written
- What already exists: written
- TODOS.md updates: skipped in Codex default mode, use this report as source
- Failure modes: 0 critical gaps flagged
- Outside voice: skipped
- Parallelization: 3 lanes, mostly sequential until Phase 1 lands
- Lake Score: 9/10, plan now matches the discussed goals and avoids output layout churn

### Verdict

ENG REVIEW CLEARED.

Implement the agreed constraints: command helper tests, per-batch work dirs,
trace/report artifacts, configurable scratch sizing in the batch API, and guarded
post-success cleanup. Keep `params.output_dir` unchanged for every batch.
