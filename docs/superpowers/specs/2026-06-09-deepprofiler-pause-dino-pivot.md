# DeepProfiler Pause and DINO Pivot Decision

Date: 2026-06-09

## Decision

Pause active DeepProfiler production development in cptools2. Treat the
DeepProfiler route as a legacy/maintenance path while the DINO feature
extraction workstream becomes the practical successor for new production-scale
development.

This is a product and engineering direction change, not a removal of existing
DeepProfiler code. Existing DeepProfiler learnings remain useful for batching,
scratch cleanup, stage-out verification, GPU diagnostics, and export table
contracts.

## Rationale

DeepProfiler is not as actively maintained as the DINO route now being developed
in the adjacent worktree. The Phase 2.8 to Phase 3.1 testing showed that the
pipeline can stage data, run GPU work, export feature tables, and preserve
evidence, but DeepProfiler-specific runtime behavior kept dominating the
acceptance loop:

- DeepProfiler/TensorFlow required serialized feature extraction; concurrent
  feature jobs failed with cuDNN initialization errors.
- Full-plate DeepProfiler runs produced useful outputs, but acceptance was
  repeatedly blocked by runtime fragility rather than by the core batching model.
- The fresh three-plate acceptance run proved the batching driver, continuation,
  stage-out evidence, and run-reporting model, but DeepProfiler-related summary
  and feature-extraction failures prevented verified cleanup from closing.
- Eddie GPU documentation and live scheduler evidence distinguish GPU VRAM from
  system memory. The observed `maxvmem` values should not be interpreted as a
  simple requirement for >128 GB GPU VRAM; they are DeepProfiler/TensorFlow host
  memory or virtual-memory behavior that needs separate handling.

## Batching Lessons To Carry Forward

The DINO route should inherit the Phase 3.1 operational architecture:

- one batch is a reproducible unit with its own params, trace, work directory,
  batch ledger row, and run-report evidence;
- stage data into scratch before compute and stage user-facing results out to
  durable storage;
- clean batch work only after durable stage-out verification succeeds;
- preserve failed or unverified batch work for inspection;
- record quota snapshots before batch start, after Nextflow return, after
  verification, after cleanup, and before the next batch;
- keep the driver in tmux on Eddie rather than submitting the Nextflow driver
  itself as a qsub job;
- prefer scheduler-managed one-GPU tasks over a custom multi-GPU wrapper;
- allow stage-specific GPU policy and concurrency controls.

## DeepProfiler Stop Line

Do not spend more development effort on DeepProfiler production acceptance unless
one of these explicit conditions is met:

- a user needs to reproduce an existing DeepProfiler result;
- a regression in already-supported DeepProfiler export behavior is reported;
- a lightweight smoke test is needed to confirm legacy compatibility;
- DINO is found unsuitable and DeepProfiler must be revived by explicit decision.

Deferred DeepProfiler items remain documented as roadmap or legacy maintenance,
not active Phase 3.1 blockers.

## DINO Successor Direction

The next active development should focus on moving the DINO workstream through
the same production hardening gates:

- full-plate or multi-plate batching with stage-in/stage-out;
- export-ready feature tables with stable `Metadata_*` columns;
- verified cleanup after durable stage-out evidence;
- clear run reports for partial failures;
- GPU resource policy based on direct `nvidia-smi`, qacct, trace, and output
  evidence rather than assumptions from DeepProfiler behavior.

## Immediate Follow-Up

- Update project TODOs so DeepProfiler acceptance is paused rather than pending.
- Preserve the failed DeepProfiler evidence until the batching lessons are
  transferred into the DINO plan.
- After review, continue with the DINO worktree as the active feature extraction
  implementation route.
