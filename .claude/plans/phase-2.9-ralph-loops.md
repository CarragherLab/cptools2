# Phase 2.9 Ralph Loops

## Loop 510: Plate Export Join Contract

```yaml
---
name: "ralph-loop-510"
task_name: "Plate Export Join Contract"
max_iterations: 3
on_max_iterations: escalate

handoff_summary:
  done: "Repaired the plate export handoff so FEATURE_EXTRACT emits explicit plate/chunk payloads, EXPORT_FEATURES stages chunk-scoped inputs, exporter aliases are plate-aware with optional run labels, and focused export/Nextflow coverage passes after spec and code-quality review."
  failed: "Initial code-quality review caught parquet-only artifact verification and resumed staging re-entry gaps; both were patched and re-reviewed before Eddie validation."
  needed: "Run Loop 520 on Eddie: sync the reviewed repair, resume or rerun the clean DataStore-backed full-plate route, and capture export/stage-out evidence for the Phase 2.9 gate."

todos:
  - id: "loop-510-1"
    content: "Add failing tests for chunk-aware export payloads, collision-proof plate grouping, and plate-aware published table names"
    skill: "test-driven-development"
    agent: "gpt-5.4-mini-implementer"
    outcome: "Focused feature export and Nextflow tests fail for the missing chunk payload and plate-aware naming behavior before implementation"
    status: completed
    priority: high
  - id: "loop-510-2"
    content: "Change the FEATURE_EXTRACT export handoff to carry plate_id, chunk_id, and a feature payload directory without breaking native feature stage-out"
    skill: "nextflow-development"
    agent: "gpt-5.4-mini-implementer"
    outcome: "nextflow/modules/feature_extract.nf and nextflow/main.nf expose an export payload grouped by plate with explicit chunk identity"
    status: completed
    priority: high
  - id: "loop-510-3"
    content: "Make EXPORT_FEATURES stage chunk payloads into unique chunk-scoped inputs and validate the payload set before exporting one plate table set"
    skill: "nextflow-development"
    agent: "gpt-5.4-mini-implementer"
    outcome: "EXPORT_FEATURES no longer stages same-named chunk payload directories into one task input namespace and preserves one CPU export task per plate"
    status: completed
    priority: high
  - id: "loop-510-4"
    content: "Add deterministic canonical table names plus plate-aware published aliases with optional caller-provided run label"
    skill: "test-driven-development"
    agent: "gpt-5.4-mini-implementer"
    outcome: "Exporter tests prove canonical table names remain stable and published CSV aliases include plate identity and the configured run label when present"
    status: completed
    priority: high
  - id: "loop-510-5"
    content: "Review the implementation for contract compliance and code quality before Eddie validation"
    skill: "subagent-driven-development"
    agent: "spec-reviewer-then-quality-reviewer"
    outcome: "Spec review and code-quality review have no unresolved findings for the export join repair"
    status: completed
    priority: high

prompt: |
  ## Context from prior loop
  Done: [inject prior.handoff_summary.done]
  Failed: [inject prior.handoff_summary.failed]
  Needed: [inject prior.handoff_summary.needed]

  ## Objective
  Repair the plate-level export join so chunked native feature payloads join safely into one export-ready table set per plate.

  ## Git checkpoint (run first)
  git add -A && git commit -m "checkpoint: before ralph-loop-510"

  ## Success criteria
  - [ ] FEATURE_EXTRACT exposes export payloads with explicit plate and chunk identity
  - [ ] EXPORT_FEATURES handles repeated internal feature directory names without Nextflow staging collisions
  - [ ] canonical table names remain deterministic inside the table directory
  - [ ] published CSV aliases include plate identity and optional caller-provided run label
  - [ ] focused Phase 2.9 export and Nextflow tests pass
  - [ ] spec-compliance and code-quality reviews are complete

  ## Required skills
  - `test-driven-development`: drive export and Nextflow contract changes from failing tests
  - `nextflow-development`: update Nextflow tuple and staging semantics safely
  - `subagent-driven-development`: use one implementer plus spec and quality review gates

  ## Inputs
  - Design: docs/superpowers/specs/2026-05-21-phase-2.9-plate-export-join-repair-design.md
  - Failure evidence: clean Phase 2.9 launcher and trace under the Eddie full-plate run root
  - Existing export code: cptools2/feature_export/, nextflow/modules/export_features.nf, nextflow/modules/feature_extract.nf

  ## Expected outputs
  - Updated feature export and Nextflow implementation
  - Focused local regression coverage for chunk joins and naming
  - Reviewed implementation ready for Eddie validation

  ## Constraints
  - Keep export CPU-only
  - Keep one user-facing plate-level table set
  - Do not hide NaN or no-cell quality information during the join
  - Do not add uncontrolled wall-clock timestamp naming inside canonical exporter outputs

  ## On completion
  1. git add -A && git commit -m "complete: ralph-loop-510 - repair plate export join"
  2. Update handoff_summary in frontmatter
  3. Mark all todos completed

  Begin. Mark todos in_progress before starting each task. One in_progress at a time.
---
```

## Loop 520: Clean Full-Plate Export Certification

```yaml
---
name: "ralph-loop-520"
task_name: "Clean Full-Plate Export Certification"
max_iterations: 3
on_max_iterations: checkpoint

handoff_summary:
  done: "Synced the reviewed export repair to Eddie, resumed the clean DataStore-backed route, fixed the FEATURE_EXTRACT output compile regression, and certified EXPORT_FEATURES plus final DataStore STAGE_OUT on the full plate."
  failed: "The first reviewed-code resume failed before SGE work on a Nextflow chunk_manifest output-scope error; the direct input expression repair passed locally and on Eddie. A login-node destination probe was inconclusive because DataStore is staging-node only."
  needed: "Use the Phase 2.9 full-plate evidence for the gate decision and carry the staging-node verification rule into future DataStore checks."

todos:
  - id: "loop-520-1"
    content: "Sync the reviewed Phase 2.9 export repair to the permanent Eddie mirror and confirm the clean route config still targets the DataStore-backed full plate"
    skill: "eddie-orchestrate"
    agent: "gpt-5.4-mini-operator"
    outcome: "Permanent Eddie mirror has the reviewed export repair and the clean full-plate route still uses stage_data true with max_chunks omitted"
    status: completed
    priority: high
  - id: "loop-520-2"
    content: "Resume or rerun the clean full-plate route through EXPORT_FEATURES and final STAGE_OUT"
    skill: "eddie-orchestrate"
    agent: "gpt-5.4-mini-operator"
    outcome: "The clean full-plate launcher reaches status 0 or produces a new classified blocker with exact trace and log evidence"
    status: completed
    priority: high
  - id: "loop-520-3"
    content: "Collect full-plate counts, table row statistics, quality summaries, stage-out verification, queue/runtime evidence, and cache reuse facts"
    skill: "eddie-validate"
    agent: "gpt-5.4-mini-verifier"
    outcome: "Tracked Phase 2.9 evidence records full plate chunk counts, output counts, exported table rows, quality facts, stage-out state, and whether GPU work was reused"
    status: completed
    priority: high
  - id: "loop-520-4"
    content: "Update Phase 2.9 documentation and TODO state with the final pass decision or remaining blocker"
    skill: "document-release"
    agent: "gpt-5.4-mini-documenter"
    outcome: "Phase 2.9 docs, Ralph handoff, and TODOs agree on whether the clean full-plate export gate passed"
    status: completed
    priority: high

prompt: |
  ## Context from prior loop
  Done: [inject prior.handoff_summary.done]
  Failed: [inject prior.handoff_summary.failed]
  Needed: [inject prior.handoff_summary.needed]

  ## Objective
  Certify the repaired export join in a clean Eddie full-plate run and capture the evidence needed for the Phase 2.9 gate.

  ## Git checkpoint (run first)
  git add -A && git commit -m "checkpoint: before ralph-loop-520"

  ## Success criteria
  - [ ] reviewed export repair is present on the permanent Eddie mirror
  - [ ] clean full-plate run completes `EXPORT_FEATURES` and final `STAGE_OUT`
  - [ ] trace and output counts prove all intended chunks and tables were processed
  - [ ] stage-out contains plate-aware table exports and canonical tables remain validated
  - [ ] Phase 2.9 documentation records pass evidence or the next exact blocker

  ## Required skills
  - `eddie-orchestrate`: manage clean Eddie route execution
  - `eddie-validate`: inspect SGE, traces, stage-out, and runtime evidence
  - `document-release`: keep tracked evidence and status consistent

  ## Inputs
  - Implementation handoff from ralph-loop-510
  - Clean route root: /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/phase-2.9-full-plate-clean-20260521/dp-growth-concurrency8
  - Phase record: .claude/plans/phase-2.9-feature-export-quality-gate.md

  ## Expected outputs
  - Eddie run evidence for a completed or classified clean full-plate export path
  - Updated Phase 2.9 tracked documentation and TODO state

  ## Constraints
  - Prefer the existing clean full-plate route when resume is valid
  - A fresh clean run is acceptable if the reviewed repair invalidates resume or evidence quality
  - Keep exact Eddie scratch paths and job evidence in tracked docs only when they are part of the active Phase 2.9 certification record

  ## On completion
  1. git add -A && git commit -m "complete: ralph-loop-520 - certify clean full plate export"
  2. Update handoff_summary in frontmatter
  3. Mark all todos completed

  Begin. Mark todos in_progress before starting each task. One in_progress at a time.
---
```

## Post-Gate Closeout

The clean full-plate gate passed on 2026-05-22. Follow-up DeepProfiler export
hardening after the gate is also complete:

- plate-aware aliases validate payload-derived plate identity before table
  publication;
- the plate export join preserves chunk routing evidence in
  `export_input_manifest.csv`;
- per-plate status artifacts and the run-level summary keep successful plates
  visible when another plate export fails or has no status artifact.

CSV remains the certified Phase 2.9 export-ready route. Optional cell-level
Parquet memory scaling is deferred to the roadmap until the writer can stream
or batch full-plate cell tables and a realistic Parquet scalability check passes.
