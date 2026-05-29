---
phase: 2.8
name: Eddie Container Validation and Runtime Certification
plan: phase-2.8-eddie-container-validation.md
status: active
loops_total: 7
---

# Phase 2.8: Eddie Container Validation and Runtime Certification Ralph Loops

## Loop 390: Planning State and Eddie Inventory

```yaml
---
name: "ralph-loop-390"
task_name: "Planning State and Eddie Inventory"
max_iterations: 3
on_max_iterations: checkpoint

handoff_summary:
  done: "Phase 2.8 phase and loop plans were created. Phase 2.7 TODOs were reconciled for completed Loops 360 and 370. AGENTS.md and CLAUDE.md now point to Phase 2.8 Loop 390. The dedicated Eddie scratch root exists with work, params, logs, traces, staging, and snapshots. First inventory shows permanent mirror commit 062a0da with many dirty/untracked changes, expected .sif files present, no container JSON manifest in containers/, and module candidates including roslin/nextflow/25.10.2, singularity/4.3.4, miniforge/25.3.1-0, cuda/11.8, and cuda/12.1.1."
  failed: ""
  needed: "Persist the remote inventory into a timestamped scratch snapshot file if needed, then continue to Loop 400 without resetting or overwriting the dirty permanent mirror."

todos:
  - id: "loop-390-1"
    content: "Reconcile Phase 2.7 TODOs and create Phase 2.8 phase/loop plans"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-390-2"
    content: "Create or verify scratch runtime subdirectories under ${CPTOOLS2_SCRATCH_ROOT}"
    status: completed
    complexity: low
    priority: high
  - id: "loop-390-3"
    content: "Snapshot permanent mirror git status, current commit, remote changes, and container inventory"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-390-4"
    content: "Snapshot Eddie module availability for Nextflow, Singularity, Miniforge, CUDA/GPU, and SGE context"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-390-5"
    content: "Document blockers and user-decision points before mirror sync or container smoke execution"
    status: completed
    complexity: low
    priority: high

prompt: |
  ## Objective
  Establish a clean Phase 2.8 planning state and non-destructively inventory Eddie before any mirror update or smoke execution.

  ## Success criteria
  - [ ] Phase 2.8 plan and Ralph loops exist
  - [ ] TODOs and planning state point at Phase 2.8
  - [ ] Scratch root exists with work/logs/traces/params/staging/snapshots
  - [ ] Permanent mirror and containers are snapshotted
  - [ ] Module availability is snapshotted
  - [ ] Blockers and user decisions are documented

  ## Constraints
  - Do not reset, overwrite, or delete permanent mirror files
  - Do not launch production jobs
  - Do not clean scratch evidence from this loop
---
```

## Loop 486: Gated MIG Functional Validation

```yaml
---
name: "ralph-loop-486"
task_name: "Gated MIG Functional Validation"
max_iterations: 3
on_max_iterations: checkpoint

handoff_summary:
  done: "Loop 485 established the resource model and proved a tiny Nextflow-managed MIG Cellpose smoke can use CUDA and complete. Loop 486 deployed the per-run staged-root generator and isolated Nextflow launch/cache state under each scratch run directory. Fresh matched Cellpose smokes completed on gpu-mig=1 and gpu=1 without cache reuse, both using CUDA and producing mask plus locations outputs. A MIG segment/extract smoke completed the zero-cell DeepProfiler handoff path and published the no-cells marker."
  failed: "The first full-GPU same-input comparison reused cached MIG work and is invalid as equivalence evidence. The first concurrent segment/extract launch exposed a shared staged-root race in the diagnostic generator. A later concurrent launch exposed the shared permanent .nextflow/cache lock, fixed by running Nextflow from each scratch run directory with per-run NXF_HOME."
  needed: "Next validation should use a real-cell tiny fixture or small real-cell Eddie smoke to prove actual DeepProfiler feature generation and capture direct GPU-memory evidence where possible before increasing chunk sizes or changing defaults."

todos:
  - id: "loop-486-1"
    content: "Deploy and verify the per-run staged-root diagnostic generator in the Eddie permanent mirror"
    skill: "eddie-validate"
    agent: "NA"
    outcome: "A newly generated diagnostic config uses a run-specific staged-root path; the trace does not report reused cached Cellpose work"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-486-2"
    content: "Run fresh matched Cellpose functional smokes on gpu-mig=1 and gpu=1"
    skill: "eddie-orchestrate"
    agent: "NA"
    outcome: "Both runs have trace/report/timeline, SGE accounting, output masks/locations evidence, and no unexplained cache reuse"
    status: completed
    complexity: high
    priority: high
  - id: "loop-486-3"
    content: "Compare matched Cellpose results across GPU classes"
    skill: "eddie-resources"
    agent: "NA"
    outcome: "Comparison separates queue wait, runtime, CPU RSS, virtual memory, GPU-memory evidence, FUSE messages, exit status, and output presence"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-486-4"
    content: "Run MIG segment/extract smoke through the DeepProfiler handoff"
    skill: "eddie-orchestrate"
    agent: "NA"
    outcome: "Workflow either writes DeepProfiler feature artifacts or exits through the documented zero-cell continuation path with status 0"
    status: completed
    complexity: high
    priority: high
  - id: "loop-486-5"
    content: "Write the MIG validation policy checkpoint"
    skill: "plan-eng-review"
    agent: "NA"
    outcome: "Tracked plan states whether MIG is approved for quick validation, stage-limited, or blocked; production-like default remains explicit"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-486-6"
    content: "Unlock the next validation phase only after the MIG policy checkpoint"
    skill: "plan-todos"
    agent: "NA"
    outcome: "TODOs identify the next permitted validation target: DeepProfiler real-cell feature output, staging/destaging, or larger chunk threshold testing"
    status: completed
    complexity: medium
    priority: medium

prompt: |
  ## Objective
  Establish a cache-safe, evidence-based MIG validation gate before using MIG for subsequent cptools2 pipeline validation.

  ## Success Criteria
  - [x] Diagnostic configs use run-specific staged roots and fresh work directories
  - [x] Matched Cellpose MIG/full-GPU functional runs are compared without cache reuse
  - [x] DeepProfiler handoff is exercised on MIG with zero-cell handling or feature output documented
  - [x] CPU RSS, virtual memory, and GPU framebuffer evidence are interpreted separately
  - [x] A tracked policy states when MIG is acceptable and what remains full-GPU only
  - [x] Downstream validation steps are gated on the policy result

  ## Constraints
  - Keep exact paths, job IDs, run roots, and dataset identifiers in scratch evidence
  - Do not treat successful scheduling as successful science/runtime validation
  - Do not infer GPU memory headroom from maxvmem or peak_vmem
  - Do not change production-like defaults until the policy checkpoint passes
---
```

## Loop 400: Permanent Bootstrap and Runtime Config

```yaml
---
name: "ralph-loop-400"
task_name: "Permanent Bootstrap and Runtime Config"
max_iterations: 3
on_max_iterations: checkpoint

handoff_summary:
  done: "Loop 400 added config/eddie_env.sh with pinned Eddie modules and explicit permanent/scratch roots. nextflow/conf/eddie.config now resolves CellProfiler, Cellpose, and DeepProfiler .sif paths from permanent containers/, loads pinned modules in process beforeScript, and routes Singularity tmp/cache through cptools2-ai-update/work. tests/test_eddie_runtime_config.py verifies bootstrap and Nextflow path/module contracts. Focused verification passed: pytest tests/test_eddie_runtime_config.py tests/test_containers.py -q -> 53 passed."
  failed: ""
  needed: "Loop 410 should reconcile the stale/dirty permanent mirror and missing container manifest before deploying these local config changes."

todos:
  - id: "loop-400-1"
    content: "Create an Eddie environment bootstrap that loads pinned modules and exports permanent/scratch roots"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-400-2"
    content: "Ensure Nextflow Eddie config routes work/cache/log artifacts to scratch and stable configs/containers to permanent space"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-400-3"
    content: "Add tests or static checks for the permanent/scratch path contract"
    status: completed
    complexity: medium
    priority: medium
  - id: "loop-400-4"
    content: "Document how to source the bootstrap and run dry-runs from the permanent mirror"
    status: completed
    complexity: low
    priority: medium

prompt: |
  ## Objective
  Make Eddie runs reproducible by putting stable environment/config setup in permanent space while keeping runtime state under scratch.

  ## Success criteria
  - [ ] Bootstrap loads Nextflow, Singularity, and Miniforge modules
  - [ ] Bootstrap exports CPTOOLS2 project, container, and scratch/work/cache roots
  - [ ] Config/docs make the permanent-vs-scratch contract explicit
  - [ ] Local tests/static checks pass
---
```

## Loop 410: Mirror Sync and Manifest Reconciliation

```yaml
---
name: "ralph-loop-410"
task_name: "Mirror Sync and Manifest Reconciliation"
max_iterations: 3
on_max_iterations: escalate

handoff_summary:
  done: "Local HEAD d11d205 and permanent mirror HEAD 062a0da were compared. Remote mirror has modified tracked source/test files and many untracked Nextflow/config/container/cache artifacts. Live qconf confirms current GPU syntax uses the gpu queue/PE and requestable -l gpu=N; gpus is not requestable. A conservative mirror strategy and approval boundary were added to the Phase 2.8 plan."
  failed: ""
  needed: "Persist a timestamped remote snapshot if desired, then either request approval for mirror replacement/reset or proceed with direct-container smoke commands that do not depend on the stale package import."

todos:
  - id: "loop-410-1"
    content: "Compare local HEAD with permanent mirror HEAD and list remote-only modifications"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-410-2"
    content: "Prepare a conservative mirror update strategy that preserves or stages remote changes"
    status: completed
    complexity: high
    priority: high
  - id: "loop-410-3"
    content: "Reconcile container manifest expectations with actual .sif files in permanent containers/"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-410-4"
    content: "Record exact user approval needed before any destructive mirror action"
    status: completed
    complexity: low
    priority: high

prompt: |
  ## Objective
  Make the permanent mirror reliable for validation without clobbering unreviewed remote state.

  ## Success criteria
  - [ ] Mirror divergence is documented
  - [ ] Manifest/container mismatches are documented or fixed non-destructively
  - [ ] Deployment command is dry-run reviewed before execution
---
```

## Loop 420: CellProfiler CPU Container Smoke

```yaml
---
name: "ralph-loop-420"
task_name: "CellProfiler CPU Container Smoke"
max_iterations: 3
on_max_iterations: checkpoint

handoff_summary:
  done: "Created scripts/eddie_smoke_cellprofiler.sh and static tests for the CPU SGE smoke script. Submitted job 55264527 from the scratch validation root. It completed successfully with CellProfiler version 4.2.8, qacct exit_status 0, failed 0, ru_wallclock 404.182, and maxvmem 12.096G. Logs are under ${CPTOOLS2_SCRATCH_ROOT}/logs and version output is under smoke/cellprofiler. The run exposed a Singularity startup caveat: FUSE mount failed on Eddie and Singularity fell back to scratch extraction before running."
  failed: ""
  needed: "A functional CPU smoke using a tiny image/config fixture is still pending; decide whether this should reuse Loop 230 example-screen data or a purpose-built small fixture."

todos:
  - id: "loop-420-1"
    content: "Run CellProfiler container startup/version check on Eddie"
    status: completed
    complexity: low
    priority: high
  - id: "loop-420-2"
    content: "Run or prepare the smallest CPU-only CellProfiler smoke using scratch staging and permanent config"
    status: pending
    complexity: high
    priority: high
  - id: "loop-420-3"
    content: "Capture logs, resource use, output locations, and blockers"
    status: completed
    complexity: medium
    priority: high

prompt: |
  ## Objective
  Prove the CellProfiler container starts and can run a minimal Eddie CPU workload.
---
```

## Loop 430: Cellpose and DeepProfiler GPU Smoke

```yaml
---
name: "ralph-loop-430"
task_name: "Cellpose and DeepProfiler GPU Smoke"
max_iterations: 3
on_max_iterations: checkpoint

handoff_summary:
  done: "Corrected GPU smoke scripts exist locally and on Eddie scratch. Cellpose job 55264576 completed successfully: Cellpose 4.1.1, CUDA visible on NVIDIA H200 NVL, qacct exit_status 0, failed 0, ru_wallclock 226.879, maxvmem 17.010G. DeepProfiler job 55269157 completed successfully: TensorFlow 2.5.3, GPU device visible, qacct exit_status 0, failed 0, ru_wallclock 210.269, maxvmem 16.212G. No <UUN> jobs remained in qstat after validation. The prior tee/pipe status-masking bug was fixed in all smoke scripts."
  failed: ""
  needed: "Functional GPU smoke inputs remain out of scope for the startup probes. Before an end-to-end smoke, choose minimal images and confirm DeepProfiler config/weights/checkpoint assets."

todos:
  - id: "loop-430-1"
    content: "Validate Cellpose container startup with --nv and CUDA visibility"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-430-2"
    content: "Validate DeepProfiler container startup with --nv and TensorFlow/CUDA visibility"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-430-3"
    content: "Prepare minimal GPU smoke inputs or document the missing assets/queue blockers"
    status: completed
    complexity: high
    priority: high

prompt: |
  ## Objective
  Prove GPU-dependent containers are compatible with Eddie or document the exact GPU/runtime blocker.
---
```

## Loop 440: End-to-End Eddie Smoke and Evidence

```yaml
---
name: "ralph-loop-440"
task_name: "End-to-End Eddie Smoke and Evidence"
max_iterations: 3
on_max_iterations: checkpoint

handoff_summary:
  done: "Loop 440 audit confirmed no lingering Eddie jobs. Added config/loop440-eddie-smoke.yaml as a candidate end-to-end smoke config that uses a tiny staged plate under cptools2-ai-update/staging, keeps stage_data false, limits max_chunks to 1, writes outputs under cptools2-ai-update/results/loop440, and references permanent <group> templates/containers. Added scripts/create_loop440_tiny_plate.py, generated a five-channel real-TIFF tiny plate in Eddie scratch, and verified Eddie indexing/chunking produced image_sets.csv and one chunk manifest. Stable templates/configs were additively copied into the permanent mirror. A cptools2 dry-run from the permanent mirror was attempted and failed before Nextflow submission because stale permanent mirror code rejects scratch_utilisation_fraction and scratch_work_factor. Focused tests passed: pytest tests/test_eddie_runtime_config.py -q -> 8 passed."
  failed: ""
  needed: "Decide mirror strategy before real Loop 440 dry-run: conservative permanent mirror source sync is preferred for the stated goal, while scratch-only code checkout is safer but less representative. Do not submit an end-to-end Nextflow job from the stale permanent source."

todos:
  - id: "loop-440-1"
    content: "Run the smallest practical multi-engine dry-run or smoke on Eddie"
    status: in_progress
    complexity: high
    priority: high
  - id: "loop-440-2"
    content: "Capture trace/report/timeline, qacct or scheduler evidence, scratch usage, and cleanup behavior"
    status: in_progress
    complexity: high
    priority: high
  - id: "loop-440-3"
    content: "Update docs with final evidence, blockers, and recommended next production gate"
    status: completed
    complexity: medium
    priority: high

prompt: |
  ## Objective
  Exercise the integrated runtime path just far enough to certify the architecture and leave a clear evidence trail.
---
```

## Loop 450: DeepProfiler Input Package Handoff

```yaml
---
name: "ralph-loop-450"
task_name: "DeepProfiler Input Package Handoff"
max_iterations: 3
on_max_iterations: checkpoint

handoff_summary:
  done: "Loop 440 reached DeepProfiler after successful local indexing/chunking, SGE submission, and Cellpose GPU execution. DeepProfiler failed because FEATURE_EXTRACT did not create the required dp_project/inputs/metadata/index.csv package contract."
  failed: ""
  needed: "Implement the robust DeepProfiler input package builder described in docs/superpowers/plans/2026-05-06-deepprofiler-metadata-bridge.md, then rerun Loop 440 with --resume from the Eddie permanent mirror."

todos:
  - id: "loop-450-1"
    content: "Add failing tests for DeepProfiler input package generation from chunk manifests and Cellpose locations"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-450-2"
    content: "Implement build_deepprofiler_input_package in cptools2.nextflow_chunking, including zero-location support"
    status: pending
    complexity: high
    priority: high
  - id: "loop-450-3"
    content: "Wire FEATURE_EXTRACT to call the DeepProfiler package helper and validate package artifacts before launching DeepProfiler"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-450-4"
    content: "Rerun Loop 440 on Eddie with --resume and capture trace/report/timeline plus any new blocker"
    status: pending
    complexity: high
    priority: high

prompt: |
  ## Objective
  Make the Cellpose-to-DeepProfiler handoff explicit and reproducible by building a complete DeepProfiler input package before FEATURE_EXTRACT runs.

  ## Success criteria
  - [ ] Package builder creates image links, metadata/index.csv, locations, and config under dp_project/inputs
  - [ ] Zero Cellpose locations produce empty per-site nuclei CSVs with headers and do not fail the package build
  - [ ] FEATURE_EXTRACT invokes the package builder and checks package artifacts before DeepProfiler
  - [ ] Focused local tests pass
  - [ ] Eddie Loop 440 rerun reaches beyond the missing-index.csv blocker

  ## Constraints
  - Do not publish bulky linked image directories
  - Keep runtime package artifacts in scratch/Nextflow work
  - Treat .tif as the default image format for now
  - Capture model/checkpoint failures as separate blockers if they appear after package generation
---
```

## Loop 485: GPU Capacity and Throughput Strategy

```yaml
---
name: "ralph-loop-485"
task_name: "GPU Capacity and Throughput Strategy"
max_iterations: 3
on_max_iterations: checkpoint

handoff_summary:
  done: "Current-docs GPU probes showed that gpu-mig=1 can be accepted and run quickly on the gpu queue with scheduler-managed CUDA_VISIBLE_DEVICES. Matched container-only probes now completed on both gpu-mig=1 and gpu=1 for Cellpose and DeepProfiler. MIG started much faster in this run; full GPU started later but completed faster once scheduled. Both GPU classes still showed the Singularity FUSE mount failure followed by sandbox extraction, so GPU class alone is not the FUSE mitigation. A Nextflow-managed MIG Cellpose functional smoke also completed successfully on a tiny TIFF input, used CUDA, wrote mask and locations outputs, and tolerated the zero-cell case. Eddie docs identify MIG as a smaller A100 partition with tighter GPU-memory limits, suitable for validation only after functional equivalence and memory headroom are proven. Memory interpretation is explicit: h_rss/peak_rss are CPU resident memory signals; maxvmem/peak_vmem are virtual address space; MIG GPU framebuffer must be measured separately where possible."
  failed: ""
  needed: "Design and run matched probes that measure total turnaround time, output equivalence, memory headroom, and failure modes across gpu-mig=1 and gpu=1 before changing pipeline defaults."

todos:
  - id: "loop-485-1"
    content: "Capture live GPU queue capacity snapshots at submission and completion using qstat -F gpu,gpu-mig,gputype,h_rss -q gpu"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-485-2"
    content: "Record queue wait, start host, GPU type, runtime, resource request, and scheduler wait reason for every GPU diagnostic job"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-485-3"
    content: "Run matched container-only probes on gpu-mig=1 and gpu=1 using identical SIFs, bind options, and temp/cache policy"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-485-4"
    content: "Run matched tiny Cellpose and DeepProfiler functional probes across GPU classes and compare outputs, runtime, CPU RSS, virtual memory context, and GPU-memory indicators"
    status: pending
    complexity: high
    priority: high
  - id: "loop-485-5"
    content: "Evaluate whether fewer longer full-GPU jobs or more smaller MIG/full-GPU jobs give better effective turnaround and lower failure blast radius"
    status: pending
    complexity: high
    priority: high
  - id: "loop-485-6"
    content: "Recommend a resource policy for validation and production-like runs, including when MIG is acceptable and when full GPU is required"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-485-7"
    content: "Run the approved max_chunks 5 matrix across MIG-only and full-GPU policies for chunk sizes 48, 24, and 96"
    status: pending
    complexity: high
    priority: high
  - id: "loop-485-8"
    content: "Run the mixed policy candidate with Cellpose on MIG and serialized DeepProfiler on full GPU if both base policies pass"
    status: pending
    complexity: high
    priority: high
  - id: "loop-485-9"
    content: "Compare total turnaround, output completeness, GPU evidence, and FUSE/runtime symptoms before recommending production policy"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-485-10"
    content: "Add process-specific GPU resource controls so segmentation and feature extraction can request different GPU classes in one run"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-485-11"
    content: "Add GPU diagnostics and normalized benchmark metrics before submitting the max_chunks 5 matrix"
    status: pending
    complexity: medium
    priority: high

prompt: |
  ## Objective
  Choose Eddie GPU resource classes from evidence, optimizing total turnaround time and reproducibility rather than preferring a node type.

  ## Success criteria
  - [x] Capacity snapshots exist for the first matched GPU diagnostic pass
  - [x] Queue wait and execution runtime are separated in the evidence
  - [x] gpu-mig=1 and gpu=1 container-only probes use matched inputs, containers, binds, and runtime settings
  - [ ] Cellpose and DeepProfiler outputs are compared for presence, dimensions/counts, failure modes, CPU RSS, and GPU-memory indicators where available
  - [ ] The plan explicitly weighs many small jobs against fewer longer jobs
  - [ ] A resource policy recommendation is documented before changing Nextflow defaults
  - [ ] The approved max_chunks 5 matrix is complete or failures are clearly classified
  - [ ] The mixed Cellpose-MIG/DeepProfiler-full-GPU candidate is tested if both base policies pass
  - [ ] Process-specific GPU resource controls exist before the mixed candidate runs
  - [ ] Results are interpreted as future multi-plate operating-policy evidence, not only validation pass/fail

  ## Constraints
  - Do not assume MIG is equivalent to a full GPU; prove fit and output equivalence first
  - Do not infer MIG GPU-memory headroom from maxvmem or peak_vmem; those are virtual memory signals, not GPU framebuffer use
  - Do not prefer a GPU type unless queue wait, runtime, memory, or output evidence makes it obvious
  - Keep exact job IDs, run roots, host names, and dataset identifiers in scratch evidence rather than tracked docs
  - Avoid broad production runs until the FUSE and resource-class evidence is clear
---
```
