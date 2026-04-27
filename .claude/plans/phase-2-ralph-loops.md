---
phase: 2
name: Eddie Deployment
plan: phase-2-eddie-deployment.md
status: in_progress
loops_total: 3
---

# Phase 2: Eddie Deployment — Containers, Staging, End-to-End

## Loop 210: Container Build & Deploy

```yaml
loop_id: 210
name: Container Build & Deploy
status: pending
type: infrastructure
key_outputs:
  - 3 .sif containers on Eddie at .../chandranlabs/cptools2/containers/
  - Container manifest deployed and verified
  - build_containers.sh parameterized for reuse
todos:
  - text: "Parameterize build_containers.sh: accept CONTAINER_DIR as first argument with default /exports/cmvm/eddie/scs/groups/chandranlabs/cptools2/containers"
    status: completed
    skill: null
    agent: null
  - text: "Remove dead eddie_container_dir param from nextflow/conf/containers.config"
    status: completed
    skill: null
    agent: null
  # AWAITING MANUAL EXECUTION — todos below require Eddie SSH or Docker daemon access
  - text: "Create cptools2/ directory on Eddie: ssh eddie 'mkdir -p /exports/cmvm/eddie/scs/groups/chandranlabs/cptools2/{containers,env,nextflow}'"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Build Docker images locally: docker pull cellprofiler:4.2.8, docker build deepprofiler:1.0, docker build cellpose-sam:1.0"
    status: pending
    skill: null
    agent: null
  - text: "Save Docker archives: docker save | gzip for all 3 images"
    status: pending
    skill: null
    agent: null
  - text: "Transfer archives to Eddie: rsync -avzP *.tar.gz to chandranlabs/cptools2/containers/"
    status: pending
    skill: null
    agent: null
  - text: "Submit SGE build job on Eddie: qsub build_containers.sh (converts docker-archive to .sif)"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Validate CellProfiler container: qlogin + singularity exec cellprofiler_4.2.8.sif cellprofiler --version"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Validate DeepProfiler container (GPU): qlogin -pe gpu-a100 1 + singularity exec --nv deepprofiler_1.0.sif TF GPU check"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Validate Cellpose-SAM container (GPU): singularity exec --nv cellpose_sam_1.0.sif torch.cuda check"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Deploy manifest: scp container_manifest_template.json to Eddie as cptools2_containers.json, update verified flags"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Run pytest tests/ -v locally and verify 140+ tests still pass after config changes"
    status: completed
    skill: null
    agent: null
```

## Loop 220: Staging + Batch Orchestration

```yaml
loop_id: 220
name: Staging + Batch Orchestration
status: completed
type: implementation
depends_on: [210]
key_outputs:
  - nextflow/modules/stage_in.nf and stage_out.nf
  - cptools2/batch.py with compute_plate_sizes, create_batches, get_scratch_quota
  - cmd_pipeline updated with subprocess.run batch loop
  - eddie.config staging label updated with time = 4.h
  - tests/test_batch.py with 6-7 unit tests
  - scripts/install_eddie.sh for shared env deployment
handoff_summary:
  done: "stage_in.nf, stage_out.nf, batch.py, test_batch.py (8 tests), scripts/install_eddie.sh created; __main__.py uses subprocess.run batch loop with scratch pre-flight; eddie.config staging time=4h; 148 tests pass."
  failed: ""
  needed: "Loop 230: end-to-end Eddie test — deploy via install_eddie.sh, run pipeline on real plate, validate output structure."
todos:
  - id: "loop-220-1"
    content: "Create nextflow/modules/stage_in.nf: STAGE_IN process with label 'staging', rsync --partial --timeout=300 from DataStore to scratch, output tuple(plate_id, staged_path)"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "nextflow/modules/stage_in.nf exists; contains STAGE_IN process with label 'staging', rsync --partial --timeout=300, and emits tuple(plate_id, staged_path)"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-220-2"
    content: "Create nextflow/modules/stage_out.nf: STAGE_OUT process with label 'staging', rsync --partial --timeout=300 results from scratch to DataStore output"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "nextflow/modules/stage_out.nf exists; contains STAGE_OUT process with label 'staging' and rsync --partial --timeout=300 to DataStore"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-220-3"
    content: "Update eddie.config: add time = '4.h' to withLabel: 'staging' block"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "nextflow/conf/eddie.config withLabel: 'staging' block contains time = '4.h'"
    status: completed
    complexity: low
    priority: high
  - id: "loop-220-4"
    content: "Update main.nf: include STAGE_IN and STAGE_OUT modules, wire into workflow (STAGE_IN before ILLUM_CALCULATE, STAGE_OUT after FEATURE_EXTRACT)"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "nextflow/main.nf includes STAGE_IN and STAGE_OUT; workflow calls STAGE_IN before ILLUM_CALCULATE and STAGE_OUT after FEATURE_EXTRACT"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-220-5"
    content: "Create cptools2/batch.py: migrate batch logic from job.py — compute_plate_sizes(input_dir), create_batches(plate_sizes, available_scratch) with 75% util / 30% overhead, get_scratch_quota() with lfs/df/YAML fallback chain"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "cptools2/batch.py exists; exports compute_plate_sizes, create_batches, get_scratch_quota; 75%/30% constants present; fallback chain implemented"
    status: completed
    complexity: high
    priority: high
  - id: "loop-220-6"
    content: "Update __main__.py cmd_pipeline: replace os.execvp with subprocess.run batch loop. Add scratch pre-flight check (warn at 80%). Add -profile eddie to nf_cmd. Skip completed batches on resume."
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "__main__.py cmd_pipeline uses subprocess.run; scratch pre-flight warns at 80%; -profile eddie in nf_cmd; completed batches skipped on resume"
    status: completed
    complexity: high
    priority: high
  - id: "loop-220-7"
    content: "Create scripts/install_eddie.sh: parameterized install script that creates cptools2/{containers,env,nextflow} structure, conda env, pip install, nextflow symlink, activate.sh"
    skill: "NA"
    agent: "ralph-loop-worker"
    outcome: "scripts/install_eddie.sh exists; accepts base dir param; creates containers/env/nextflow subdirs; installs conda env; creates activate.sh"
    status: completed
    complexity: medium
    priority: medium
  - id: "loop-220-8"
    content: "Create tests/test_batch.py: test create_batches (single plate fits, multi-batch, oversized plate, empty list), test get_scratch_quota fallbacks (mock lfs/df), test compute_plate_sizes with tmp_path"
    skill: "test-driven-development"
    agent: "ralph-loop-worker"
    outcome: "tests/test_batch.py exists; contains 6-7 test functions covering all specified cases; all tests pass"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-220-9"
    content: "Run pytest tests/ -v and verify 146+ tests pass (140 existing + 6 new batch tests)"
    skill: "verification-before-completion"
    agent: "ralph-loop-worker"
    outcome: "pytest exits 0; 146 or more tests collected and passing; no regressions in existing test suite"
    status: completed
    complexity: low
    priority: high
```

## Loop 230: End-to-End Eddie Test

```yaml
loop_id: 230
name: End-to-End Eddie Test
status: pending
type: validation
depends_on: [220]
key_outputs:
  - Full pipeline run on Eddie with representative Sarah-screen Cell Painting data
  - Verified output structure at output_dir/plate_id/stage/
  - Pipeline timeline and report HTML generated
handoff_summary:
  done: "Eddie install completed; Sarah-screen DataStore path verified on staging nodes; representative plate `3723-D-100` selected; plate size measured at ~103G; scratch-contained config and params generated at `/exports/eddie/scratch/mharvey2/cptools2-loop230`; parser now enforces `stage_data: true` for DataStore inputs."
  failed: "Full representative compute run has not completed yet; local Windows pytest remains blocked by temp/cache permission errors; Eddie runtime env does not include pytest."
  needed: "Run the staged Sarah-screen validation from `/exports/eddie/scratch/mharvey2/cptools2-loop230/params.json`, starting with staging/illumination and then full compute once scheduler state is clean."
todos:
  - id: "loop-230-1"
    content: "Deploy cptools2 to Eddie using install_eddie.sh: bash scripts/install_eddie.sh /exports/cmvm/eddie/scs/groups/chandranlabs — verify activate.sh created and conda env installs cleanly"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "install_eddie.sh exits 0; activate.sh exists at chandranlabs/cptools2/env/activate.sh; cptools2 importable in the new conda env"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-230-2"
    content: "Create test YAML config on Eddie pointing at representative Sarah-screen plate `3723-D-100` on DataStore, with runtime assets copied into scratch project"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "config/loop230-sarah-screen.yaml exists locally and on Eddie; scratch copy exists at /exports/eddie/scratch/mharvey2/cptools2-loop230/config/loop230-sarah-screen.yaml; params resolve shared .sif containers via CPTOOLS2_CONTAINER_DIR"
    status: completed
    complexity: low
    priority: high
  - id: "loop-230-3"
    content: "Run dry-run: cptools2 pipeline test_config.yaml --dry-run — inspect params.json for correct container paths, expanded stage names, and batch info"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "dry-run exits 0; /exports/eddie/scratch/mharvey2/cptools2-loop230/params.json contains scratch-local pipelines, expanded stage names, explicit plate list, stage_data=true, and resolved container paths"
    status: completed
    complexity: medium
    priority: high
  - id: "loop-230-4"
    content: "Run staged illumination test for Sarah-screen plate `3723-D-100` using scratch params — wait for SGE jobs to complete and verify outputs"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "output_dir/plate_id/illum_functions/ exists with at least one .npy file; output_dir/plate_id/corrected_images/ exists; no SGE job failures"
    status: pending
    complexity: medium
    priority: high
  - id: "loop-230-5"
    content: "Run segmentation stage test: cptools2 pipeline test_config.yaml --stages segmentation — wait for SGE jobs to complete and verify outputs"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "output_dir/plate_id/segmentation/ exists with output files; no SGE job failures"
    status: pending
    complexity: medium
    priority: medium
  - id: "loop-230-6"
    content: "Run feature extraction stage test (GPU): cptools2 pipeline test_config.yaml --stages feature_extract — wait for GPU SGE jobs and verify output"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "output_dir/plate_id/features/ exists with at least one CSV or HDF5 output; GPU job exits 0; singularity --nv flag confirmed in job script"
    status: pending
    complexity: high
    priority: medium
  - id: "loop-230-7"
    content: "Run full representative Sarah-screen pipeline end-to-end using scratch params — wait for all SGE jobs and verify all stage outputs plus pipeline report"
    skill: "eddie:eddie-login"
    agent: "ralph-loop-worker"
    outcome: "All stage output directories present; pipeline_info/report.html and pipeline_info/timeline.html exist; no failed jobs in qstat/qacct"
    status: pending
    complexity: high
    priority: high
  - id: "loop-230-8"
    content: "Verify scratch quota compliance: check lfs quota or df on scratch after full run to confirm usage stayed within 75% target"
    skill: "verification-before-completion"
    agent: "ralph-loop-worker"
    outcome: "Scratch usage reported at or below 75% of quota; result recorded in a one-line note committed to dev_docs/eddie_pipeline_gotchas.md"
    status: pending
    complexity: low
    priority: medium
```
