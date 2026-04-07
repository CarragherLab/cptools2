---
phase: 2
name: Eddie Deployment
plan: phase-2-eddie-deployment.md
status: pending
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
  - Full pipeline run on Eddie with real imaging data
  - Verified output structure at output_dir/plate_id/stage/
  - Pipeline timeline and report HTML generated
todos:
  - text: "Deploy cptools2 to Eddie using install_eddie.sh: bash scripts/install_eddie.sh /exports/cmvm/eddie/scs/groups/chandranlabs"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Create test YAML config pointing at a small plate (~100 images) on DataStore with container_path set to chandranlabs/cptools2/containers/"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Run dry-run: cptools2 pipeline test_config.yaml --dry-run — verify params.json has correct container paths, expanded stage names, batch info"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Run illum-only test: cptools2 pipeline test_config.yaml --stages illum — verify output at output_dir/plate_id/illum_functions/ and corrected_images/"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Run segmentation test: cptools2 pipeline test_config.yaml --stages segmentation — verify output at output_dir/plate_id/segmentation/"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Run feature extraction test (GPU): cptools2 pipeline test_config.yaml --stages feature_extract — verify output at output_dir/plate_id/features/"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Run full pipeline: cptools2 pipeline test_config.yaml — verify all outputs, check pipeline_info/report.html and timeline.html"
    status: pending
    skill: eddie:eddie-login
    agent: null
  - text: "Verify scratch stayed within quota during run (lfs quota or df)"
    status: pending
    skill: eddie:eddie-login
    agent: null
```
