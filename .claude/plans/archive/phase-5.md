# Phase 5: Pipeline Orchestrator

## Objective

Add a `cptools2 pipeline` command that generates all pipeline stages (illumination correction, segmentation, feature extraction) in a single invocation with correct SGE `hold_jid` dependency chains, producing one master submit script for the entire Cell Painting workflow.

## Scope

### Included:
- Pipeline orchestrator module: `cptools2/pipeline.py`
- CLI: `cptools2 pipeline config.yml` — generates all stages at once
- Master submit script: chains all stages with `hold_jid` dependencies
- Configurable stage selection: `pipeline_stages` YAML key controls which stages run (defaults to all)
- Integration with existing `generate` command: `cptools2 generate` remains for standalone CellProfiler-only workflows
- End-to-end validation command: `cptools2 pipeline validate config.yml` — runs all stage validations

### Explicitly NOT included:
- New pipeline stages (all stages already implemented in Phases 2-4)
- Container building or deployment
- Downstream analysis (pycytominer normalisation, aggregation)
- Runtime monitoring or job status tracking

## Key Deliverables

| Deliverable | Format | Location |
|---|---|---|
| Pipeline orchestrator | Python | `cptools2/pipeline.py` |
| Updated __main__.py | Python | `cptools2/__main__.py` |
| Updated parse_yaml.py | Python | `cptools2/parse_yaml.py` |
| Example pipeline config | YAML | `tests/pipeline_config.yaml` |
| Pipeline integration tests | Python | `tests/test_pipeline.py` |

## Success Criteria

- ✓ `cptools2 pipeline config.yml` generates SGE scripts for all 4 stages (illum_calculate, illum_apply, segmentation, feature_extraction) in one invocation — verified by counting generated .sh files
- ✓ Each stage's SGE script has a `-hold_jid` referencing the correct preceding stage's job name — verified by grep chain: illum_apply depends on illum_calculate, segmentation depends on illum_apply, feature_extraction depends on segmentation
- ✓ A master submit script is generated that `qsub`s all stage scripts in order — verified by inspection
- ✓ `pipeline_stages` YAML key allows running a subset of stages (e.g., only illum + segmentation, omitting feature extraction) — verified by test with partial config
- ✓ When `pipeline_stages` is omitted from config, all stages run — verified by test
- ✓ `cptools2 pipeline validate config.yml` runs all stage validations and reports pass/fail per stage — verified by test
- ✓ GPU stages (feature extraction) correctly use `-pe gpu-a100` while CPU stages use standard compute queue — verified by grep on generated scripts
- ✓ Staging queue constraints respected: staging jobs use `-q staging` without `-pe sharedmem` — verified by grep
- ✓ Existing `cptools2 generate` command still works independently — verified by existing tests

## Dependencies

### Must Complete Before This Phase:
- Phase 1 (Container Integration): All container infrastructure in place
- Phase 2 (Illumination Correction): `illum_calculate` and `illum_apply` stages exist
- Phase 3 (Segmentation): `segmentation` stage exists
- Phase 4 (Feature Extraction): `feature_extract` stage exists

### Blocked By:
- Nothing beyond Phases 1-4

### Optional:
- Phase 0 (Container Deploy): Full end-to-end Eddie testing requires all containers deployed

## Skills Required (Broad Categories)

- `eddie-job-chaining`: Multi-stage hold_jid dependency chains across CPU and GPU queues
- `eddie-script-standards`: Master submit script formatting
- `python-testing`: Integration tests for full pipeline generation

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Job name collisions between pipeline runs | Low | Medium | Include timestamp or experiment ID in job names (existing pattern from generate_scripts.py) |
| GPU queue bottleneck delays entire pipeline | Medium | Medium | Feature extraction is the last stage; CPU stages complete independently. Document that GPU queue times are variable. |
| Stage-to-stage data handoff path mismatch | Medium | High | Pipeline orchestrator uses a shared `location` base path; each stage reads from the previous stage's documented output directory. Validate paths during generation. |
| Partial pipeline re-runs need manual intervention | Medium | Low | Individual stage commands (`cptools2 illum apply`, `cptools2 extract`) exist for re-running specific stages. Document re-run workflow. |

## Assumptions

- `Linear stage ordering sufficient`: The current pipeline is strictly sequential (illum → seg → extract). No branching or parallel stages needed yet. If Cellpose-SAM is added as an alternative to CellProfiler segmentation, the orchestrator would need a branch — but that's future work.
- `Single experiment per pipeline run`: Each `cptools2 pipeline` invocation handles one experiment/config. Multi-experiment orchestration is done by running the command multiple times.
- `Job names are unique per invocation`: Timestamp-based job naming (existing pattern) ensures no collisions between pipeline runs.

## Notes / Design Decisions

- **Pipeline orchestrator is thin**: It instantiates each stage in order, calls `generate_script()`, collects the script paths and job names, then writes the master submit script. No complex state machine — just a sequential loop with dependency tracking.
- **Master submit script is a shell script, not a qsub script**: It runs on the login node and submits jobs via `qsub`. This matches the existing pattern from `create_master_submit_script()` in `generate_scripts.py`.
- **Stage selection via YAML**: `pipeline_stages` is a list of stage names. The orchestrator skips stages not in the list and adjusts `hold_jid` dependencies accordingly (each stage depends on the last included stage, not necessarily the previous stage in the full pipeline).
- **Validation is aggregate**: `cptools2 pipeline validate` runs each stage's `validate()` method and produces a combined report with pass/fail per stage and an overall pass/fail exit code.
- **`generate` command remains separate**: The existing `cptools2 generate` workflow (staging → analysis → destaging → join → transfer) is a different use case from the multi-stage pipeline. Both coexist.

## Source File Change Summary

### New files
- `cptools2/pipeline.py` — Pipeline orchestrator: instantiates stages, generates scripts with hold_jid chains, writes master submit script
- `tests/pipeline_config.yaml` — Example config with all pipeline fields populated
- `tests/test_pipeline.py` — Integration tests for full pipeline generation

### Modified files
- `cptools2/__main__.py` — register `pipeline` subcommand with `config_file` argument and optional `validate` sub-subcommand
- `cptools2/parse_yaml.py` — parse `pipeline_stages` config key (list of stage names, optional, defaults to all stages)

## Ralph Loops (3)

| Loop | Name | Type | Key Outputs |
|---|---|---|---|
| 510 | Pipeline orchestrator module | Implementation | `pipeline.py` with stage instantiation, dependency tracking, master submit script generation |
| 520 | CLI wiring and config parsing | Implementation | Updated `__main__.py` (pipeline subcommand), updated `parse_yaml.py` (pipeline_stages key) |
| 530 | Integration tests and example config | Implementation | `test_pipeline.py`, `pipeline_config.yaml`, end-to-end dry-run verification |
