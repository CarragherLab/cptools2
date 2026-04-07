---
phase: 1
name: Code Review Fixes + Multi-Assay Hardening
plan: 2026-04-06-code-review-fixes-design.md
status: pending
loops_total: 3
---

# Phase 1: Code Review Fixes + Multi-Assay Hardening

Fix 27 code review issues across 6 commits. Prove multi-assay flexibility with 13 new tests.

## Loop 110: Foundation + Bug Fixes

```yaml
loop_id: 110
name: Foundation + Bug Fixes
status: completed
type: bugfix
key_outputs:
  - Lane A parse_yaml merge-back (VALID_STAGES, resolve_stages, join_files, data_destination)
  - Critical bug fixes (cmd_join function name, check_dataframe_size None guard, is_new_ix stub)
  - test_base64_encoding.py converted to proper pytest style
todos:
  - text: "Merge Lane A parse_yaml improvements: add VALID_STAGES, STAGE_ALIASES, resolve_stages(), join_files(), data_destination(), expanded valid_args"
    status: completed
    skill: null
    agent: null
  - text: "Fix cmd_join: change file_tools.join_output_files() to file_tools.join_plate_files(plate_store=None, raw_data_location=os.path.join(args.location, 'raw_data'), patterns=args.patterns)"
    status: completed
    skill: null
    agent: null
  - text: "Fix file_tools.py:84: replace pl.arange() with pl.int_range() for forward compatibility"
    status: completed
    skill: null
    agent: null
  - text: "Fix loaddata.py check_dataframe_size: add 'if min_rows is None: return' guard"
    status: completed
    skill: null
    agent: null
  - text: "Stub is_new_ix() to unconditionally return False"
    status: completed
    skill: null
    agent: null
  - text: "Convert test_base64_encoding.py to proper pytest: assert instead of return, remove __main__ block, parametrize, remove print() calls"
    status: completed
    skill: null
    agent: null
  - text: "Update test_expand_stage_aliases to test_resolve_stages with validation tests"
    status: completed
    skill: null
    agent: null
  - text: "Run pytest tests/ -v and verify 114+ tests pass"
    status: completed
    skill: null
    agent: null
```

## Loop 120: Nextflow + Design Fixes

```yaml
loop_id: 120
name: Nextflow + Design Fixes
status: completed
type: implementation
depends_on: [110]
key_outputs:
  - main.nf stage gating fixed (List/String handling, expanded names)
  - publishDir on all 4 modules
  - --nv flag for GPU containers
  - _append_illum_columns channel-agnostic
  - Container path resolution through parse_config_file
  - cellpose_sam in containers.config
handoff_summary:
  done: "main.nf stage gating fixed; publishDir on all 4 modules; --nv in eddie.config gpu block; illum_apply.nf documented; _append_illum_columns channel-agnostic; cellpose_sam in containers.config and DEFAULT_CONTAINERS; resolved_containers in parse_config_file/generate_params_json; 128 tests pass"
  failed: ""
  needed: "Loop 130: dead code removal, _prepare_config() helper, version bump to 0.3.0, .gitignore Nextflow entries, 13 new multi-assay tests"
todos:
  - text: "Fix main.nf stage gating: handle both List and String for params.stages, check for expanded names (illum_calculate, illum_apply, segmentation, feature_extract)"
    status: completed
    skill: null
    agent: null
  - text: "Add publishDir to illum_calculate.nf, illum_apply.nf, segmentation.nf, feature_extract.nf with per-plate/per-stage structure"
    status: completed
    skill: null
    agent: null
  - text: "Add containerOptions = '--nv' to eddie.config withLabel: 'gpu' block"
    status: completed
    skill: null
    agent: null
  - text: "Add comment to illum_apply.nf documenting that CellProfiler reads illum function paths from the .cppipe file"
    status: completed
    skill: null
    agent: null
  - text: "Make _append_illum_columns channel-agnostic: detect channel numbers from FileName_W* columns instead of hardcoded range(1, 6)"
    status: completed
    skill: null
    agent: null
  - text: "Add cellpose_sam entry to containers.config"
    status: completed
    skill: null
    agent: null
  - text: "Add container .sif path resolution in parse_config_file() using containers.resolve_container_path(), pass through config_dict to generate_params_json"
    status: completed
    skill: null
    agent: null
  - text: "Run pytest tests/ -v and verify no regressions"
    status: completed
    skill: null
    agent: null
```

## Loop 130: Cleanup + Multi-Assay Tests

```yaml
loop_id: 130
name: Cleanup + Multi-Assay Tests
status: pending
type: testing
depends_on: [120]
key_outputs:
  - Dead code removed (EddieNodeError, debug prints, unused imports)
  - DRY: _prepare_config() helper extracted
  - Version aligned to 0.3.0
  - .gitignore updated for Nextflow
  - 13 new tests proving multi-assay flexibility
todos:
  - text: "Remove EddieNodeError dead class from __main__.py"
    status: pending
    skill: null
    agent: null
  - text: "Extract _prepare_config() helper to deduplicate cmd_pipeline/cmd_prepare shared logic"
    status: pending
    skill: null
    agent: null
  - text: "Remove duplicate resolve_stages() call inside generate_params_json (CLI already expands)"
    status: pending
    skill: null
    agent: null
  - text: "Remove unused imports: subprocess/sys from test_cli.py, subprocess/shutil from test_base64_encoding.py"
    status: pending
    skill: null
    agent: null
  - text: "Update nextflow.config manifest version from 0.1.0 to 0.3.0"
    status: pending
    skill: null
    agent: null
  - text: "Resolve main.nf path relative to package root using os.path.dirname(os.path.abspath(__file__))"
    status: pending
    skill: null
    agent: null
  - text: "Add .nextflow/, .nextflow.log*, work/ to .gitignore"
    status: pending
    skill: null
    agent: null
  - text: "Remove debug print() statements in generate_scripts.py, replace with pretty_print()"
    status: pending
    skill: null
    agent: null
  - text: "Add 13 multi-assay tests: _append_illum_columns 3/7/0 channels, resolve_stages None/empty/dedup/validation, check_dataframe_size None, generate_params_json custom channels + containers, cmd_join mock, enrich idempotency, CLI --stages dry-run"
    status: pending
    skill: null
    agent: null
  - text: "Run pytest tests/ -v and verify 127+ tests pass (114 existing + 13 new)"
    status: pending
    skill: null
    agent: null
```
