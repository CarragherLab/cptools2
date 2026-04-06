# Phase E: Integration, Validation & Review

## Objective

Merge all 4 parallel lanes, resolve any integration conflicts, run the full test suite, perform code review with eddie plugin validation, and execute end-to-end pipeline testing on Eddie to confirm everything works together.

## Scope

### Included:
- Merge Lane A (polars), Lane B (Nextflow), Lane C (containers), Lane D (CLI/config) into ai-update
- Resolve any merge conflicts between lanes
- Full test suite: `pytest tests/ -v` (Python) + `nextflow run -profile test` (Nextflow)
- Code review with `/code-review:code-review` on the merged diff
- Eddie script validation with `eddie:eddie-validate` on generated Nextflow configs and scripts
- End-to-end pipeline test on Eddie: `cptools2 pipeline config.yml -profile eddie` with real images
- Scratch quota pre-flight check implementation (critical gap from eng review)
- CLAUDE.md update to reflect new architecture
- README.md update for new CLI and Nextflow usage
- Final `pyproject.toml` verification (dependencies, version bump)

### Explicitly NOT included:
- New features beyond what Lanes A-D deliver
- nf-core submission
- Multi-cluster (SLURM/PBS) testing
- DINOv2 container
- QC dashboard/reporting implementation

## Key Deliverables

| Deliverable | Format | Location |
|---|---|---|
| Merged ai-update branch | Git branch | `ai-update` |
| Code review report | Markdown | `.claude/plans/phase-E-code-review.md` |
| Eddie validation report | Markdown | `.claude/plans/phase-E-eddie-validation.md` |
| End-to-end test results | Log | Eddie scratch |
| Updated CLAUDE.md | Markdown | `CLAUDE.md` |
| Updated README.md | Markdown | `README.md` |
| Scratch quota check | Python | `cptools2/pipeline.py` or `__main__.py` |

## Success Criteria

- ✓ All 4 lanes merged into ai-update without conflicts — verified by `git merge` succeeding cleanly or conflicts resolved
- ✓ `pytest tests/ -v` passes with 0 failures — all Python tests green
- ✓ `nextflow run nextflow/main.nf -profile test` passes with exit 0 — local CI pipeline works
- ✓ `/code-review:code-review` on the full ai-update diff against master produces no P1 findings
- ✓ `eddie:eddie-validate` approves all generated scripts and configs (APPROVED or APPROVED WITH WARNINGS)
- ✓ End-to-end on Eddie: `cptools2 pipeline tests/pipeline_config.yaml -profile eddie` completes with a real plate producing illumination-corrected images
- ✓ Containers resolve correctly from manifest on Eddie
- ✓ Nextflow uses `h_rss` (not `h_vmem`) in all generated SGE scripts — verified by `qacct -j` on completed jobs
- ✓ `cptools2 pipeline --dry-run` generates valid params.json locally
- ✓ Scratch quota pre-flight check warns if estimated pipeline storage exceeds 75% of available scratch
- ✓ CLAUDE.md reflects Nextflow architecture, not custom Python orchestrator
- ✓ README.md documents `cptools2 pipeline`, `cptools2 prepare`, channel configuration, and container setup

## Dependencies

### Must Complete Before:
- Lane A (Phase A): polars migration complete
- Lane B (Phase B): Nextflow pipeline complete
- Lane C (Phase C): containers deployed and validated on Eddie
- Lane D (Phase D): CLI and config refactor complete

### Blocked By:
- All 4 lanes must be complete before integration begins

### Optional:
- Nothing — this is the final phase

## Skills Required (Broad Categories)

- `code-review:code-review`: full diff review of ai-update vs master
- `eddie:eddie-validate`: script and config validation
- `eddie:eddie-login`: SSH access for end-to-end testing
- `eddie:eddie-resources`: verify resource allocations in running jobs
- `python-testing`: full test suite verification

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Merge conflicts between lanes | Low | Medium | Lanes touch different files by design. Only pyproject.toml is shared (Lane A adds polars, Lane D removes scissorhands — both edits, different sections). |
| Integration test reveals incompatibility between polars DataFrames and Nextflow data_prep process | Medium | Medium | data_prep.nf process calls cptools2 Python scripts as subprocesses; DataFrame format stays internal to Python |
| Eddie end-to-end test fails due to container path or config mismatch | Medium | Medium | Validate config and container paths individually before full pipeline test |
| Scratch quota check is too conservative/aggressive | Low | Low | Calibrate against actual plate sizes from test run |

## Assumptions

- `All 4 lanes deliver what their phase plans specify`: each lane has its own success criteria and test suite
- `Git worktree merges are clean`: lanes were designed for file isolation
- `Eddie access available for end-to-end testing`: SSH + compute node queue time

## Notes / Design Decisions

- **This phase is sequential, not parallel**: it runs AFTER all 4 lanes complete
- **Code review uses full diff**: `git diff master..ai-update` is the review scope, covering all changes across all lanes
- **Eddie validation uses the eddie plugin skills**: `eddie:eddie-validate` produces APPROVED/BLOCKED verdicts on generated scripts
- **End-to-end test uses minimal data**: one plate, one batch, to verify the full pipeline works without burning GPU hours
- **Scratch quota check**: pre-flight check in the `pipeline` subcommand that estimates total storage (images × stages × plates) and compares against `df` output on scratch. Warns if >75% of available space.
- **README and CLAUDE.md updates**: these capture the new architecture for future developers and AI assistants

## Ralph Loops (4)

| Loop | Name | Type | Key Outputs |
|---|---|---|---|
| E10 | Merge lanes + resolve conflicts + full test suite | Implementation | Clean merged branch, all Python + Nextflow tests passing |
| E20 | Code review + eddie validation | Review | /code-review report, eddie:eddie-validate verdicts, findings resolved |
| E30 | End-to-end Eddie pipeline test | Implementation | Successful pipeline run on Eddie, qacct verification, scratch quota check |
| E40 | Documentation + cleanup | Implementation | Updated CLAUDE.md, README.md, pyproject.toml version bump |
