# DINO Consolidation Merge Map

Date: 2026-06-15

## Source Snapshot

| Branch | Worktree | Head | State |
| --- | --- | --- | --- |
| `ai-update` | `C:/Users/mharvey2/Coding/cptools2` | `29de2bd22d83b418f2e0235d65f025b37c0f917a` | dirty |
| `ai-update-DINO` | `C:/Users/mharvey2/Coding/cptools2-DINO` | `32d5b1d690a5bf683ca1e2934fe4023e8355fa80` | clean |

Diff excluding `.claude/**` and `.context/**`:

```text
104 files changed, 10057 insertions(+), 587 deletions(-)
```

This merge map supports
`docs/superpowers/plans/2026-06-09-worktree-remediation-consolidation.md`.

## Merge Strategy

Base the consolidation branch on `ai-update-DINO`, then port selected pause and
Phase 3.1 decision records from `ai-update`.

Do not direct-merge `ai-update` into `ai-update-DINO` or direct-merge
`ai-update-DINO` into `ai-update`. The overlap in shared runtime files is too
large, and the DINO branch also contains repository-hygiene changes that should
not be bundled with feature/runtime consolidation.

## File-Level Buckets

| Path or group | Bucket | Source preference | Reason | Required checks | Status |
| --- | --- | --- | --- | --- | --- |
| `cptools2/dino_embed.py` | DINO successor | DINO | Core DINO embedding implementation. | `tests/test_dino_embed.py` | planned |
| `cptools2/dino_export.py` | DINO successor | DINO | DINO feature table export implementation. | `tests/test_dino_export.py`, export table smoke | planned |
| `cptools2/feature_extract/__init__.py`, `cptools2/feature_extract/contract.py` | Shared model contract | DINO, then review against DeepProfiler | Provides extractor abstraction needed for DINO and useful for future models. | `tests/test_feature_extract_contract.py`, DeepProfiler compatibility tests | planned |
| `cptools2/dockerfiles/Dockerfile.dinov2` | DINO successor | DINO | Required DINO container build. | Dockerfile static review; Eddie build smoke when needed | planned |
| `scripts/eddie_build_dinov2_sif.sh`, `scripts/eddie_smoke_dinov2.sh`, `scripts/eddie_smoke_cell_dino*.sh`, `scripts/eddie_smoke_channel_adaptive_dino_realdata.sh` | DINO successor | DINO | DINO validation scripts and evidence flow. | Shell syntax on Eddie/Git Bash; script path redaction review | planned |
| `nextflow/modules/export_cell_dino.nf` | DINO successor | DINO | DINO stage export module. | `tests/test_dinov2_stageout.py`, architecture smoke | planned |
| `config/dinov2-phase-smoke.yaml`, `config/channel-adaptive-dino-smoke.yaml` | DINO successor | DINO | DINO smoke configs. | Config parse tests; site-path review | planned |
| `tests/test_cell_dino_adapter.py`, `tests/test_cell_dino_crops.py`, `tests/test_channel_adaptive_adapter.py`, `tests/test_dino_real_adapter.py`, `tests/test_dinov2_config.py`, `tests/test_dinov2_stageout.py` | DINO successor | DINO | Direct DINO coverage. | Run focused DINO tests | planned |
| `cptools2/incucyte.py`, `tests/test_incucyte.py`, `tests/test_incucyte_index.py` | DINO input support | DINO | Supports segment-free/phase-image DINO route. | IncuCyte parser/index tests | planned |
| `cptools2/illum.py`, `tests/test_illum.py` | DINO support, manual review | DINO if still used by current DINO flow | Python illumination may be useful but should not overwrite existing CellProfiler assumptions without review. | `tests/test_illum.py`; Nextflow illum route smoke | planned |
| `cptools2/container_manifest_template.json`, `cptools2/containers.py`, `cptools2/dockerfiles/README.md`, `cptools2/dockerfiles/build_containers.sh` | DINO successor | DINO, manual review | Adds DINO container entries while preserving legacy containers. | Container manifest tests; static review | planned |
| `pyproject.toml`, `uv.lock` | Dependency consolidation | DINO, manual review | DINO adds dependencies. Lockfile churn should be accepted only if local tests resolve. | Install/test in clean env if feasible | planned |
| `nextflow/conf/hpc.config` | Shared runtime hardening | DINO | Generic HPC profile broadens portability and can coexist with Eddie. | `nextflow -profile hpc` syntax/static check; architecture tests | planned |
| `nextflow/conf/eddie.config` | Shared runtime hardening | Manually compose | Both branches changed Eddie behavior. Need preserve Phase 3.1 GPU/cleanup lessons and DINO HPC improvements. | `tests/test_eddie_runtime_config.py`; Eddie dry-run | manual-compose |
| `nextflow/main.nf` | Shared runtime plus DINO wiring | Manually compose | Must preserve DeepProfiler legacy path, DINO route, feature export, stage-out evidence, and batch behavior. | `tests/test_nextflow_architecture_smoke.py`; DINO and DeepProfiler route static checks | manual-compose |
| `nextflow/modules/stage_in.nf` | Shared runtime hardening | Manually compose | Thumbnail exclusion and scratch behavior matter for both routes. | architecture tests; Eddie stage-in dry-run | manual-compose |
| `nextflow/modules/stage_out.nf` | Shared runtime hardening | Manually compose | Durable evidence and byte-level verification are data-loss-critical. | `tests/test_nextflow_architecture_smoke.py`, stage-out tests, Eddie stage-out smoke | manual-compose |
| `nextflow/modules/feature_extract.nf` | Shared runtime plus DINO wiring | Manually compose | Must support DeepProfiler legacy and DINO successor without breaking outputs. | DINO tests, DeepProfiler feature export tests | manual-compose |
| `nextflow/modules/build_imageset_index.nf`, `illum_apply.nf`, `illum_calculate.nf` | Shared runtime | Manually compose | DINO may have input/index changes; legacy CellProfiler flow must remain intact. | architecture smoke; illum/index tests | manual-compose |
| `cptools2/__main__.py` | Shared driver | Manually compose | Highest-risk file: batch lifecycle, cleanup, run reports, quota, resume, and DINO additions intersect here. | `tests/test_cli.py`, batch tests, DINO config tests | manual-compose |
| `cptools2/batch.py`, `tests/test_batch.py`, `tests/test_batch_reclaim.py` | Shared batching | DINO plus Phase 3.1 review | DINO contains later scratch quota/reclaim improvements; must keep verified cleanup semantics. | `tests/test_batch.py`, `tests/test_batch_reclaim.py` | manual-compose |
| `cptools2/parse_yaml.py`, `tests/test_parse_yaml.py` | Shared config | Manually compose | DINO config generalization intersects legacy configs. | parse-yaml tests; example config dry-run | manual-compose |
| `cptools2/nextflow_chunking.py` | Shared runtime | Manually compose | DINO may add segment-free chunking support. | chunking-related CLI/Nextflow tests | manual-compose |
| `cptools2/generate_scripts.py`, `cptools2/utils.py` | Shared utilities | DINO if diff is small after review | Likely portability/generalization changes. | focused existing tests | planned |
| `docs/reference/dinov2-phase4-evidence.md`, `docs/reference/model-storage.md`, `docs/reference/phase-3.2-multi-plate-acceptance-evidence.md` | DINO evidence docs | DINO | Captures DINO validation and model storage decisions. | Site-sensitive path review | planned |
| `docs/reference/eddie-scratch-sizing-and-run-prep.md`, `docs/reference/interactive-session-batch-runbook.md`, `docs/reference/site-configuration.md` | Shared operations docs | DINO, then update with DeepProfiler pause if needed | Useful operational docs for DINO route and public package. | Manual doc review | planned |
| `docs/reference/nextflow-config-yaml.md` | Shared operations docs | Manually compose | Both branches updated operator guidance; must include DINO successor and DeepProfiler pause state. | Manual doc review; config examples | manual-compose |
| `docs/reference/deepprofiler-scalability.md`, DeepProfiler historical plans | Legacy docs | Keep as legacy, update only for pause wording | Should not drive active DINO work, but useful history. | Manual doc review | defer |
| `docs/applications/**`, `docs/general/**`, `docs/local-testing-strategy.md`, `docs/reference/storage*.md`, `docs/reference/memory*.md` | Repository hygiene/public docs | Decide separately | DINO generalizes site/lab language. This is good for public release but not required for runtime merge. | Public-doc review | separate-decision |
| `.env.example`, `config/project.md`, `config/active_pipelines.md`, `config/eddie_paths.example.env` | Site config hygiene | Decide separately, likely DINO | Needs site-redaction review and compatibility with current local Eddie setup. | Manual config review; Eddie dry-run | separate-decision |
| `config/multi-plate-batch.example.yaml`, `config/subset-validation-example.yaml` | Public examples | DINO, manual review | Better examples for DINO/public route, but rename may affect docs/tests. | Config parse tests; doc link check | planned |
| `.gitattributes`, `.gitignore`, `CLAUDE.md` | Repository hygiene | Decide separately | DINO includes line-ending policy, redaction, and local-agent tracking changes. Should not block runtime consolidation. | Manual repo policy review | separate-decision |
| `.claude/**`, `.context/**` | Repository hygiene | Defer | DINO stops tracking these. Do not include in runtime consolidation unless explicitly approved. | Separate review/commit | defer |
| `plans/gate-verdicts/**` | Planning history | Defer or keep from current | Not runtime-critical. Avoid churn during DINO merge. | Manual planning review | defer |

## Proposed Execution Order

1. Checkpoint current `ai-update` pause/remediation documentation.
2. Create `codex/dino-consolidation` from `ai-update-DINO`.
3. Add this merge map to the consolidation branch and update source heads.
4. Port DeepProfiler pause decision docs from `ai-update`.
5. Accept DINO-only additions.
6. Manually compose shared runtime files.
7. Defer `.claude/**`, `.context/**`, and broad public-doc cleanup.
8. Run focused local tests.
9. Run Eddie DINO dry-run and one conservative acceptance route.
10. Merge only after verified cleanup and stage-out evidence pass on DINO.

## Verification Set

Local focused tests:

```text
pytest tests/test_feature_extract_contract.py tests/test_dino_embed.py tests/test_dino_export.py tests/test_dinov2_config.py tests/test_dinov2_stageout.py tests/test_batch.py tests/test_batch_reclaim.py tests/test_nextflow_architecture_smoke.py tests/test_eddie_interactive_launcher.py -q
```

Additional compatibility tests:

```text
pytest tests/test_feature_export_deepprofiler.py tests/test_parse_yaml.py tests/test_cli.py -q
```

Eddie checks:

- DINO dry-run from a fresh scratch root.
- One conservative DINO plate/subset run.
- Confirm durable `stage_out_evidence`.
- Confirm verified cleanup reclaims batch work.
- Confirm run report clearly records any partial failures.

## Open Decisions

- Whether to untrack `.claude/**` and `.context/**` in the final public branch.
- Whether the generic `hpc` profile should become the documented default with
  Eddie as the worked example.
- Whether Python illumination becomes production-supported or remains DINO
  validation support.
- Whether DeepProfiler summary-path repair is worth porting for legacy support
  before the DINO consolidation lands.
