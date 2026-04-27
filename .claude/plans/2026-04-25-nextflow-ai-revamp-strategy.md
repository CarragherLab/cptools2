# cptools2 Nextflow AI Revamp Strategy

Date: 2026-04-25
Status: DONE_WITH_CONCERNS
Mode: Builder / product strategy
Source context:

- `PROJECT_CONTEXT.md`
- `.claude/plans/phase-2-eddie-deployment.md`
- `.claude/plans/phase-2-ralph-loops.md`
- Notion page: "AI Embedding Models - DINO", fetched 2026-04-25

## One-Line Strategy

Rebuild cptools2 as a Nextflow-first, Eddie-ready high-content imaging platform that preserves proven SGE-era setup mechanics while adding modular analysis engines: Cellpose for segmentation, CellProfiler for classical workflows, and user-selected AI feature extractors starting with DeepProfiler, then Cell-DINO and uniDINO.

## Problem Statement

cptools2 has strong working ideas from the SGE/CellProfiler era, but the project is now trying to become something sharper: a Nextflow-orchestrated imaging platform that can run classical CellProfiler workflows, Cellpose segmentation, and modern AI feature extractors on Eddie.

The risk is that the project gets pulled in two directions at once:

- Infrastructure work: make Nextflow, Eddie, staging, containers, scratch handling, and real-plate execution work.
- Product architecture work: define how assays, channels, stains, segmentation engines, selected feature extractors, method-native exports, resources, and minimum traceability metadata are represented.

If those are blurred together, Loop 230 becomes too big and the AI roadmap becomes a pile of one-off modules. If they are separated cleanly, Phase 2 proves the substrate and Phase 3 defines the engine contract that Cellpose, CellProfiler, and future extractors build against.

## What Makes This Worth Building

The valuable thing is not another wrapper around CellProfiler.

The valuable thing is a practical path from raw high-content imaging data to reproducible, cluster-scale segmentation and feature exports: Cellpose masks, CellProfiler outputs, DeepProfiler features, DINO embeddings, and method-native outputs that remain traceable to plate, well, site, and source image.

That matters because scientists should not have to rebuild the same Eddie staging, container, batching, and traceability plumbing every time they want to run Cellpose, CellProfiler, DeepProfiler, Cell-DINO, uniDINO, or comparison outputs.

## Why This Matters

The old cptools2 solved a real operational problem: turning high-content microscopy data into cluster jobs that CellProfiler could run reliably. That problem still matters.

The new opportunity is larger. Modern image-analysis workflows are no longer just CellProfiler feature tables. They need segmentation, learned feature extractors, GPU workloads, and reproducible execution on shared HPC infrastructure.

The goal is not "convert SGE scripts to Nextflow." That is the plumbing.

The goal is: make high-content imaging segmentation and feature extraction reproducible, extensible, and AI-ready for real biological screens.

## What We Have

### Existing Strengths

- Legacy SGE implementation with practical domain logic for plate discovery, image chunking, LoadData generation, batching, command construction, and CSV joining.
- Python CLI package with tests and a clear install path.
- Nextflow skeleton with Eddie profile, stage modules, staging processes, and batch-aware invocation from `cptools2 pipeline`.
- Container manifest concept covering the four core container areas: `cptools2`, CellProfiler, Cellpose, and AI feature-extraction models.
- Eddie deployment plan with install script, staging queue design, scratch quota handling, and an end-to-end validation loop.
- DINO documentation that defines three concrete embedding model routes, with downstream analysis deliberately out of scope for the current cptools2 revamp.

### Current Gaps

- The active Phase 2 plan proves the Eddie substrate, but it does not yet define the full AI-enabled product architecture.
- Docs still contain tension between legacy `generate` and new `pipeline` command.
- The configuration schema for multi-engine workflows is not yet explicit.
- DINO model-selection guidance is described in documentation, but user-selected extractors are not yet reflected in repository plans, config schema, containers, or Nextflow modules.
- Output contracts are not yet defined for Cellpose masks, CellProfiler exports, DeepProfiler features, Cell-DINO embeddings, uniDINO embeddings, and any baseline setup artifacts.

## Premises To Align On

1. Nextflow is the future orchestration layer.

The old SGE script path should be treated as a reference implementation for behavior, not the main product surface.

2. Eddie validation is a substrate milestone, not the finish line.

Loop 230 should prove that deployment, staging, containers, scratch handling, and Nextflow execution work on real data. It should not try to absorb the full AI feature roadmap.

3. The hard product problem is the engine contract.

The platform needs to know what kind of experiment it is running and which engines the user has selected: modality, channel count, stain set, segmentation engine, feature extractors, resources, and output expectations. Without that, each engine becomes a one-off workflow.

4. Segmentation and feature extraction engines should be modular.

Cellpose, CellProfiler, DeepProfiler, Cell-DINO, and uniDINO should look like pluggable stages with explicit input, output, container, GPU, and metadata requirements. Cellpose is a vital segmentation engine, not an optional later add-on. Single-cell outputs are in scope at the end of the pipeline, but should be implemented after the image/site contract unless an engine makes them cheap to expose earlier.

5. Minimum traceability is a first-class feature.

Every output must carry or be joinable to `plate_name`, `well`, `site`, and `image_path`. Rich plate metadata can come later or live outside cptools2; the first contract should guarantee per-site/per-image traceability.

6. Licence constraints are documentation constraints.

The currently documented DINO models are research/non-commercial licensed. That does not block academic use, but the warning belongs in the project documentation and licence notes rather than in runtime logic.

## Constraints

- Eddie is the primary execution target.
- Nextflow is the future orchestration layer.
- SGE remains relevant through Eddie's scheduler and as a source of legacy design knowledge.
- DataStore access requires staging-node awareness.
- GPU model stages must be explicit about queue, GPU, memory, and container requirements.
- DINO model weights and licences need deliberate handling.
- cptools2 should focus on segmentation, per-site/per-image feature extraction, and eventually single-cell feature outputs, not aggregation, downstream analysis, MoA prediction, or hit calling.
- Engine outputs should preserve method-native exports where possible rather than forcing every method into one rigid schema.
- The repo already has dirty planning state; planning changes should avoid trampling unrelated edits.
- Phase 2 should stay narrow enough to finish.

## Strategy Choice

### Alternative A: Finish Eddie First, Then Redesign AI Features

Do Loop 230 exactly as written, validate the existing Nextflow path, then start a new phase for AI model architecture.

Pros:

- Lower risk.
- Keeps the current deployment work focused.
- Produces a working baseline before adding GPU-heavy model complexity.

Cons:

- Current plans remain narrow and may understate the real destination.
- Some Phase 2 decisions may need rework if the AI schema is ignored too long.

### Alternative B: Pause Eddie Validation And Redesign Everything Now

Stop Loop 230 and rewrite the plan around DeepProfiler and DINO before further Eddie testing.

Pros:

- Clean architecture can be designed before more code accumulates.
- Better chance to avoid config and output-contract rework.

Cons:

- High risk of analysis paralysis.
- No proof that the base Nextflow/Eddie substrate works.
- AI containers and model weights add uncertainty before the foundation is validated.

### Alternative C: Keep Loop 230, Add A Parallel Architecture Track

Proceed with Loop 230 as the infrastructure proof, while adding a new architecture/design track that defines the feature-extraction workflow contract, user-selected extractors, output traceability rules, and next phases.

Pros:

- Keeps momentum on Eddie.
- Uses real validation to inform the bigger architecture.
- Avoids stuffing all future product ambition into one overloaded loop.
- Gives future agents a clear north star.

Cons:

- Requires discipline to keep Loop 230 narrow.
- Requires updating plan docs so the project does not look smaller than it is.

Recommended approach: Alternative C.

## Updated Goal Hierarchy

### Goal 1: Prove The Cluster Substrate

Users can install cptools2 on Eddie, run `cptools2 pipeline config.yml`, stage data, execute Nextflow processes, respect scratch constraints, and collect outputs.

Success measures:

- `install_eddie.sh` completes on Eddie.
- `cptools2 pipeline test_config.yaml --dry-run` produces correct `params.json`.
- At least one real plate runs through the current Nextflow stages.
- Nextflow report and timeline are produced.
- Scratch usage stays within the intended batching target.

### Goal 2: Define The Segmentation And Feature-Extraction Workflow Contract

Users can describe an experiment once and explicitly choose segmentation and feature-extraction engines.

Required schema concepts:

- `modality`: fluorescence, phase_contrast, brightfield, mixed.
- `channels`: count, names, stains, file patterns.
- `assay_type`: standard_cell_painting, custom_fluorescence, phase_contrast_screen, other.
- `segmentation`: selected engine and settings, with Cellpose as the primary segmentation engine.
- `cellprofiler`: optional classical workflow engine and pipeline settings.
- `feature_extractors`: selected engines, for example `deepprofiler`, `cell_dino`, `unidino`.
- `outputs`: requested method-native exports and traceability sidecars.
- `resources`: CPU, GPU, memory, queue, container overrides.

Success measures:

- Config can express CellProfiler, Cellpose, and AI feature-extraction paths without special-case CLI flags.
- Config can run baseline setup tasks before AI extraction.
- Config can run Cellpose as the core segmentation engine.
- Config can run DeepProfiler first as the initial AI extractor.
- Config can run Cell-DINO, uniDINO, or both when users want comparison outputs.

### Goal 3: Make Analysis Engines Modular

Each analysis engine should have a small contract:

- Inputs it accepts.
- Outputs it emits.
- Container image and weights it needs.
- Whether it requires GPU.
- How outputs preserve or join back to `plate_name`, `well`, `site`, and `image_path`.

Initial engines:

- Baseline setup tasks: image discovery, staging, channel handling, and traceability metadata.
- cptools2: orchestration, config parsing, staging metadata, and Nextflow parameter generation.
- CellProfiler: classical workflow execution and existing feature-table behavior.
- Cellpose: segmentation masks and segmentation metadata.
- DeepProfiler: learned features.
- Cell-DINO: standard Cell Painting embeddings.
- uniDINO: variable-channel fluorescence embeddings.
- DINOv2 baseline: optional later baseline for phase contrast and brightfield embeddings.

Success measures:

- Each engine maps cleanly to a Nextflow module or module family.
- Container manifest can represent the four core areas: `cptools2`, CellProfiler, Cellpose, and feature-extraction models.
- Engine outputs keep method-native files plus required traceability fields.
- GPU requirements are visible in config and Nextflow labels.

### Goal 4: Keep Outputs Traceable Without Owning Downstream Analysis

cptools2 should stop at segmentation and per-site/per-image feature extraction for this revamp.

Core rules:

- Focus on per-site/per-image extraction.
- Preserve `plate_name`, `well`, `site`, and `image_path` in every output path or output table.
- Infer batch from `plate_name` where needed rather than requiring full plate metadata up front.
- Work with method-native exports from Cellpose, CellProfiler, DeepProfiler, Cell-DINO, and uniDINO.
- Treat single-cell outputs as an end-of-pipeline feature path, after segmentation and image/site extraction contracts are stable.
- Leave aggregation, downstream phenotype analysis, MoA classification, and hit calling to later phases or separate tools.

Success measures:

- Minimum traceability schema is defined.
- Method-native segmentation and feature outputs are preserved and documented.
- Users can compare DeepProfiler, Cell-DINO, and uniDINO outputs because each export is linked to the same image/site identity.

## DINO-Specific Product Decisions

### Selection

Use explicit user selection as the first rule:

```text
if user selects deepprofiler:
    run DeepProfiler
if user selects cell_dino:
    run Cell-DINO
if user selects unidino:
    run uniDINO
if user selects more than one:
    run each extractor separately and preserve comparable traceability fields
```

Assay metadata can provide validation hints and documentation guidance, but it should not silently override user-selected extractors in the first implementation.

### Model Contracts

Cell-DINO:

- Best for standard 5-channel Cell Painting.
- ViT-L/16.
- 384 x 384 crops.
- 1024-dimensional embeddings.
- Weight path should resolve to `/weights/cell_dino_cp.pth` inside the container.

uniDINO:

- Best for variable-channel fluorescence assays.
- ViT-S/16.
- Per-channel 384-dimensional embeddings.
- Final embedding size is `384 * n_channels`.
- Weight path should resolve to `/weights/unidino.pth`.

DINOv2 baseline:

- Optional later baseline for phase contrast or brightfield.
- Single-channel images are triplicated to pseudo-RGB.
- 1024-dimensional embeddings.
- LoRA fine-tuning is a later validation phase, not a first implementation requirement.

### Licence Constraint

The current DINO model set is research/non-commercial licensed. The plan should say this clearly in documentation and licence notes anywhere DINO features are described. Do not accidentally imply commercial deployment readiness.

## Proposed Phase Updates

### Keep Phase 2: Eddie Deployment

Purpose: prove the substrate.

Refinement:

- Rename or clarify Phase 2 as "Eddie substrate validation" in docs.
- Keep Loop 230 focused on one real plate and current pipeline stages.
- Do not add DINO to Loop 230 unless needed as a smoke-test-only container proof.

### Add Phase 3: Segmentation And Feature-Extraction Contract

Purpose: define the config schema, engine contracts, method-native outputs, container areas, and minimum traceability model before adding more modules.

Suggested loops:

1. `310-config-contract`: design YAML schema for assay metadata, selected segmentation engine, selected feature extractors, outputs, and resources.
2. `320-engine-contracts`: define module contracts for cptools2, CellProfiler, Cellpose, DeepProfiler, Cell-DINO, and uniDINO.
3. `330-output-contract`: define minimum output metadata: `plate_name`, `well`, `site`, and `image_path`.
4. `340-container-contract`: define the four container areas and how versions/paths are recorded.
5. `350-plan-review`: engineering review of schema, module boundaries, and backwards compatibility.

### Add Phase 4: Segmentation And AI Feature-Extractor Implementation

Purpose: implement the modular segmentation and AI feature-extraction engines incrementally.

Suggested loops:

1. `410-baseline-setup`: simplest setup tasks needed before AI extraction.
2. `420-cellpose-module`: Cellpose segmentation module, container, GPU/CPU profile, and method-native mask export handling.
3. `430-deepprofiler-module`: DeepProfiler module, container, GPU profile, and method-native feature export handling.
4. `440-cell-dino-module`: Cell-DINO module, container, GPU profile, and embedding export handling.
5. `450-unidino-module`: uniDINO module, container, GPU profile, and embedding export handling.
6. `460-single-cell-output-path`: expose single-cell feature outputs when supported by Cellpose/DeepProfiler/CellProfiler without forcing aggregation.
7. `470-ai-e2e-validation`: one fluorescence and one phase contrast validation run.

### Add Phase 5: Product Hardening

Purpose: make this usable by a scientist who is not inside the implementation.

Suggested loops:

1. `510-docs-and-examples`: canonical config examples for baseline setup, Cellpose, CellProfiler, DeepProfiler, Cell-DINO, uniDINO, and multi-engine comparison.
2. `520-error-messages`: validate configs before cluster submission and fail with useful messages.
3. `530-reproducibility`: manifest all containers, model weights, versions, params, and output provenance.
4. `540-release-readiness`: package, docs, Eddie install, known limitations, licence notes.

## What To Update Now

High-confidence documentation updates:

- `README.md`: make `cptools2 pipeline` canonical and mark `generate` as legacy.
- `AGENTS.md` and `CLAUDE.md`: update architecture sections to say this branch is Nextflow-first and SGE is reference-only unless explicitly restored.
- `.claude/plans/phase-2-eddie-deployment.md`: clarify that Phase 2 proves the Eddie substrate, not the full AI roadmap.
- `.claude/plans/PLANS-INDEX.md`: add this strategy doc and show future Phase 3/4/5 placeholders.
- `PROJECT_CONTEXT.md`: already updated with user-selected extractor scope and DINO model notes.

Planning updates after that:

- Create `phase-3-segmentation-and-feature-extraction-contract.md`.
- Create `phase-4-segmentation-and-ai-feature-extractor-implementation.md`.
- Create `phase-5-product-hardening.md`.

## What Not To Do Yet

- Do not build DINO modules before the config, segmentation, and output contracts are clear.
- Do not overload Loop 230 with Cellpose, DeepProfiler, and DINO.
- Do not preserve the old `generate` command just because docs still mention it. Decide deliberately.
- Do not add downstream analysis to cptools2 in this revamp.
- Do not design single-cell as aggregation. It is an output granularity, not a downstream analysis stack.
- Do not treat segmentation or feature extraction as complete until `plate_name`, `well`, `site`, and `image_path` are guaranteed.

## Open Questions

1. What are the exact easiest setup tasks that should form the baseline before Cellpose and DeepProfiler?
2. Which Cellpose mask/export files define success for the first segmentation engine?
3. Which DeepProfiler export files define success for the first usable feature extractor?
4. Where should model weights live on Eddie, and who owns updating them?
5. Should DINOv2 remain in the roadmap now, or should the plan focus only on Cell-DINO and uniDINO?
6. What exact metadata naming convention should be used: `plate_name`, `well`, `site`, `image_path`, or CellProfiler-style `Metadata_*` names?
7. For single-cell outputs, what minimum object identity should be guaranteed in addition to plate/well/site/image path: `object_id`, `x`, `y`, mask label, or all of these?

## Success Criteria

### Office-Hours Success Criteria

- The project direction is stated in one sentence.
- Phase 2 is reframed as Eddie substrate validation, not the full product.
- Phase 3 is identified as the segmentation and feature-extraction contract phase.
- DINO is incorporated as real product input, not a vague future feature.
- The plan names what not to build yet.

Status: met, with open questions listed above.

### Product Success Criteria

- A scientist can describe an assay once in config and explicitly select Cellpose, CellProfiler, DeepProfiler, Cell-DINO, uniDINO, or a comparison run.
- A developer can add a new segmentation or feature-extraction engine without rewriting the whole workflow.
- Eddie runs are reproducible: container versions, model weights, params, logs, reports, and output schemas are recorded.
- Masks, embeddings, and features always remain joinable to `plate_name`, `well`, `site`, and `image_path`.
- The first full validation proves both infrastructure and one useful segmentation plus feature-extraction path.

## Distribution Plan

Primary distribution remains a Python CLI package plus project-owned Eddie deployment:

- Python package: installed locally or on Eddie via `pip install -e .[dev]`, `uv`, or a future release install.
- Eddie deployment: `scripts/install_eddie.sh` creates the shared project environment, Nextflow location, and activation script.
- Workflow execution: `cptools2 pipeline config.yml` invokes Nextflow with the Eddie profile.
- Containers: Singularity `.sif` files live under the project container directory on Eddie.
- CI: `.gitlab-ci.yml` includes Eddie validation templates from `mharvey2/eddie-for-agents`.

Future distribution needs:

- Canonical example configs for baseline setup, Cellpose, CellProfiler, DeepProfiler, Cell-DINO, uniDINO, and multi-engine comparison.
- A versioned container and model-weight manifest covering `cptools2`, CellProfiler, Cellpose, and feature-extraction models.
- Clear licence notes for non-commercial DINO model usage.

## The Assignment

The next concrete action is not to implement DINO.

The assignment is:

1. Finish enough of Loop 230 to prove the Eddie substrate with one real plate.
2. Update stale docs so `cptools2 pipeline` is canonical and `generate` is clearly legacy/reference-only.
3. Draft `phase-3-segmentation-and-feature-extraction-contract.md` with the YAML schema, engine contracts, container areas, method-native output handling, and minimum traceability rules.

The third item is the keystone. Once that exists, Cellpose, DeepProfiler, Cell-DINO, and uniDINO work can proceed without turning into disconnected feature branches.

## What I Noticed

- You framed this as "a brand new revamp which is nextflow orchestrated." That is the correct level of ambition. This is not a patch series.
- You said the SGE script has "many good elements" and should provide "ideas, and methods." That distinction is important: preserve the behavior, not necessarily the interface.
- You added DINO documentation before asking for implementation. Good order. Model choice, output traceability, and licence constraints are product architecture, not just container build details.
- The strongest product instinct here is the narrowing from broad downstream analysis to segmentation plus per-site/per-image feature extraction. That is the scope that can become useful quickly without swallowing the whole analysis stack.
- The single-cell correction is important: single-cell belongs in scope as an output granularity at the end of the pipeline, not as aggregation or downstream biological interpretation.

## Plan Engineering Review

Date: 2026-04-25
Review skill: `/plan-eng-review`
Status: ISSUES_OPEN

### Scope Challenge

The plan direction is right, but the implementation sequence needs one correction:

- Phase 2 should remain an Eddie substrate proof.
- Phase 3 should define the contract for engines, outputs, containers, and object identity.
- Phase 4 should implement engines in this order: baseline setup, Cellpose, DeepProfiler, Cell-DINO, uniDINO, single-cell output path.

Do not treat single-cell as "downstream analysis." It is an output level. The mistake to avoid is bolting it onto the end without stable object identity.

### Current Data Flow

```text
config.yml
  |
  v
cptools2 parse/prepare
  |
  v
params.json
  |
  v
Nextflow per plate
  |
  +--> stage_in, optional DataStore -> scratch
  |
  +--> illumination, CellProfiler today
  |
  +--> segmentation, CellProfiler today, Cellpose target
  |
  +--> feature engines, DeepProfiler/DINO target
  |
  +--> single-cell outputs, late-path target
  |
  v
method-native exports + traceability sidecars
```

### Architecture Issues

1. **Current code supports one feature engine, but the product plan requires multiple selected engines.**

Evidence: `nextflow/main.nf` uses one `params.feature_extraction_tool`; `nextflow/modules/feature_extract.nf` switches on that one value. This cannot express DeepProfiler plus Cell-DINO comparison runs cleanly.

Recommendation: Phase 3 must define `feature_extractors` as a list and Phase 4 should split feature engines into separate module contracts rather than keep growing one switch block.

2. **Segmentation is still CellProfiler-shaped in implementation.**

Evidence: `nextflow/modules/segmentation.nf` runs CellProfiler `nuclear_segmentation.cppipe`; the current plan now says Cellpose is the vital segmentation engine.

Recommendation: create a segmentation engine contract before coding Cellpose. The contract should state image inputs, channel selection, mask format, object table format, GPU/CPU mode, and traceability fields.

3. **Container roles are not yet aligned with the four-container architecture.**

Evidence: `cptools2/containers.py` has defaults for CellProfiler, DeepProfiler, and Cellpose, but no `cptools2` role or DINO roles. `nextflow/conf/containers.config` has `dinov2`, but the Python resolver does not.

Recommendation: Phase 3 should define one manifest schema used by both Python and Nextflow. Do not let Python and Nextflow invent separate role names.

4. **Single-cell needs object identity before implementation.**

If single-cell outputs are added without `object_id`, mask label, centroid, plate, well, site, and image path, the output will be hard to trust or join.

Recommendation: add an object-level identity contract in Phase 3, even if implementation waits until Phase 4.

### Test Review

Current Phase 2 tests cover batch orchestration well, but the future plan needs contract tests before engine implementation:

- Config contract tests: selected segmentation engine, selected feature extractor list, invalid engine names, multi-engine comparison config.
- Params generation tests: `params.json` preserves engine lists and container role mappings.
- Container manifest tests: every declared engine resolves to one versioned role.
- Output contract tests: image/site exports include `plate_name`, `well`, `site`, `image_path`; single-cell exports also include object identity.
- Nextflow dry-run or static tests: engine selection expands to expected modules.

Critical gap: silent empty outputs are currently possible in `feature_extract.nf` because `cp -r ... features/ 2>/dev/null || true` can hide missing DeepProfiler output. Future engine modules should fail loudly when expected output files are absent.

### Performance Review

- GPU scheduling must be per engine, not per generic feature stage. DeepProfiler, Cell-DINO, uniDINO, and Cellpose may have different GPU/CPU modes and memory needs.
- Single-cell output volume can be much larger than per-site embeddings. It needs explicit output format and compression decisions before it lands.
- Multi-engine comparison runs will multiply staging and GPU costs unless corrected images, masks, and manifests are reused.

### Failure Modes

| Failure mode | Test exists | Error handling exists | User sees clear error | Severity |
|--------------|-------------|-----------------------|-----------------------|----------|
| Engine name is accepted in YAML but ignored by Nextflow | no | partial | no | high |
| DeepProfiler produces no files but process exits 0 due to `|| true` | no | no | no | critical |
| Cellpose mask exported without object table or traceability | no | no | no | high |
| Python and Nextflow disagree on container role names | partial | partial | maybe | high |
| Single-cell output lacks stable object IDs | no | no | no | high |
| Multi-engine run overwrites `features/` output directory | no | no | no | high |

### What Already Exists

- `cptools2/batch.py`: reuse for scratch-aware plate batching.
- `cptools2/containers.py`: reuse as the starting point for manifest-based role resolution, but extend role coverage.
- Legacy `filelist.py`, `splitter.py`, `loaddata.py`: reuse parsing and image-list mechanics where possible.
- `nextflow/modules/stage_in.nf` and `stage_out.nf`: keep as substrate modules.
- `nextflow/modules/feature_extract.nf`: treat as a prototype, not the final multi-engine abstraction.

### NOT In Scope

- Aggregation to well-level summaries: separate analysis layer.
- UMAP, clustering, MoA, and hit calling: downstream analysis, not cptools2 core.
- Commercial DINO readiness: blocked by model licences.
- Rebuilding every legacy `generate` behavior: only preserve useful mechanics.
- Full single-cell analysis: in scope only as output generation and identity preservation, not interpretation.

### Parallelization Strategy

| Step | Modules touched | Depends on |
|------|-----------------|------------|
| Engine config contract | `cptools2/`, `tests/` | Phase 2 substrate |
| Container manifest contract | `cptools2/`, `nextflow/conf/`, `tests/` | Engine config contract |
| Cellpose module | `nextflow/modules/`, `nextflow/main.nf`, `tests/` | Engine config contract |
| DeepProfiler module cleanup | `nextflow/modules/`, `nextflow/main.nf`, `tests/` | Engine config contract |
| DINO modules | `nextflow/modules/`, `nextflow/main.nf`, `tests/` | Container manifest contract |
| Single-cell output path | `nextflow/modules/`, output contract tests | Cellpose module, DeepProfiler module cleanup |

Parallel lanes:

- Lane A: engine config contract -> container manifest contract.
- Lane B: Phase 2 Eddie validation, can continue in parallel because it proves substrate.
- Lane C: docs/examples for configs, can start after the contract draft.
- Later lanes: Cellpose and DeepProfiler can proceed in parallel after the engine contract, but both touch `nextflow/main.nf`, so merge carefully.

### Recommended TODOs

1. Draft Phase 3 contract with `segmentation`, `cellprofiler`, `feature_extractors`, `single_cell_outputs`, `containers`, and `outputs`.
2. Add a contract test suite for config parsing and `params.json` generation before coding engine modules.
3. Remove silent success paths from feature extraction modules.
4. Define Cellpose success outputs: mask files, object table, and traceability sidecar.
5. Define single-cell identity fields before implementing single-cell feature outputs.
6. Unify Python and Nextflow container role names.

### Completion Summary

- Step 0: Scope Challenge: scope adjusted, single-cell retained as late output granularity.
- Architecture Review: 4 issues found.
- Code Quality Review: 1 critical issue found, silent DeepProfiler output copy.
- Test Review: diagram produced, 5 gaps identified.
- Performance Review: 3 issues found.
- NOT in scope: written.
- What already exists: written.
- Failure modes: 1 critical gap flagged.
- Parallelization: 3 useful lanes, with `nextflow/main.nf` merge conflict risk.
- Lake Score: 5/6 recommendations choose complete option.

## Recommended Next Action

Do not start by coding DINO or Cellpose modules before the engine contract is written.

First, finish Loop 230 enough to prove the substrate. In parallel, update the plan docs so the project says what it is really becoming: a Nextflow-first platform with modular Cellpose, CellProfiler, cptools2, and AI feature-extraction containers.

Then write Phase 3 around the segmentation and feature-extraction contract. That is the keystone.

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|--------|---------|-----|------|--------|----------|
| CEO Review | `/plan-ceo-review` | Scope & strategy | 0 | not run | Office-hours strategy exists, but no CEO review log captured |
| Codex Review | `/codex review` | Independent 2nd opinion | 0 | not run | Not requested |
| Eng Review | `/plan-eng-review` | Architecture & tests | 1 | issues_open | 4 architecture issues, 5 test gaps, 1 critical failure mode |
| Design Review | `/plan-design-review` | UI/UX gaps | 0 | not applicable | No UI scope |
| DX Review | `/plan-devex-review` | Developer experience gaps | 0 | not run | Could be useful after Phase 3 config schema draft |

- **UNRESOLVED:** 6 open product/architecture questions, plus single-cell object identity.
- **VERDICT:** ENG REVIEW OPEN. Ready to finish Loop 230 substrate validation, not ready to implement Cellpose/DINO modules until Phase 3 contract exists.
