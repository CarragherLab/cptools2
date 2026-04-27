# Concerns and Uncertainties

This is a living document tracking open questions, known issues, and decisions made during the development of the `eddie-for-agents` skill set. It is intended to be updated as work progresses and as new information is gathered.

**Last updated:** 2026-03-27

---

## RESOLVED CONCERNS

These were identified as issues and have been addressed. Kept here for audit trail.

### C1. Subagent paths referenced `.cursor/` framework
- **Issue:** All 3 subagent files used `.cursor/skills/...` and `.cursor/config/...` paths, making the skills Cursor-specific.
- **Resolution:** Updated to repo-relative paths (`skills/...`, `subagents/...`, `config/...`) for framework-agnostic operation.
- **Status:** Resolved in Phase 3

### C2. project_paths.yaml contained hardcoded project-specific paths
- **Issue:** `skills/eddie-login/project_paths.yaml` contained hardcoded Chandran Lab drugseq paths and specific DataStore directories.
- **Resolution:** Paths parameterised using `$PROJECT_NAME` and `$COLLEGE` variables. Standard Eddie infrastructure paths retained. Chandran Lab example preserved in `config/examples/chandranlabs.yaml`.
- **Status:** Resolved in Phase 3

### C3. Config files referenced by subagents did not exist
- **Issue:** Subagents referenced `.cursor/config/eddie/project.md` and `active_pipelines.md` which were absent from the repo.
- **Resolution:** Created `config/project.md` and `config/active_pipelines.md` as templates with sensible defaults.
- **Status:** Resolved in Phase 3

### C4. eddie-staging-login was project-specific
- **Issue:** The staging login skill navigated to a hardcoded ALS DataLakehouse path.
- **Resolution:** Generalised to navigate to project directory from `config/project.md` instead.
- **Status:** Resolved in Phase 3

### C5. h_vmem grep in CI would match legacy docs
- **Issue:** If CI grep checked for `h_vmem`, it would incorrectly flag legacy migration docs that legitimately discuss the deprecated directive.
- **Resolution:** CI uses GitLab built-in scanning templates only (SAST + Secret Detection). Script validation via `validate_eddie_script.sh` checks only `.sh` files.
- **Status:** Resolved — CI is basic-only, no runner-based grep checks

### C6. 12 skills had significant overlaps
- **Issue:** Four pairs of skills covered overlapping domains with inconsistent depth and structure.
- **Resolution:** Consolidated to 8 skills via 4 merges (see Phase 1 of plan).
- **Status:** Resolved in Phase 1

---

## OPEN UNCERTAINTIES

These are active questions that require investigation or expert input. Update status as they are resolved.

### U1. Agent framework compatibility
- **Description:** The skills were originally built for Cursor. It is unclear whether they work identically in Claude Code. The skill loading mechanism differs between frameworks — Cursor uses explicit skill loading via `.cursor/` config; Claude Code uses skill descriptions in frontmatter for invocation.
- **Impact:** Medium — if skills do not trigger correctly in Claude Code, the agent output will be degraded without obvious error.
- **Action needed:** Test key skills (eddie-validate, eddie-resources) in Claude Code after consolidation. Compare outputs with and without skill loading.
- **Status:** Open

### U2. Context size after skill consolidation
- **Description:** The construction subagent loads 4 skills simultaneously. After merging, reference files are larger. It is unclear whether the additional context degrades agent focus or introduces irrelevant content into responses.
- **Impact:** Medium — could cause less precise or longer outputs.
- **Action needed:** Run the skill-evaluator benchmark on the construction pipeline before and after consolidation to detect any regression.
- **Status:** Open

### U3. Functional evaluation without Eddie access
- **Description:** `validate_eddie_script.sh` and `local_qsub.sh` check syntax and patterns locally but cannot verify scripts actually run correctly on Eddie. The Grid Engine scheduler, module system, node hardware, DataStore NFS mounts, and staging queue constraints are not replicable outside the cluster.
- **Impact:** Irreducible — this is a fundamental limitation, not a gap to close.
- **Action needed:** Document clearly in README and skill-evaluator results. Accept as known limitation.
- **Status:** Accepted limitation — document in evaluation reports

### U4. Sole-user assumption for skill renaming
- **Description:** Renaming/merging skills will break any existing Cursor configurations that reference current skill paths. The assumption is that this repo is the single source of truth and no external configs reference the old paths.
- **Impact:** High if wrong — silent breakage of existing workflows with no error messages.
- **Action needed:** Confirm with Mungo Harvey that no other Cursor project configs reference the old skill names (`eddie-memory-and-cores`, `eddie-job-chaining-and-arrays`, `eddie-qa-validator`, `eddie-job-syntax`).
- **Status:** Open — **requires user confirmation before or after Phase 1**

### U5. validate_eddie_script.sh maturity
- **Description:** The validator script exists only as example code embedded in `eddie-qa-validator/eddie-qa-validator.md`. It has never been extracted or tested as a standalone executable.
- **Impact:** High — if the extracted script has bugs, test fixtures will give incorrect results and CI validation will be unreliable.
- **Action needed:** Carefully extract, test against all fixtures, and fix any issues before using in CI or documentation.
- **Status:** Open — will be resolved in Phase 4

### U6. College/school path variations
- **Description:** Eddie mount roots vary by college. Known variations: CMVM (`/exports/cmvm/`), CSCE (`/exports/csce/`), CHSS (`/exports/chss/`), IGMM (`/exports/igmm/`). It is unclear whether there are additional college codes or whether some colleges have non-standard path structures.
- **Impact:** Low-Medium — incorrect paths in config templates would cause data staging failures.
- **Action needed:** Confirm complete list of Edinburgh college mount root codes with user. Check Edinburgh HPC documentation for authoritative list.
- **Status:** Open — **requires expert input from user**

### U7. Eval scope for skill-evaluator
- **Description:** Full skill-evaluator benchmarks (with/without-skill subagent comparison) are compute-intensive. Running full benchmarks on all 8 skills is expensive. The alternative is structural/content-only review for low-risk skills.
- **Impact:** Low — the decision only affects how thoroughly we evaluate low-risk skills.
- **Action needed:** Decide on approach. Proposed: full benchmarks on the 4 merged skills (highest risk of content loss); structural/content review only for the 4 retained skills.
- **Status:** Open — decision pending

### U8. New skill boundaries
- **Description:** 5 new skills have been proposed (eddie-module-management, eddie-monitoring, eddie-troubleshooting, eddie-gpu, eddie-storage-strategy). Some of this content could alternatively be folded into existing skills as additional reference files rather than standalone skills.
- **Impact:** Low — affects repo organisation and skill count, not correctness.
- **Action needed:** User input on which warrant standalone skills. Proposed priority: eddie-monitoring and eddie-storage-strategy are most distinct; eddie-module-management content could go into eddie-script-standards; eddie-troubleshooting is a new workflow type; eddie-gpu is standalone.
- **Status:** Open — **requires user input on priorities**

### U9. No external HPC skill validation
- **Description:** The VoltAgent catalogue contains zero HPC/Linux/SGE skills. There is no community baseline against which to compare the design of the Eddie skills.
- **Impact:** Low-Medium — absence of external validation means our design choices are untested against community standards.
- **Action needed:** Consider publishing the Eddie skills to the VoltAgent catalogue after evaluation — would provide external visibility and community feedback.
- **Status:** Informational — no immediate action required

---

## DECISION LOG

Record significant architectural decisions here for future reference.

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-03-26 | Use repo-relative skill paths | Framework-agnostic; works with Claude Code, Cursor, and others |
| 2026-03-26 | Basic CI only (SAST + Secret Detection) | No Docker runners on Edinburgh GitLab; fundamental limitations of CI for HPC scripts make heavy investment unjustified |
| 2026-03-26 | Consolidate 12 skills to 8 | Remove overlapping skills; standardise on SKILL.md + references/ structure |
| 2026-03-26 | Config templates with standard Eddie paths | Infrastructure paths are standard; project names need configuration; follow `data/raw`, `data/processed` conventions |
| 2026-03-27 | VoltAgent catalogue not useful for HPC | Exhaustive search found zero HPC/Linux/scientific computing skills; all Eddie enhancements must be created in-house |
