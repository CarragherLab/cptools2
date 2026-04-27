# Design: eddie-docs-search skill

Status: DRAFT
Date: 2026-04-13
Mode: Builder (internal feature)

## Problem Statement

Agents working in this repo need to locate authoritative content across ~30 markdown
documents in `docs/` (reference, applications, general) and `skills/*/references/`.
Today there is no skill-level affordance for this — agents ad-hoc `rg` the tree, which
surfaces legacy content (e.g. `memory-legacy-hvmem.md`) as if authoritative and misses
the cross-skill references layer. The silent-failure rules in CLAUDE.md (h_rss vs.
h_vmem, DataStore staging, qsub-in-script, module init, $NSLOTS) demand that doc search
be legacy-aware by default.

## What Makes This Useful

A single skill that any agent (including `eddie-orchestrator`) can load when a request
is doc-shaped. It wraps ripgrep with opinionated defaults, tiers results by authority,
and embeds the canonical memory/storage/module rules directly in SKILL.md so that any
hit against a legacy file is immediately corrected in-context.

## Constraints

- Framework-agnostic: repo-relative paths only, no `.cursor/` or `.claude/` assumptions.
- Must not duplicate `docs/reference/` content — skill references it as source of truth.
- Must not introduce a second orchestrator agent (see Premise 2).
- Must degrade cleanly when `rg` is unavailable (fall back to `grep -r`).
- Windows/Git Bash compatible (this is the primary dev environment).

## Premises (agreed)

1. Raw `rg` is insufficient. The skill adds: authority tiering, legacy exclusion by
   default, canonical-rule annotation on legacy hits, structured output.
2. No new orchestrator agent. `eddie-orchestrator` loads this skill when the request
   is doc-shaped. Adding a second orchestrator duplicates routing surface.
3. The `*-legacy-*.md` files are the highest-stakes correctness issue. Excluded by
   default; when explicitly included, every legacy hit is annotated with the current
   rule. The skill itself carries the canonical memory/storage rules at the top so
   the correction lives where the search happens.
4. Future qmd/embedding support is a north star, not a design constraint today. Ship
   ripgrep cleanly; swap internals later without changing the interface.

## Recommended Approach: Skill-only (Approach A)

### Files

- `skills/eddie-docs-search/SKILL.md` — frontmatter, when-to-use triggers, canonical
  rules block, search workflow, output format.
- `skills/eddie-docs-search/references/search-patterns.md` — tier definitions, glob
  patterns per tier, ranking rules, legacy annotation templates, worked examples.

### Canonical rules block (top of SKILL.md)

Embed the five silent-failure rules verbatim from CLAUDE.md so any legacy hit can be
auto-annotated. Specifically the memory rule: "Use `h_rss`, not `h_vmem`. Since
September 2025, `h_vmem` defaults to unlimited. Memory is per-slot:
`total RAM = h_rss * slots`." Any match in a `*-legacy-*.md` file is returned with
this rule prepended as a warning.

### Authority tiers (highest to lowest)

1. `docs/reference/*.md` (excluding `*-legacy-*.md`) — canonical infrastructure rules
2. `docs/applications/**/*.md` — per-application current guidance
3. `docs/general/*.md` — access, projects, conventions
4. `skills/*/references/*.md` — operational depth per skill
5. `docs/reference/*-legacy-*.md` — excluded by default; included only with explicit flag

### Search workflow

1. Accept query + optional tier filter + optional `--include-legacy` flag.
2. Search tier 1-4 in order; stop early if tier 1 returns high-confidence hits
   (configurable threshold, default: 3+ matches).
3. Return `path:line — snippet` grouped by tier with tier labels.
4. If `--include-legacy`, search tier 5 last and annotate every hit with the canonical
   rule from the SKILL.md rules block.
5. If zero hits across tiers 1-4, automatically widen to tier 5 with annotations (user
   was probably searching for a legacy term that's been renamed).

### Output format

```
[TIER 1: reference]
docs/reference/memory-specification.md:42 — Use h_rss to request per-slot memory...
docs/reference/memory-specification.md:58 — Total RAM = h_rss * slots...

[TIER 4: skill-refs]
skills/eddie-resources/references/sizing.md:12 — h_rss sizing formula...

[TIER 5: legacy — SUPERSEDED]
docs/reference/memory-legacy-hvmem.md:8 — h_vmem sets virtual memory...
  ⚠ SUPERSEDED: Use h_rss, not h_vmem. Since Sept 2025 h_vmem defaults to unlimited.
```

### Integration

- `eddie-orchestrator` adds one routing line: doc-shaped requests ("where does it
  say", "what's the rule for", "find docs on") load `eddie-docs-search`.
- Other agents can load it directly when they need grounding citations.
- No changes to construction/review agents.

## Open Questions

- Should the skill return file paths for agent follow-up reads, or inline excerpts
  sized to a token budget? Propose: paths + short excerpts (2-3 lines around match),
  let the caller Read the file if it needs more.
- Should tier 4 (skill-refs) be searchable by default, or only when tiers 1-3 miss?
  Propose: included by default but ranked below tiers 1-3.

## Success Criteria

- Query "h_vmem" returns zero tier-1 hits (excluded legacy) and surfaces the canonical
  h_rss rule at the top of output.
- Query "h_vmem --include-legacy" returns the legacy hits with the SUPERSEDED warning
  on each.
- Query "DataStore" returns `storage-and-staging.md` as tier 1, not
  `storage-legacy-source.md`.
- Query "module load python" returns `applications/python.md` as tier 2 with the
  `. /etc/profile.d/modules.sh` init rule visible.
- `eddie-orchestrator` successfully routes a doc-shaped question to the skill in an
  end-to-end test.

## Distribution

Repo-local skill. No packaging or CI changes. Add a `tests/fixtures/` doc-search case
that asserts the legacy exclusion behavior.

## The Assignment

Write `skills/eddie-docs-search/SKILL.md` first, with the canonical rules block and
the workflow. Then `references/search-patterns.md`. Add one routing line to
`skills/eddie-orchestrate/SKILL.md` or its routing reference. Add a fixture test
asserting that `h_vmem` search does not surface legacy files by default.

---

## Future Directions (not in scope for v1)

These are intentionally deferred. Captured here so they don't get lost.

### Validation-hook coupling (Approach C)

Extend `scripts/validate_eddie_script.sh` so that when it detects `h_vmem`, it invokes
the same canonical-rule lookup used by the skill and prints the correction with a
citation (`see docs/reference/memory-specification.md:L42`). Benefit: grep hits and
validator errors route through the same rule source — one place to update when rules
change. Deferred because v1 ships faster without it; revisit once the skill is stable
and the rules block has settled.

### Dedicated docs agent (Approach B)

Promote the skill to a `subagents/eddie-docs-search.md` when doc queries start
needing multi-step planning ("find the rule, then show me an example script that
follows it, then diff it against my draft"). Today this is overkill and overlaps with
`eddie-orchestrator`. Trigger to revisit: when a single doc query requires more than
two tool calls on average, or when we want doc-grounded answers composed across
multiple doc sections.

### qmd + embeddings backend

The north-star upgrade. Clone the qmd project setup, embed `docs/` and
`skills/*/references/` with a local embedding model, and swap the skill's ripgrep
backend for semantic search. Key design note: keep the skill's *interface* stable
(query in, tiered `path:line — snippet` out) so the backend swap doesn't ripple
through agents that depend on it. Tier authority ranking still applies — embeddings
change recall, not authority. Legacy exclusion still applies — embed them in a
separate index queried only when `--include-legacy` is set.

### Cross-skill reference deduplication

If the skill surfaces the same rule from 3+ locations (canonical doc + two skill
references), dedupe in output and cite the canonical source. Likely needed once
tier 4 coverage grows.
