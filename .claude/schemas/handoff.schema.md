# Handoff Schema

A handoff summary is the **only** context carried between ralph loops. It captures what happened in the previous loop so the next loop can pick up precisely where work left off, without dragging forward thousands of tokens of prior conversation.

---

## Format

```yaml
handoff_summary:
  done: ""      # What was completed (one sentence max)
  failed: ""    # What failed and why (one sentence max; empty string if nothing failed)
  needed: ""    # What must still happen (one sentence max; empty string if fully done)
```

---

## Field Specifications

| Field | Type | Required | Max Length | When Empty |
|-------|------|----------|-----------|------------|
| `done` | string | Yes | One sentence | Never empty at completion |
| `failed` | string | Yes | One sentence | Empty string `""` if nothing failed |
| `needed` | string | Yes | One sentence | Empty string `""` if phase/loop is fully complete |

---

## Writing Rules

1. **Written at completion, never before.** The handoff block exists with empty strings in the template. It is populated when the loop finishes or hits `max_iterations`.

2. **One sentence per field.** The handoff's purpose is to prevent the prior session's full context from being dragged forward. If it grows verbose, it defeats its own purpose. One sentence forces precision.

3. **`done` references artefacts, not effort.** Name the files written, tests passing, or decisions made — not "worked on X".

4. **`failed` gives root cause, not just symptom.** Include enough context for the next loop to understand *why* something failed, not just *that* it failed.

5. **`needed` is an action, not a restatement of the phase goal.** The next loop should know exactly what to do first.

---

## Examples

### Fully successful loop

```yaml
handoff_summary:
  done: "All 3 bugs fixed and verified; position sizing rejects stop_pips < MIN; duplicate limit changed from 2→1 per symbol/direction/cycle."
  failed: ""
  needed: ""
```

### Partially successful loop

```yaml
handoff_summary:
  done: "Retroactive predictions generated for 24 of 26 pairs; 18,200 rows added to predictions_base."
  failed: "USD/TRY and USD/ZAR failed — OHLCV data coverage below 50% for 2025; excluded from training."
  needed: "Proceed to feature expansion (loop-043) using the 24 valid pairs; document excluded pairs in phase results."
```

### Loop that hit max_iterations

```yaml
handoff_summary:
  done: "GNN dataset prepared with 41 features; temporal splits validated; no look-ahead contamination."
  failed: "GAT training diverged after epoch 12 — loss oscillating between 0.8-1.2; suspected learning rate too high for attention mechanism."
  needed: "Retry GAT with lr=1e-4 (was 1e-3) and gradient clipping at 1.0; if still diverges, skip GAT and proceed to TemporalGCN."
```

---

## Injection Protocol

At execution time, the next loop's `prompt` field contains an injection block:

```yaml
prompt: |
  ## Context from prior loop
  Done: [inject prior.handoff_summary.done]
  Failed: [inject prior.handoff_summary.failed]
  Needed: [inject prior.handoff_summary.needed]
```

The executing agent (or the orchestrator, or `/next-loop`) replaces the `[inject ...]` placeholders with the actual values from the prior loop's frontmatter. For the first loop in a phase, all three fields are set to empty strings or a brief phase context note.

---

## Where Handoffs Live

| Location | Purpose | Updated When |
|----------|---------|-------------|
| Loop frontmatter `handoff_summary:` | Authoritative record | At loop completion |
| `CLAUDE.md ## Planning State → Last handoff:` | Quick orientation at session start | At loop completion |
| `.claude/state/loop-complete.json → handoff:` | Machine-readable for state bus | At loop completion |
| `.claude/state/history.jsonl` | Append-only audit log | At loop completion (optional) |

All four locations contain the same three values. The loop frontmatter is the canonical source.

---

## Anti-Patterns

| Anti-Pattern | Why It's Wrong | Fix |
|--------------|---------------|-----|
| Multi-paragraph `done` | Defeats bounded context purpose | Condense to one sentence; put detail in the loop's plan file |
| `failed: "NA"` | String `"NA"` is not empty; causes confusion | Use empty string `""` |
| `needed: "Continue Phase 2"` | Too vague; next loop doesn't know where to start | Specify the exact first action |
| Handoff written before loop starts | Fields are guesses, not facts | Always write at completion |
| Copying the prior loop's full handoff forward | Context accumulation; handoffs should not chain | Each loop writes its own fresh handoff |

---

## Validation Checklist

Before advancing to the next loop, verify:

- [ ] `done` is populated (never empty at completion)
- [ ] `failed` is empty string or a single sentence with root cause
- [ ] `needed` is empty string or a specific next action
- [ ] No field exceeds one sentence
- [ ] `done` references artefacts (files, tests, decisions), not effort
- [ ] The same values appear in CLAUDE.md and loop-complete.json
