# DeepProfiler Full Plate Scalability Run Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run one full test plate through the scalable DeepProfiler route on Eddie, using TensorFlow GPU memory growth and evidence-based fallback rules.

**Architecture:** Keep cptools2 as the orchestration entrypoint and let Nextflow submit per-chunk SGE jobs. Add a full-plate route generator so the run cannot accidentally reuse the `max_chunks: 5` diagnostic matrix, then launch one named scalable route and collect evidence from Nextflow traces, output counts, queue status, and logs.

**Tech Stack:** Python, YAML, cptools2 CLI, Nextflow DSL2, Eddie SGE, Singularity, TensorFlow/DeepProfiler.

---

## File Structure

- Modify `scripts/eddie_deepprofiler_matrix.py`
  - Allow `--max-chunks` to be optional.
  - When omitted, remove `max_chunks` from the route YAML so the full plate is processed.
  - Add `--route-root` so full-plate evidence lands outside the 5-chunk diagnostics directory.
- Modify `tests/test_deepprofiler_matrix_runner.py`
  - Cover full-plate route generation without `max_chunks`.
  - Cover custom route root rendering.
- Use existing `scripts/launch_deepprofiler_route.py`
  - Launch the selected full-plate route from the matrix JSON.
- Use existing `scripts/collect_deepprofiler_evidence.py`
  - Generate evidence Markdown/JSON after completion or failure.
- Modify `docs/reference/deepprofiler-scalability.md`
  - Record the full-plate route selected, command used, outcome, and fallback decision.
- Create remote outputs under:
  - `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability`

## Run Design

The staged `sarah-representative` plate currently contains `1,920` TIFF files.
With `5` expected channels and `chunk: 24`, that is:

```text
1,920 images / 5 channels = 384 image sets
384 image sets / 24 image sets per chunk = 16 chunks
```

`max_chunks` is therefore a run limiter, not a scaling mechanism. For this
plate:

- `max_chunks: 5` trims to 5/16 chunks.
- `max_chunks: 8` trims to 8/16 chunks.
- `max_chunks: 12` trims to 12/16 chunks.
- `max_chunks: 16` processes the current full plate.
- omitting `max_chunks` processes all chunks discovered for the selected plate
  and is the least brittle full-plate configuration.

Primary scalable route environment:

```bash
CPTOOLS2_FEATURE_GPU_QUEUE=gpu
CPTOOLS2_FEATURE_GPU_RESOURCE='-l gpu=1'
CPTOOLS2_SEGMENT_GPU_QUEUE=gpu
CPTOOLS2_SEGMENT_GPU_RESOURCE='-l gpu=1'
CPTOOLS2_SEGMENT_MAX_FORKS=5
CPTOOLS2_FEATURE_MAX_FORKS=8
CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH=true
CPTOOLS2_DEEPPROFILER_HOST_LOCK=false
```

Fallback route:

```bash
CPTOOLS2_FEATURE_GPU_QUEUE=gpu
CPTOOLS2_FEATURE_GPU_RESOURCE='-l gpu=1'
CPTOOLS2_SEGMENT_GPU_QUEUE=gpu
CPTOOLS2_SEGMENT_GPU_RESOURCE='-l gpu=1'
CPTOOLS2_SEGMENT_MAX_FORKS=5
CPTOOLS2_FEATURE_MAX_FORKS=4
CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH=true
CPTOOLS2_DEEPPROFILER_HOST_LOCK=true
```

Base config:

```text
/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/fuse/fuse-gate5-generic-gpu-cudaenv-max5-chunk24-20260515-100001/config.yaml
```

Full-plate route config must preserve the same `input_dir`, plate name, containers, model paths, and `chunk: 24`, but must not include the previous `max_chunks: 5` limiter.

Recommended ramp:

| Gate | Route root | `max_chunks` | Purpose |
| --- | --- | --- | --- |
| Half-plate gate | `diagnostics/deepprofiler-full-plate-ramp-max8` | `8` | Confirm scaling beyond the 5-chunk diagnostic without running the whole plate. |
| Three-quarter gate | `diagnostics/deepprofiler-full-plate-ramp-max12` | `12` | Exercise longer scheduling/runtime and more DeepProfiler outputs. |
| Full-plate gate | `diagnostics/deepprofiler-full-plate-scalability` | omitted | Process all discovered chunks without trimming. |

## Pass Criteria

- Launcher status file contains `0`.
- Nextflow completes with no failed tasks.
- `FEATURE_EXTRACT` completed count equals total count in the trace.
- `CELLPOSE_SEGMENT` completed count equals total count in the trace.
- `.npz` output count is greater than the 5-chunk baseline count of `360`.
- `no_cells.tsv` count remains `0` unless trace/log evidence shows legitimate empty chunks.
- No task log contains:

```text
Could not create cudnn handle
CUDNN_STATUS_INTERNAL_ERROR
Failed to get convolution algorithm
bus error
CUDA_VISIBLE_DEVICES=unset
```

## Task 1: Make Matrix Runner Safe For Full Plate Routes

**Files:**
- Modify: `scripts/eddie_deepprofiler_matrix.py`
- Test: `tests/test_deepprofiler_matrix_runner.py`

- [ ] **Step 1: Add failing tests for full-plate config generation**

Add this test to `tests/test_deepprofiler_matrix_runner.py`:

```python
def test_write_route_config_omits_max_chunks_when_none():
    workspace = Path.cwd() / "tmp-codex" / "deepprofiler-full-plate-config"
    shutil.rmtree(workspace, ignore_errors=True)
    workspace.mkdir(parents=True, exist_ok=True)
    try:
        base = workspace / "base.yml"
        base.write_text(
            "input_dir: /input\n"
            "output_dir: /old-output\n"
            "plates:\n"
            "  - sarah-representative\n"
            "chunk: 96\n"
            "max_chunks: 5\n"
        )
        entry = {
            **eddie_deepprofiler_matrix.build_matrix(max_chunks=None, chunk=24)[0],
            "name": "fullplate-growth-forks8",
        }
        route_dir = workspace / "route"

        config_path = eddie_deepprofiler_matrix.write_route_config(
            entry,
            base_config=base,
            route_dir=route_dir,
        )

        text = config_path.read_text()
        assert "chunk: 24" in text
        assert "max_chunks:" not in text
        assert f"output_dir: {route_dir.as_posix()}/outputs" in text
    finally:
        shutil.rmtree(workspace, ignore_errors=True)
```

Add this test for route roots:

```python
def test_render_remote_command_uses_custom_route_root():
    entry = eddie_deepprofiler_matrix.build_matrix(
        max_chunks=None,
        chunk=24,
        route_root="diagnostics/deepprofiler-full-plate-scalability",
    )[0]
    command = eddie_deepprofiler_matrix.render_remote_command(
        entry,
        project_root="/project",
        scratch_root="/scratch",
        config_path="/scratch/base.yml",
    )

    assert "/scratch/diagnostics/deepprofiler-full-plate-scalability/" in command
    assert "python3 -m cptools2.__main__ pipeline" in command
```

- [ ] **Step 2: Run tests and verify they fail**

Run:

```bash
pytest tests/test_deepprofiler_matrix_runner.py -q
```

Expected: fail because `build_matrix(max_chunks=None, route_root=...)` is not supported yet and `write_route_config` always writes `max_chunks`.

- [ ] **Step 3: Implement optional `max_chunks` and route root support**

Update `scripts/eddie_deepprofiler_matrix.py` as follows:

```python
DEFAULT_ROUTE_ROOT = "diagnostics/deepprofiler-scalability"
LOCK_DIR = "${CPTOOLS2_SCRATCH_ROOT}/locks/deepprofiler"


def build_matrix(
    max_chunks: int | None,
    chunk: int,
    route_root: str = DEFAULT_ROUTE_ROOT,
) -> list[dict[str, object]]:
    """Return the DeepProfiler scalability matrix definition."""

    routes = [
        ("dp-serialized", 1, "false", "false"),
        ("dp-concurrency2", 2, "false", "false"),
        ("dp-hostlock-concurrency4", 4, "false", "true"),
        ("dp-growth-concurrency2", 2, "true", "false"),
        ("dp-growth-concurrency5", 5, "true", "false"),
        ("dp-growth-hostlock-concurrency4", 4, "true", "true"),
    ]

    matrix: list[dict[str, object]] = []
    for name, max_forks, tf_allow_growth, host_lock in routes:
        env = _base_env()
        env.update(
            {
                "CPTOOLS2_FEATURE_MAX_FORKS": str(max_forks),
                "CPTOOLS2_DEEPPROFILER_TF_ALLOW_GROWTH": tf_allow_growth,
                "CPTOOLS2_DEEPPROFILER_HOST_LOCK": host_lock,
            }
        )
        entry: dict[str, object] = {
            "name": name,
            "chunk": chunk,
            "env": env,
            "route_root": route_root,
        }
        if max_chunks is not None:
            entry["max_chunks"] = max_chunks
        matrix.append(entry)
    return matrix
```

Update `write_route_config`:

```python
    config["chunk"] = int(entry["chunk"])
    if entry.get("max_chunks") is None:
        config.pop("max_chunks", None)
    else:
        config["max_chunks"] = int(entry["max_chunks"])
```

Update `render_remote_command`:

```python
    route_root = str(entry.get("route_root", DEFAULT_ROUTE_ROOT))
    run_root = f"{scratch_root}/{route_root}/{name}"
```

Update `_build_route_entry`:

```python
    route_root = str(entry.get("route_root", DEFAULT_ROUTE_ROOT))
    run_root = Path(scratch_root) / route_root / str(entry["name"])
```

Update parser:

```python
    parser.add_argument("--max-chunks", type=int)
    parser.add_argument("--route-root", default=DEFAULT_ROUTE_ROOT)
```

Update main:

```python
    matrix = build_matrix(
        max_chunks=args.max_chunks,
        chunk=args.chunk,
        route_root=args.route_root,
    )
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
pytest tests/test_deepprofiler_matrix_runner.py -q
```

Expected: all tests pass.

## Task 2: Generate Full Plate Matrix On Eddie

**Files:**
- Remote write: `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/matrix.json`
- Remote write: route-specific `route_config.yml` files under the same directory

- [ ] **Step 1: Deploy the updated matrix runner to Eddie**

Run from local workspace:

```powershell
C:\Windows\System32\OpenSSH\scp.exe scripts/eddie_deepprofiler_matrix.py mharvey2@eddie.ecdf.ed.ac.uk:/exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2/scripts/eddie_deepprofiler_matrix.py
```

Expected: command exits `0`.

- [ ] **Step 2: Generate the full-plate matrix without `--max-chunks`**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 && python3 scripts/eddie_deepprofiler_matrix.py --chunk 24 --project-root /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 --scratch-root /exports/eddie/scratch/mharvey2/cptools2-ai-update --route-root diagnostics/deepprofiler-full-plate-scalability --config-path /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/fuse/fuse-gate5-generic-gpu-cudaenv-max5-chunk24-20260515-100001/config.yaml --out /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/matrix.json"
```

Expected: command exits `0`.

- [ ] **Step 3: Verify the scalable route config is full-plate**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "grep -n 'max_chunks' /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/route_config.yml || true"
```

Expected: no `max_chunks` line.

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "grep -n 'output_dir\|chunk:\|plates:' /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/route_config.yml"
```

Expected: output includes `chunk: 24`, `plates:`, and an output directory under `deepprofiler-full-plate-scalability/dp-growth-concurrency5/outputs`.

## Task 3: Launch Primary Full Plate Scalable Route

**Files:**
- Remote write: `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/launch.sh`
- Remote write: `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/launcher.log`
- Remote write: `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/status.txt`

- [ ] **Step 1: Launch the route**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 && python3 scripts/launch_deepprofiler_route.py /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/matrix.json dp-growth-concurrency5"
```

Expected: output includes:

```text
ROUTE=dp-growth-concurrency5
RUN_ROOT=/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5
STATUS=/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/status.txt
```

- [ ] **Step 2: Confirm submitted jobs are in the GPU queue**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "qstat -u mharvey2"
```

Expected during GPU stages: `nf-CELLPOS` and then `nf-FEATURE` jobs show queue names beginning with `gpu@`.

- [ ] **Step 3: Confirm one feature job requests one GPU**

When an `nf-FEATURE` job appears, run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "qstat -j <FEATURE_JOB_ID>"
```

Expected output includes:

```text
hard_queue_list:            gpu
hard_resource_list:         gpu=1
```

## Task 4: Monitor And Capture Evidence

**Files:**
- Remote read: `launcher.log`
- Remote read: `outputs/traces/trace.batch_001.txt`
- Remote write: `evidence.json`
- Remote write: `evidence.md`

- [ ] **Step 1: Poll status**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "cat /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/status.txt 2>/dev/null || echo RUNNING"
```

Expected while active:

```text
RUNNING
```

Expected after success:

```text
0
```

- [ ] **Step 2: Check recent launcher log**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "tail -n 120 /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/launcher.log"
```

Expected successful ending:

```text
All batches complete!
```

- [ ] **Step 3: Collect evidence**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 && python3 scripts/collect_deepprofiler_evidence.py /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5 --json-out /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/evidence.json --md-out /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/evidence.md && cat /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/evidence.md"
```

Expected pass shape:

```text
FEATURE_EXTRACT: <N>/<N> completed, 0 failed
CELLPOSE_SEGMENT: <M>/<M> completed, 0 failed
First Error

None detected
```

## Task 5: Fallback If Full Plate Concurrency Fails

**Files:**
- Remote write: fallback route outputs under `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-hostlock-concurrency4`

- [ ] **Step 1: Classify failure**

Run evidence collection for `dp-growth-concurrency5` even on failure:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 && python3 scripts/collect_deepprofiler_evidence.py /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5 --json-out /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/evidence.json --md-out /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/evidence.md && cat /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/evidence.md"
```

If `First Error` includes cuDNN, convolution algorithm, bus error, or CUDA visibility failure, run the fallback route.

- [ ] **Step 2: Launch fallback route**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "cd /exports/cmvm/eddie/smgphs/groups/ChandranLabs/cptools2 && python3 scripts/launch_deepprofiler_route.py /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/matrix.json dp-growth-hostlock-concurrency4"
```

Expected: fallback starts and uses host locking.

- [ ] **Step 3: Confirm host lock is active**

Run:

```powershell
ssh -o ConnectTimeout=10 mharvey2@eddie.ecdf.ed.ac.uk "find /exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-hostlock-concurrency4/outputs/work -name .command.log -exec grep -H 'DeepProfiler host lock enabled' {} + 2>/dev/null | head"
```

Expected: at least one `.command.log` line showing a host lock path under `/exports/eddie/scratch/mharvey2/cptools2-ai-update/locks/deepprofiler/`.

## Task 6: Document Outcome

**Files:**
- Modify: `docs/reference/deepprofiler-scalability.md`

- [ ] **Step 1: Add full-plate outcome section**

Append this section after the current selected policy:

```markdown
## Full Plate Scalability Gate

Full-plate run root:

```text
/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability
```

Primary route:

```text
dp-growth-concurrency5
```

Outcome:

| Route | Status | Evidence |
| --- | --- | --- |
| `dp-growth-concurrency5` | `<PASS_OR_FAIL>` | `/exports/eddie/scratch/mharvey2/cptools2-ai-update/diagnostics/deepprofiler-full-plate-scalability/dp-growth-concurrency5/evidence.md` |

Decision:

```text
<USE_ALLOW_GROWTH_ROUTE_OR_FALLBACK_HOST_LOCK_ROUTE>
```
```

Replace `<PASS_OR_FAIL>` with `Pass` only if the pass criteria above are satisfied. Replace `<USE_ALLOW_GROWTH_ROUTE_OR_FALLBACK_HOST_LOCK_ROUTE>` with the selected production/full-plate policy.

- [ ] **Step 2: Run docs/tooling tests**

Run:

```bash
pytest tests/test_deepprofiler_matrix_runner.py tests/test_collect_deepprofiler_evidence.py -q
```

Expected: all tests pass.

## Self-Review

- Spec coverage: This plan covers full-plate route generation, launch, queue confirmation, evidence collection, fallback, and documentation.
- Placeholder scan: The plan has no `TBD`, no vague "handle later" tasks, and each command has an expected outcome.
- Type consistency: Route names match the current matrix names: `dp-growth-concurrency5` and `dp-growth-hostlock-concurrency4`.
