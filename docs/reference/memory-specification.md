# Memory Specification on Eddie

**Source:** University of Edinburgh Research Services wiki, scraped October 2025  
**Applies to:** Eddie HPC cluster, Rocky Linux 9  
**Critical update:** September 2025 — memory system changed from `h_vmem` to `h_rss`

---

## CRITICAL: September 2025 Changes

The memory management system changed significantly in September 2025:

1. **RSS memory (`h_rss`)** is now the primary memory control
2. **Virtual memory (`h_vmem`)** defaults to **unlimited** — omit it in new scripts
3. Jobs are **killed** if they exceed their `h_rss` limit (hard enforcement)
4. Use `maxrss` (not `maxvmem`) when checking actual memory with `qacct`
5. Default allocation if unspecified: **16–32 GB per slot** depending on node type

**Migration:** Scripts using `-l h_vmem` will still submit, but `h_vmem` no longer
controls actual memory allocation. Replace with `-l h_rss`.

---

## Understanding the Two Memory Types

### RSS Memory — Primary Control

Resident Set Size: the portion of your job's memory **actually held in RAM**.

```bash
#$ -l h_rss=16G       # 16 GB RSS per slot
```

- **Scheduler enforces this as a hard kill limit**
- Check allocation during job: `ulimit -m` (shows KB)
- Check usage after job: `qacct -j <JOB_ID>` → `maxrss` field

### Virtual Memory — Usually Omit

Total address space (RAM + swap). Defaults to **unlimited** since Sept 2025.

```bash
# Do NOT specify unless you have a specific reason
#$ -l h_vmem=64G      # Only if a legacy application requires it
```

---

## Core Formula

```
Total RAM available to job = h_rss × slots
```

**Examples:**

| Need | Slots | h_rss | Total |
|---|---|---|---|
| 8 GB | 1 | 8G | 8 GB |
| 24 GB | 1 | 24G | 24 GB |
| 64 GB | 2 | 32G | 64 GB |
| 128 GB | 4 | 32G | 128 GB |
| 256 GB | 8 | 32G | 256 GB |
| 512 GB | 16 | 32G | 512 GB |

---

## Hardware: Node Types and Memory Limits

| Node Type | Cores | RAM | Max h_rss/slot | Max slots |
|---|---|---|---|---|
| Intel 256G 32-core | 32 | 256 GB | 16 GB | 32 |
| Intel 384G 40-core | 40 | 384 GB | 19 GB | 40 |
| Intel 512G 32-core | 32 | 512 GB | 32 GB | 32 |
| AMD 512G 64-core | 64 | 512 GB | 16 GB | 64 |
| Intel 768G 64-core | 64 | 768 GB | 24 GB | 64 |
| **Intel 1T 64-core (standard)** | **64** | **1024 GB** | **32 GB** | **64** |
| AMD 1.5T 128-core | 128 | 1536 GB | 1536 GB* | 128 |
| AMD 1.5T 168-core | 168 | 1536 GB | 1536 GB* | 168 |
| Intel 2T 56-core | 56 | 2048 GB | 72 GB | 56 |
| Intel 4T 64-core | 64 | 4096 GB | 128 GB | 64 |
| GPU: Intel/NVIDIA A100 | 64 | 768 GB | 32 GB | 64 |

*Large nodes allow requesting entire node RAM per slot.

**Notes:**
- Standard nodes: 32 GB max per slot is the common ceiling
- GPU nodes: 4× NVIDIA A100 80GB each; 7 nodes total
- IGC and Roslin nodes are restricted to specific users

---

## Specifying Memory in Job Scripts

### Single Core, Low Memory
```bash
#!/bin/sh
#$ -N serial_job
#$ -cwd
#$ -l h_rt=02:00:00
#$ -l h_rss=8G

. /etc/profile.d/modules.sh
module load python/3.11.4
python my_script.py
```

### Multi-Core, High Memory (most common pattern)
```bash
#!/bin/sh
#$ -N parallel_job
#$ -cwd
#$ -pe sharedmem 8
#$ -l h_rt=04:00:00
#$ -l h_rss=32G        # 8 × 32G = 256GB total

. /etc/profile.d/modules.sh
module load R/4.4.0

export OMP_NUM_THREADS=$NSLOTS
Rscript analysis.R
```

### GPU Job
```bash
#!/bin/sh
#$ -N gpu_job
#$ -cwd
#$ -q gpu
#$ -l gpu=1
#$ -l h_rt=02:00:00
#$ -l h_rss=16G

. /etc/profile.d/modules.sh
module load cuda
python gpu_script.py
```

---

## Estimating and Tuning Memory

### Initial Estimate
- Data analysis: ~2–3× input file size
- Genomic tools: check documentation — often specified
- Unknown: start at 32G and measure

### Measure After a Test Run
```bash
qacct -j <JOB_ID> | grep maxrss
# maxrss    27.384G
```

### Tuning Rules

| maxrss vs requested | Action |
|---|---|
| < 70% of requested | Reduce — you're over-requesting |
| 70–90% of requested | Good — add a small buffer |
| > 90% of requested | Increase — risk of OOM kills |
| Job was killed (OOM) | Increase significantly |

**Buffer recommendation:** Set `h_rss` to ~1.2× typical `maxrss`.

---

## Default Memory Allocation

If you omit `h_rss`, Eddie allocates automatically based on node type:

```
Default memory per slot = 2 × (Total Node RAM) / (Number of Cores)
```

Typical defaults: **16–32 GB per slot** (32 GB on standard 1T nodes).

**Use defaults when:** your job needs ≤16 GB and you want maximum scheduling flexibility.  
**Specify explicitly when:** your job needs >16 GB or you need consistent behaviour across node types.

---

## Checking Memory During a Job

```bash
# On the compute node — shows allocated RSS limit in KB
ulimit -m

# Live usage — look at RES column
top
htop
```

---

## Common Pitfalls

| Mistake | Fix |
|---|---|
| `#$ -l h_vmem=32G` | Replace with `#$ -l h_rss=32G` |
| `#$ -l h_rss=128G` (no `-pe`) | Single slot max is 32G — add `-pe sharedmem 4` |
| Checking `maxvmem` in qacct | Check `maxrss` instead |
| No memory spec for >16GB jobs | Always specify `h_rss` explicitly |

---

## Old System Reference (Pre-September 2025)

The old system used virtual memory specification:
```bash
#$ -l h_vmem=8G    # OLD — per slot, hard kill limit
# qacct → maxvmem field
```

This is now deprecated. Scripts using `h_vmem` still submit but the directive
no longer controls actual memory allocation to your job.

---

## Related Documentation

- [parallel-environments.md](parallel-environments.md) — slot/PE configuration
- [submitting-jobs.md](submitting-jobs.md) — full job script guide
- [getting-started.md](getting-started.md) — Eddie overview
