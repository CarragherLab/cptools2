# Memory Specification — Pre-September 2025 (Legacy Reference)

**Status:** DEPRECATED — retained for historical reference only  
**Replaced by:** [memory-specification.md](memory-specification.md)  
**Applies to:** Scripts written before September 2025 that use `h_vmem`

---

> ⚠️ **Do not use this as a template for new scripts.** The `h_vmem` directive
> no longer controls actual memory allocation on Eddie. Use `h_rss` instead.
> See [memory-specification.md](memory-specification.md) for current guidance.

---

## Overview (Historical)

Prior to September 2025, memory on Eddie was specified using `-l h_vmem`, which
set the maximum virtual memory (RAM + swap) per CPU core. The scheduler enforced
this as a hard kill limit.

```bash
#$ -l h_vmem=8G    # OLD — virtual memory per slot, now deprecated
```

Memory usage was checked with `qacct` using the `maxvmem` field:
```
maxvmem      8.152G
```

## Migration to Current System

| Old | New |
|---|---|
| `#$ -l h_vmem=16G` | `#$ -l h_rss=16G` |
| `qacct → maxvmem` | `qacct → maxrss` |
| Kill on virtual memory exceed | Kill on RSS memory exceed |
| Virtual memory = hard limit | Virtual memory = unlimited |

## Why This Reference Exists

Many older scripts in the Chandran Lab codebase (DRUGseq, cptools2, staging scripts)
were written before September 2025 and use `h_vmem`. The CI linting rules in this
repo flag `h_vmem` as a deprecation error to prompt migration.

When reviewing or updating legacy scripts, replace `h_vmem` with `h_rss`.
The value is typically the same — the directive syntax changes but the memory
sizing logic does not.

---

## Legacy Example Scripts (for context only)

```bash
# OLD serial job — do not copy
#!/bin/sh
#$ -N SerialJob
#$ -cwd
#$ -l h_rt=01:00:00
#$ -l h_vmem=4G        # DEPRECATED

module load python
python my_script.py
```

```bash
# UPDATED equivalent
#!/bin/sh
#$ -N SerialJob
#$ -cwd
#$ -l h_rt=01:00:00
#$ -l h_rss=4G         # CURRENT

. /etc/profile.d/modules.sh
module load python/3.11.4
python my_script.py
```
