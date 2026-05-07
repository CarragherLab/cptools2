# Eddie HPC Reference Documentation

This directory contains reference documentation for the University of Edinburgh
Eddie HPC cluster, maintained for use by the Chandran Lab and as context for
the agent skills in this repository.

**System:** Eddie HPC, Rocky Linux 9, Grid Engine scheduler  
**Last reviewed:** March 2026

---

## Current Reference Documents

| Document | Contents |
|---|---|
| [getting-started.md](getting-started.md) | Eddie overview, key concepts, Chandran Lab paths, minimal job template |
| [submitting-jobs.md](submitting-jobs.md) | Full job script guide — directives, templates, monitoring, array jobs, chaining |
| [memory-specification.md](memory-specification.md) | RSS memory system (post Sept 2025), node type table, sizing workflow, tuning |
| [parallel-environments.md](parallel-environments.md) | sharedmem, mpi-32, scatter, gpu-a100 — with real STAR and DESeq2 examples |
| [storage-and-staging.md](storage-and-staging.md) | DataStore/Eddie filesystem, staging node rules, rsync patterns, NFS permissions, resubmission |
| [gpus.md](gpus.md) | NVIDIA A100 and MIG GPUs — queue, directives, CUDA setup, interactive sessions |
| [interactive-sessions.md](interactive-sessions.md) | qlogin usage, set_qlogin_environment.sh, memory/slots, screen reconnection |
| [eddie-modules.txt](eddie-modules.txt) | Cached full module list (terse, one per line) — refresh via `skills/eddie-modules/SKILL.md` |

## Additional Documentation

| Directory | Contents |
|---|---|
| [`../applications/`](../applications/) | Per-application guides: Python, R, MATLAB, AlphaFold, Singularity, conda/miniforge, Jupyter, TensorFlow, etc. |
| [`../general/`](../general/) | Access eligibility, compute projects/costs, SSH keys, citation, GridEngine-to-SLURM conversion |

## Legacy / Source Documents

Retained as primary sources and for migrating old scripts.

| Document | Contents |
|---|---|
| [memory-legacy-hvmem.md](memory-legacy-hvmem.md) | Pre-Sept 2025 `h_vmem` migration reference |
| [memory-legacy-source.md](memory-legacy-source.md) | Original wiki text with legacy examples |
| [storage-legacy-source.md](storage-legacy-source.md) | Original wiki storage text (corrected errors noted inline) |

---

## Critical Rules (Summary)

These are the rules most likely to cause silent failures on Eddie. All are
encoded in the CI linting and skill validation logic.

### Memory (September 2025 change)
- Use `h_rss`, not `h_vmem` — `h_vmem` no longer controls actual allocation
- Memory is per-slot: total = `h_rss` × slots
- Check actual usage with `qacct -j <JOB_ID>` → `maxrss` field
- Virtual memory defaults to unlimited — omit `h_vmem` entirely in new scripts

### Staging Queue
- DataStore is only accessible from staging nodes (`-q staging`)
- `sharedmem` PE is **incompatible** with the staging queue — always omit it
- Never reference `/exports/.../datastore/` paths from compute jobs

### Job Chaining
- Compute nodes cannot submit jobs — no `qsub` inside job scripts
- All chaining must be set up from the login node using `-hold_jid`
- Use job names not IDs for dependencies — more robust and self-documenting

### Modules
- Always initialise before loading: `. /etc/profile.d/modules.sh`
- Pin module versions for reproducibility: `module load python/3.11.4`

### Thread Configuration
- Use `$NSLOTS` to set thread counts — automatically set to allocated cores
- `export OMP_NUM_THREADS=$NSLOTS` for OpenMP programs

---

## Sources

| Source | Date | Notes |
|---|---|---|
| Eddie wiki (Confluence) — Memory Specification page | Oct 2025 | Scraped via Claude in Chrome; captured `h_rss` change at announcement |
| Eddie wiki — Parallel Environments PDF | Jan 2025 | Downloaded from Research Services wiki |
| Eddie wiki — Getting Started, Submitting Jobs, Storage | Various | Original text preserved in legacy docs |
| Chandran Lab operational experience | Ongoing | DRUGseq, cptools2, ALS DataLakehouse pipeline development |
| eddie-documentation repo (git.ecdf.ed.ac.uk/<UUN>) | Mar 2026 | Wiki scrapes consolidated; GPUs, applications, general info merged in |

**One document pending import:**  
`Memory_Specification.docx` — located at  
`\\<datastore-host>\<college>\<school>\groups\<group>\Mungo\Eddie\docs\`
Copy manually into this directory when access permits.

---

## Relationship to Skills

Skills load relevant documentation into their own `references/` subdirectory. This table shows which source documents informed each skill's reference content.

| Skill | Reference docs incorporated |
|---|---|
| `eddie-resources` | memory-specification.md, parallel-environments.md, gpus.md |
| `eddie-job-chaining` | submitting-jobs.md (chaining + array sections) |
| `eddie-validate` | All — cross-references all rules |
| `eddie-script-standards` | submitting-jobs.md, getting-started.md |
| `eddie-login` | getting-started.md |
| `eddie-staging-login` | storage-and-staging.md |
| `eddie-construct-pipeline` | All — construction uses all reference docs |
| `eddie-orchestrate` | All — routing requires full context |
| `eddie-modules` | eddie-modules.txt (cached list), module-namespaces.md |
