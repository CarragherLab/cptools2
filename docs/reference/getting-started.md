# Getting Started with Eddie HPC

**Source:** University of Edinburgh Research Services wiki  
**Applies to:** Eddie HPC cluster, Rocky Linux 9  
**Note:** Example scripts in the original source use the deprecated `h_vmem` directive.
All examples here have been updated to reflect the current `h_rss` system (Sept 2025).

---

## Overview

Eddie is the University's HPC cluster, providing a powerful platform for running
computationally intensive jobs. It consists of over 300 compute nodes with varying
amounts of CPU cores (16 to 64 cores per node) and RAM (64GB to 3TB per node).
Special GPU nodes are also available with NVIDIA A100 GPUs for accelerated computing.
A batch scheduling system (Grid Engine) is used to submit, schedule and run jobs on
the cluster nodes. Interactive sessions are also available for real-time analysis and
development.

---

## Key Concepts

**1. Login nodes**
The "front door" to the cluster that you SSH into. Never run computational work
directly on the login nodes. Only use them to submit jobs, manage files, or start
interactive sessions.

**2. Job submission scripts**
To run work on Eddie, you create job scripts that specify the resources your job needs
(number of cores, amount of RAM, max runtime, etc.) and the actual commands to run.
These are plain text files you write on the login nodes.

**3. Submitting jobs**
You submit job scripts to the scheduler using the `qsub` command:
```bash
qsub myjob.sh
```
This puts the job in a queue to wait until the requested resources become available
on the compute nodes.

**4. Job statuses**

| State | Meaning |
|---|---|
| `qw` | Queued, waiting for resources |
| `r` | Running on compute nodes |
| `hqw` | Held — waiting for a dependency |
| `Eqw` | Error state — failed but waiting to retry |
| `Ft` | Failed, terminated |

**5. Monitoring jobs**
- `qstat` — shows queued and running jobs
- `qacct -j <JOBID>` — shows resource usage of finished jobs (memory, runtime, exit code)
- `qdel <JOBID>` — cancels a job

**6. Parallel environments**
Allow requesting multiple CPU cores. `sharedmem` is for multi-threaded jobs on a
single node. `mpi/mpi-32` environments are for multi-node MPI jobs.

**7. Modules system**
Applications, libraries and tools are available via the modules system. Always
initialise before loading:
```bash
. /etc/profile.d/modules.sh
module load python/3.11.4
```

---

## Getting Started Suggestions

1. Start with a simple test job to get comfortable with `qsub`, `qstat`, and `qacct`
2. Request only the resources you actually need — over-requesting increases queue time
3. Prototype interactively with `qlogin` before submitting long batch jobs
4. Use scratch filesystems (`/exports/eddie/scratch/$USER`) for temporary data during jobs
5. Check `qacct` after every job to understand actual memory and runtime usage

---

## Minimal Job Script Template

```bash
#!/bin/sh
#$ -N MyJobName
#$ -cwd
#$ -pe sharedmem 8
#$ -l h_rt=02:00:00
#$ -l h_rss=16G          # Use h_rss — h_vmem is deprecated since Sept 2025

# Initialise modules
. /etc/profile.d/modules.sh
module load python/3.11.4

# Define paths
DATA_DIR=/path/to/input/data
RESULTS_DIR=/exports/eddie/scratch/$USER/$JOB_ID

mkdir -p "$RESULTS_DIR"

# Execute
python my_analysis.py -i "$DATA_DIR" -o "$RESULTS_DIR"
```

---

## Key Paths (Chandran Lab)

| Location | Path | Purpose |
|---|---|---|
| Home | `/home/<UUN>` | Config files, scripts (10GB, backed up) |
| Scratch | `/exports/eddie/scratch/${USER}` | Active job data (2TB, purged after 1 month) |
| Group space | `/exports/<college>/eddie/<school>/groups/<group>` | Shared group storage |
| DataStore | `/exports/<college>/datastore/<school>/groups/<group>` | Long-term archive (staging nodes only) |

---

## Related Documentation

- [submitting-jobs.md](submitting-jobs.md) — detailed job script guide
- [memory-specification.md](memory-specification.md) — RSS memory system (post Sept 2025)
- [parallel-environments.md](parallel-environments.md) — sharedmem, mpi, GPU queue requests
- [storage-and-staging.md](storage-and-staging.md) — DataStore, Eddie filesystem, staging nodes
