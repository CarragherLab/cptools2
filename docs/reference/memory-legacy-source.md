# Memory Specification — Legacy Source Document (Pre-September 2025)

**Status:** DEPRECATED — retained as original source document  
**Replaced by:** [memory-specification.md](memory-specification.md)  
**Note:** All examples use the old `h_vmem` directive which is now deprecated.
Do not use these scripts as templates — see the current guide instead.

---

> This document is the original University of Edinburgh wiki text for memory
> specification, predating the September 2025 `h_rss` change. It is preserved
> here for reference when migrating legacy scripts that use `h_vmem`.

---

## Memory Specification (Original Text)

One of the most important aspects of submitting jobs on Eddie is requesting the
appropriate amount of memory (RAM) for your program. If you don't request enough,
your job may fail with an "Out of Memory" error. But if you request too much, your
job will wait in the queue unnecessarily long because the scheduler has to wait for
a node with enough free memory to become available.

Memory is specified using the `-l h_vmem` option to `qsub`. The value is the amount
of memory to request per CPU core. You can use suffixes like 'K', 'M', 'G', or 'T'
for kilobytes, megabytes, gigabytes, or terabytes respectively.

```bash
#$ -l h_vmem=8G    # DEPRECATED — use h_rss instead
```

The total memory available to your job is the `h_vmem` value multiplied by the
number of CPU cores requested.

Check maximum memory used by previous runs:

```bash
qacct -j 1234567
# maxvmem      8.152G    ← use maxrss in current system
```

## Parallel Environments (Original Text)

The three main parallel environments:

1. `sharedmem` — multithreading on a single node (OpenMP)
2. `mpi` and `mpi-32` — MPI across multiple nodes
3. `gpu-a100` — GPU-accelerated programs

```bash
#$ -pe sharedmem 8    # 8 cores on one node
```

## Legacy Example Scripts

> ⚠️ These use deprecated `h_vmem`. Replace with `h_rss` when updating.

### Serial Job (Legacy)

```bash
#!/bin/sh
#$ -N SerialJob
#$ -cwd
#$ -l h_rt=01:00:00
#$ -l h_vmem=4G        # DEPRECATED

module load python
python my_script.py
```

### Shared Memory Parallel Job (Legacy)

```bash
#!/bin/sh
#$ -N SharedMemJob
#$ -cwd
#$ -pe sharedmem 16
#$ -l h_rt=02:00:00
#$ -l h_vmem=8G        # DEPRECATED — total: 16 × 8G = 128GB

module load python
export OMP_NUM_THREADS=$NSLOTS
python my_openmp_script.py
```

### MPI Parallel Job (Legacy)

```bash
#!/bin/sh
#$ -N MPIJob
#$ -cwd
#$ -pe mpi-32 64
#$ -l h_rt=04:00:00
#$ -l h_vmem=4G        # DEPRECATED

module load python
module load openmpi
mpirun -n $NSLOTS python my_mpi_script.py
```

### GPU Job (Legacy)

```bash
#!/bin/sh
#$ -N GPUJob
#$ -cwd
#$ -pe gpu-a100 1
#$ -l h_rt=01:00:00
#$ -l h_vmem=8G        # DEPRECATED
#$ -l gpus=1

module load python
module load cuda
python my_gpu_script.py
```

---

## Migration Guide

When updating scripts from this legacy system to the current system:

| Old directive | New directive | Notes |
|---|---|---|
| `#$ -l h_vmem=16G` | `#$ -l h_rss=16G` | Value is typically the same |
| `qacct → maxvmem` | `qacct → maxrss` | Different field name |
| Kill on vmem exceed | Kill on RSS exceed | Same enforcement behaviour |

The sizing logic (per-slot × slots = total) is unchanged. Only the directive
name and the `qacct` field name differ.
