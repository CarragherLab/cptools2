# Parallel Environments on Eddie

**Source:** University of Edinburgh Research Services wiki (January 2025)  
**Applies to:** Eddie HPC cluster, Rocky Linux 9

---

## Introduction

Parallel environments (PEs) allow jobs to request more than a single scheduler
slot. On Eddie, one slot = one physical CPU core.

```bash
qsub -pe <parallel_environment> <number_of_slots> jobscript.sh
```

> **Important:** Requesting a parallel environment allocates the slots but does
> NOT parallelise your code automatically. Your program must use parallel
> techniques (OpenMP, MPI, threading) to make use of multiple cores.

---

## Resource Reservation

For large slot requests, use resource reservation to reduce scheduling wait time:

```bash
#$ -R y
```

This tells the scheduler to start reserving slots incrementally rather than
waiting for all of them to become free simultaneously. Recommended for any
`sharedmem` request >16 slots and all `mpi-32` jobs.

---

## Available Parallel Environments

### `sharedmem` — Single Node Parallelism

**Use for:** OpenMP programs, multithreaded applications, or jobs needing
more memory than a single slot provides (even if not truly parallel).

```bash
#$ -pe sharedmem 8
```

- Allocates cores on a **single node**
- Maximum: 64 cores on most nodes; 168 on the largest node
- Most common PE for bioinformatics workloads
- **Incompatible with the staging queue** (`-q staging`) — omit for staging jobs

**Memory use case:** Standard nodes cap at 32GB per slot. To access more
total memory, request multiple slots:

```bash
# Need 128GB total? Use 4 slots × 32GB
#$ -pe sharedmem 4
#$ -l h_rss=32G
```

---

### `interactivemem` — Interactive Sessions Only

**Use for:** `qlogin` interactive sessions requiring multiple cores or extra memory.

```bash
qlogin -pe interactivemem 4 -l h_rss=32G -l h_rt=03:00:00
```

Do not use in batch job scripts — use `sharedmem` for batch jobs.

---

### `mpi-32` — Multi-Node MPI (32-core nodes)

**Use for:** MPI programs that genuinely need multiple nodes.

```bash
#$ -pe mpi-32 64    # Must be multiple of 32
#$ -R y             # Resource reservation strongly recommended
```

- Slot count must be a **multiple of 32**
- Allocates **whole nodes** — longer queue times than `sharedmem`
- Requires `mpirun` to distribute work across nodes
- Only use if you genuinely need multi-node MPI

**Memory with mpi-32:**
```bash
# 2 × 32-core nodes, 12GB per slot = 384GB per node
#$ -pe mpi-32 64
#$ -l h_rss=12G
```

---

### `mpi` — Multi-Node MPI (16-core nodes) ⚠️ DEPRECATED

**Status:** Being retired. 16-core nodes are out of warranty.

```bash
#$ -pe mpi 32       # Multiple of 16
#$ -l rl9=false     # Required — only on Scientific Linux nodes
#$ -R y
```

Use `mpi-32` for all new work.

---

### `scatter` — Distributed Cores (MPI only)

**Use for:** MPI jobs where you need many cores quickly and don't mind them
being spread across multiple nodes.

```bash
#$ -pe scatter 16
```

- Cores allocated **wherever available** across the cluster
- No resource reservation needed
- **MPI programs only** — requires `mpirun` to use scattered cores
- Faster than `mpi-32` for large core counts but cores are distributed

---

### GPU queue — GPU Acceleration

**Use for:** CUDA, TensorFlow, PyTorch, and other GPU-accelerated workloads.

```bash
#$ -q gpu              # Required GPU queue
#$ -l gpu=1            # Number of full GPUs (1-4 per node)
```

- 7 GPU nodes: 4× NVIDIA A100 80GB per node
- 64 CPU cores and 768 GB RAM per node
- For test-scale jobs, `#$ -l gpu-mig=1` can request one MIG partition instead of a full GPU.
- The old `gpu-a100` parallel environment is no longer available; do not use `-pe gpu-a100` or `-l gpus=N`.
- See GPU documentation for full job configuration details

---

## Choosing the Right PE

| Scenario | PE to use | Notes |
|---|---|---|
| Single-threaded job | None | Omit `-pe` entirely |
| Multithreaded / OpenMP | `sharedmem` | Single node only |
| Need >32GB total memory | `sharedmem` | Even if not parallel |
| Interactive session | `interactivemem` | With `qlogin` only |
| Multi-node MPI | `mpi-32` | Add `-R y` |
| MPI, need cores quickly | `scatter` | Distributed, no `-R y` needed |
| GPU workload | none, use `-q gpu` | Request full GPUs with `-l gpu=N`; request one MIG test partition with `-l gpu-mig=1` |

---

## Staging Queue Constraint

The `sharedmem` PE is **incompatible with the staging queue**. Always omit
`-pe sharedmem` from staging scripts:

```bash
#!/bin/sh
#$ -N stage_in
#$ -cwd
#$ -q staging
#$ -l h_rt=01:00:00
#$ -l h_rss=4G
# No -pe directive here — sharedmem breaks staging jobs

rsync -av "${DATASTORE_SRC}/" "${EDDIE_DST}/"
```

---

## Thread Count Configuration

Always pass `$NSLOTS` to tools that support threading — it's automatically
set to your allocated core count:

```bash
export OMP_NUM_THREADS=$NSLOTS        # OpenMP programs
python script.py --threads $NSLOTS    # Python tools
Rscript script.R $NSLOTS              # R parallel package
```

---

## Real-World Examples

### STAR RNA-seq Alignment (sharedmem)

```bash
#!/bin/sh
#$ -N rnaseq_align
#$ -cwd
#$ -pe sharedmem 16
#$ -l h_rt=08:00:00
#$ -l h_rss=16G          # 16 × 16G = 256GB total

. /etc/profile.d/modules.sh
module load star/2.7.10a

export OMP_NUM_THREADS=$NSLOTS

STAR --runThreadN $NSLOTS \
     --genomeDir /exports/<college>/eddie/<school>/groups/<group>/refs/star_index \
     --readFilesIn sample_R1.fastq.gz sample_R2.fastq.gz \
     --readFilesCommand zcat \
     --outSAMtype BAM SortedByCoordinate \
     --outFileNamePrefix results/sample_
```

### High-Memory R Analysis (sharedmem for memory, not parallelism)

```bash
#!/bin/sh
#$ -N deseq2_analysis
#$ -cwd
#$ -pe sharedmem 8
#$ -l h_rt=04:00:00
#$ -l h_rss=32G          # 8 × 32G = 256GB total for large count matrix

. /etc/profile.d/modules.sh
module load R/4.4.0

# R handles its own parallelism via BiocParallel / future
Rscript deseq2_analysis.R --cores $NSLOTS
```

### GPU Training Job

```bash
#!/bin/sh
#$ -N model_train
#$ -cwd
#$ -q gpu
#$ -l gpu=4
#$ -l h_rt=12:00:00
#$ -l h_rss=16G          # System RAM per CPU slot

. /etc/profile.d/modules.sh
module load cuda
module load python/3.11.4

python train.py --gpus 4 --batch-size 512
```

---

## Memory and Slot Interaction

Memory (`h_rss`) is always **per slot**. Total = `h_rss` × slots.

| Need | Slots | h_rss | Total |
|---|---|---|---|
| 24 GB | 1 | 24G | 24 GB |
| 64 GB | 2 | 32G | 64 GB |
| 128 GB | 4 | 32G | 128 GB |
| 256 GB | 8 | 32G | 256 GB |
| 512 GB | 16 | 32G | 512 GB |

---

## Common Mistakes

| Mistake | Fix |
|---|---|
| Using `mpi-32` for non-MPI code | Use `sharedmem` instead |
| `mpi-32` slot count not multiple of 32 | Must be 32, 64, 96... |
| Not setting `OMP_NUM_THREADS` | Add `export OMP_NUM_THREADS=$NSLOTS` |
| Using `sharedmem` in staging queue | Incompatible — omit `-pe` for staging |
| Using `interactivemem` in batch script | Use `sharedmem` for batch jobs |

---

## Related Documentation

- [memory-specification.md](memory-specification.md) — h_rss per-slot calculations
- [submitting-jobs.md](submitting-jobs.md) — full directive reference
- [storage-and-staging.md](storage-and-staging.md) — staging queue constraints
