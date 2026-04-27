# GPUs

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/GPUs>

> **IMPORTANT:** The `gpu-a100` parallel environment is no longer available. Use the `gpu` queue and `-l gpu=N` resource as described below. Memory is now controlled via `-l h_rss` (not `-l h_vmem`).

## Available GPUs

The Eddie cluster has compute nodes containing **NVIDIA A100 GPUs**:

- Each GPU-enabled node: 4× A100 GPUs, 64 CPU cores, 768 GB system RAM
- Each A100 GPU: 80 GB GPU RAM
- There are also two nodes with **multi-instance GPU (MIG)** partitions: 8 MIG partitions and 2 A100 GPUs each. Only **1 MIG partition** can be used per job. MIG partitions are 'slices' of A100 GPUs — less powerful but suitable for testing.

To see available GPU nodes:

```bash
qstat -F gpu,gpu-mig,gputype,h_rss -q gpu
```

## Submitting Jobs

```bash
# Required: -q gpu (queue) AND -l gpu=N or -l gpu-mig=1
# Optional: -pe sharedmem M (for multiple CPU cores)
```

| Option | Description |
|--------|-------------|
| `-q gpu` | Submit to the GPU queue (**required**) |
| `-l gpu=N` | Request N GPUs (max 4; most jobs need 1) |
| `-l gpu-mig=1` | Request 1 MIG partition (max 1 per job) |
| `-pe sharedmem M` | Request M CPU cores (max 64; default: 1) |
| `-l h_rss=<MEMORY>` | Request system RAM per slot |

### Example Job Script

```bash
#!/bin/bash
# Grid Engine options
#$ -l h_rt=01:00:00
#$ -cwd
#$ -q gpu
#$ -l gpu=1
#$ -l h_rss=32G

# Initialise modules
. /etc/profile.d/modules.sh
module load cuda

# Run the executable
./example
```

Submit with:

```bash
qsub job.sh
```

## Interactive Sessions

```bash
# One A100 GPU, 12-hour runtime
qlogin -q gpu -l gpu=1 -l h_rt=12:00:00
```

> **You MUST specify a runtime** for GPU interactive sessions — otherwise the session will never be scheduled.
> Maximum runtime for interactive GPU sessions: **24 hours**.

After obtaining your session, set the scheduler environment:

```bash
source /exports/applications/support/set_qlogin_environment.sh
```

## CUDA_VISIBLE_DEVICES

On Eddie, GPU access is controlled by the `CUDA_VISIBLE_DEVICES` environment variable. This is set **automatically** for `qsub` jobs.

**Do not define or modify this variable** — it can cause jobs to fail by trying to access GPUs in use by other users.

For interactive sessions, it is **not** set automatically. Run the following after starting your session:

```bash
source /exports/applications/support/set_qlogin_environment.sh
```

## Requesting Memory

Default: 32 GB per slot. Maximum per slot: 32 GB. Total maximum: 768 GB per job.

If your job needs more than 32 GB, use multiple slots:

```bash
# Batch job: 2 CPU cores, 64 GB total (32 GB per slot)
-pe sharedmem 2 -l h_rss=32G

# Interactive session
-pe interactivemem 2 -l h_rss=32G
```

> GPU RAM (80 GB per GPU / 20 GB per MIG partition) is separate from system RAM and cannot be modified.

## Requesting Runtime

```bash
-l h_rt=HH:MM:SS
```

| Session type | Maximum runtime |
|-------------|-----------------|
| Batch jobs | 48 hours |
| Interactive sessions | 24 hours |

For runtimes beyond these limits, contact the IS Helpline.

## Compiling Code

Compile CUDA code anywhere on the cluster. Prefer an interactive session over the login nodes for large compilations:

```bash
module load cuda
nvcc -o example example.cu
```
