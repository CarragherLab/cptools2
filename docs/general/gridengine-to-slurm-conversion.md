# Gridengine to Slurm Conversion

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/Gridengine+to+Slurm+conversion>

Quick reference for converting Gridengine (SGE) commands to Slurm equivalents.

## User Commands

| Action | Gridengine | Slurm |
|--------|-----------|-------|
| Interactive login | `qrsh` | `srun --pty bash` |
| Interactive login with X11 forwarding | `qrsh` | `srun --x11 --pty bash` |
| Job submission | `qsub jobscript` | `sbatch jobscript` |
| Specific PE/partition | `qsub -pe pename nslots jobscript` | `sbatch -p pname jobscript` |
| Wildcard PE/partition | `qsub -pe \* 10 jobscript` | `sbatch -p \* jobscript` |
| Partial wildcard PE | `qsub -pe shared\* 10 jobscript` | `sbatch -p shared\* -N 10 jobscript` |
| Run on 2 nodes (1 process/node) | `qsub -pe sharedmem_1 2 jobscript` | `sbatch -p ompi\* -N 2 jobscript` |
| Run on 4 nodes, 2 processes/node | `qsub -pe shared\*_2 8 jobscript` | `sbatch -p ompi\* -N 4 -c 2 jobscript` |
| Resource request | `qsub -l feature[=value]` | `sbatch -C feature[:value]` |
| Negative resource request (avoid node) | `qsub -l h='!node1h17'` | Not possible — use positive features |
| Runtime limit | `qsub -l h_rt=HH:MM:SS` | `sbatch --time=minutes` |
| Request GPUs | `qsub -l gpu=N` | `sbatch --gres=gpu:N` |
| Job deletion | `qdel jobid` | `scancel jobid` |
| Job status by ID | `qstat -j jobid` | `squeue -j jobid` |
| Queue list | `qstat` | `squeue` |
| List nodes | `qhost` | `sinfo -N -o "%10N %6c %10m %G"` |

## Batch Script Comparison

**Gridengine:**

```bash
#!/bin/bash --login
#$ -N gpu
#$ -o gpu.$HOSTNAME.$JOB_ID.out
#$ -j y
#$ -cwd
#$ -pe sharedmem 2
#$ -l gpu=1

module load cuda/12.0
nvidia-smi
```

**Slurm:**

```bash
#!/bin/bash --login
#SBATCH --job-name=gpu
#SBATCH --output=gpu.%N.%A.out
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=2
#SBATCH --gres=gpu:1

module load cuda/12.0
nvidia-smi
```
