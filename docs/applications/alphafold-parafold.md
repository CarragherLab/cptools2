# AlphaFold (ParaFold)

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/AlphaFold+ParafFold>
> **GitHub:** <https://github.com/Zuricho/ParallelFold>

> **Note:** The examples below use `-l h_vmem` which is **deprecated since September 2025**. Replace with `-l h_rss` in all new scripts. See `docs/reference/memory-specification.md`.

The **ParaFold** implementation of AlphaFold splits computation into two parts:
1. **CPU step:** Multiple sequence alignments (8 CPUs)
2. **GPU step:** Structure prediction (single GPU)

This gives the most efficient use of resources on Eddie — the CPU part can run anywhere and the GPU nodes are reserved for prediction only. ParaFold only requires miniconda (unlike AlphaFold which also requires Singularity or Docker).

> **Note:** You can replace `miniconda` with `miniforge` in the scripts below:
> ```bash
> module use igmm/apps/ParaFold igmm/apps/miniforge/24.3.0
> ```

## Running on Eddie

Create `input/` and `output/` directories in your working directory, and place your FASTA file in `input/`. The AlphaFold genetic databases are at `/exports/igmm/eddie/AlphaFoldDB`.

```bash
qsub AlphaFold2-CPU.sh T1050.fasta
qsub AlphaFold2-GPU.sh T1050.fasta
```

### CPU Alignment Script (AlphaFold2-CPU.sh)

```bash
#! /bin/sh
#$ -S /bin/bash
#$ -cwd
#$ -l h_vmem=12G
#$ -pe sharedmem 8

source /etc/profile.d/modules.sh
module load igmm/apps/ParaFold igmm/apps/miniconda3/23.5.2
eval "$(conda shell.bash hook)"
conda activate parafold

run_alphafold.sh \
  -d /exports/igmm/eddie/AlphaFoldDB \
  -o ./output \
  -i ./input/$1 \
  -t 2022-01-01 \
  -p monomer \
  -m model_1 \
  -c reduced_dbs \
  -f
```

### GPU Prediction Script (AlphaFold2-GPU.sh)

```bash
#! /bin/sh
#$ -S /bin/bash
#$ -cwd
#$ -l h_vmem=384G
#$ -q gpu
#$ -pe gpu-a100 1
#$ -hold_jid AlphaFold2-CPU.sh
#$ -l h_rt=2:0:0:0

source /etc/profile.d/modules.sh
module load igmm/apps/ParaFold igmm/apps/miniconda3/23.5.2
eval "$(conda shell.bash hook)"
conda activate parafold

run_alphafold.sh \
  -d /exports/igmm/eddie/AlphaFoldDB \
  -o ./output \
  -i ./input/$1 \
  -t 2022-01-01 \
  -p monomer \
  -m model_1,model_2,model_3,model_4,model_5 \
  -c reduced_dbs \
  -g
```

## Multimer Predictions

For protein-protein interactions, use multimer scripts by changing the `-p` and `-m` options to multimer variants and placing multiple proteins in one FASTA file.

```bash
qsub AlphaFold2-CPU-multimer.sh RGPD6-LIMS3.fasta
qsub AlphaFold2-GPU-multimer.sh RGPD6-LIMS3.fasta
```

## GPU Step Options

| Option | Description |
|--------|-------------|
| `-G` | Disable GPU relax |
| `-r all/best/none` | Models to relax (default: `all`) |
| `-m <model_names>` | Comma-separated model names (default: all 5 models) |

## AlphaFoldPost

Generate confidence plots for AlphaFold/ParaFold output:

```bash
module load igmm/apps/AlphaFoldPost
postprocessing.py -o <plots-directory> <alphafold-output-directory>
```
