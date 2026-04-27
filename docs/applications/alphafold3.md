# AlphaFold3

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/AlphaFold3>
> **GitHub:** <https://github.com/google-deepmind/alphafold3>

> **Note:** The examples below use `-l h_vmem` which is **deprecated since September 2025**. Replace with `-l h_rss` in all new scripts. See `docs/reference/memory-specification.md`.

AlphaFold3 predicts the structure of interacting proteins, DNA, RNA, ligands, and more.

## Licence Restrictions

AlphaFold3 has **strict licence restrictions** (no commercial activities). Review the terms at: <https://github.com/google-deepmind/alphafold3/blob/main/WEIGHTS_TERMS_OF_USE.md>

To gain access, raise a helpdesk call confirming you agree with the terms of use. You will then be granted read access to the AI models.

## Architecture

AlphaFold3 separates the pipeline into:
1. **CPU step:** Data pipeline (genetic and template search) — runs on any CPU node
2. **GPU step:** Inference pipeline — runs on a GPU node

A single GPU inference job can process output from many CPU data jobs, improving throughput.

## Loading AlphaFold3

```bash
module load igmm/apps/AlphaFold3/3.0.0
```

This sets two environment variables:

```bash
echo $ALPHAFOLD3SIF   # Path to the Singularity container
echo $ALPHAFOLD3DB    # Path to the databases
```

## Input Format

Place input data in a directory called `af_input/` as a JSON file. For the CPU and GPU scripts to work together, the JSON filename must match the `"name"` field inside the file. For example, `bbc.json` must contain `"name": "BBC"` or `"name": "bbc"`.

See <https://github.com/google-deepmind/alphafold3/blob/main/docs/input.md> for JSON format documentation.

## Submitting Jobs

```bash
# Run CPU data processing for each input, then single GPU job
qsub alphafold3-cpu.sh bbc
qsub alphafold3-cpu.sh 2pv7
qsub alphafold3-gpu.sh bbc 2pv7
```

### CPU Data Processing Script (alphafold3-cpu.sh)

```bash
#! /bin/sh
#$ -S /bin/bash
#$ -cwd
#$ -l h_vmem=12G
#$ -pe sharedmem 8
# Usage: qsub alphafold3-cpu.sh <Name-Of-Input>
# <Name-Of-Input>: lowercase, no .json extension, matches "name" field in JSON

source /etc/profile.d/modules.sh
module load igmm/apps/AlphaFold3/3.0.0

NAME_OF_INPUT=$1

singularity exec \
  --scratch /dev/shm \
  --bind $PWD:$HOME \
  --bind $TMPDIR:/tmp \
  --bind af_input:/root/af_input \
  --bind af_output:/root/af_output \
  --bind $ALPHAFOLD3DB/models:/root/models \
  --bind $ALPHAFOLD3DB:/root/public_databases \
  $ALPHAFOLD3SIF \
  python /app/alphafold/run_alphafold.py \
  --json_path=/root/af_input/$NAME_OF_INPUT.json \
  --model_dir=/root/models \
  --db_dir=/root/public_databases \
  --output_dir=/root/af_output \
  --run_inference=false
```

### GPU Inference Script (alphafold3-gpu.sh)

```bash
#! /bin/sh
#$ -S /bin/bash
#$ -cwd
#$ -q gpu
#$ -l gpu=1
#$ -l h_vmem=64G
#$ -hold_jid alphafold3-cpu.sh
# Usage: qsub alphafold3-gpu.sh <Name-Of-Input> [<Name-Of-Input> ...]

source /etc/profile.d/modules.sh
module load igmm/apps/AlphaFold3/3.0.0

for NAME in $*
do
  singularity exec --nv \
    --scratch /dev/shm \
    --bind $PWD:$HOME \
    --bind $TMPDIR:/tmp \
    --bind af_input:/root/af_input \
    --bind af_output:/root/af_output \
    --bind $ALPHAFOLD3DB/models:/root/models \
    --bind $ALPHAFOLD3DB:/root/public_databases \
    $ALPHAFOLD3SIF \
    python /app/alphafold/run_alphafold.py \
    --json_path=/root/af_output/$NAME/${NAME}_data.json \
    --model_dir=/root/models \
    --db_dir=/root/public_databases \
    --output_dir=/root/af_output \
    --run_data_pipeline=false
done
```
