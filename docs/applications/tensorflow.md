# TensorFlow

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/TensorFlow>

This page covers the use of TensorFlow on the Eddie cluster. Please read carefully before using TensorFlow on Eddie.

## Installation

TensorFlow is installed as a **conda environment**. Read the [Anaconda](anaconda.md) page before attempting installation.

All installations must be done in an **interactive session on a GPU node**:

```bash
qlogin -q gpu -l gpu=1 -l h_rt=24:00:00
```

Specify a CUDA Toolkit version of **12.5 or lower** for compatibility with Eddie's NVIDIA kernel modules.

### Minimal Installation (TensorFlow 2.1, CUDA 10.1)

```bash
module load anaconda
conda create -n tensorflow tensorflow=2.1 cudatoolkit=10.1
```

### Activate / Deactivate

```bash
module load anaconda
conda activate tensorflow
conda deactivate
```

> **Do not** use `module load cuda` when the conda environment already contains a CUDA Toolkit.

## Installing Other Versions

### From conda-forge (e.g. TensorFlow 2.17)

```bash
module load anaconda
conda create -n tensorflow -c conda-forge tensorflow=2.17 cuda-toolkit=12.5
```

### With pip

```bash
module load anaconda
conda create -n tensorflow pip
conda activate tensorflow
pip install tensorflow[and-cuda]
```

## CUDA Toolkit Compatibility

Tested compatible versions: **CUDA 10.x, 11.x, and up to 12.5**. CUDA versions above 12.5 are not currently compatible.

## Memory

If your job fails with memory errors, request more than the default 32 GB:

```bash
#$ -l gpu=1
#$ -pe sharedmem 2
#$ -l h_rss=32G
```

Also consider adding `gpu_options.allow_growth = True` in your Python code:

```python
cfg = tf.ConfigProto()
cfg.gpu_options.allow_growth = True
with tf.Session(config=cfg) as sess:
    ...
```

## CUDA_VISIBLE_DEVICES

Do not define or modify `CUDA_VISIBLE_DEVICES`. In interactive sessions, set the scheduler environment after starting your session:

```bash
source /exports/applications/support/set_qlogin_environment.sh
```
