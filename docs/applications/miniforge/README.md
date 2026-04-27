# Miniforge

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/Miniforge>

Miniforge3 is a virtual environment manager for Python and a **drop-in replacement for Anaconda**. Anaconda had licensing restrictions making it unsuitable for Eddie; Miniforge is the recommended alternative.

The default Miniforge version is **24.7.1** (contains Python 3.12.5).

```bash
qlogin
module load miniforge
conda list
```

## Sub-pages

- [bcbio](bcbio.md) — Automated high-throughput sequencing analysis pipeline

## Creating a Conda Environment

### 1. Configure Storage Directories

Use an Eddie group space — environments can get very large:

```bash
qlogin
module load miniforge

conda config --add envs_dirs /exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP NAME>/miniforge/envs
conda config --add pkgs_dirs /exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP NAME>/miniforge/pkgs
```

Or edit `~/.condarc` directly:

```yaml
envs_dirs:
  - /exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP NAME>/miniforge/envs
pkgs_dirs:
  - /exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP NAME>/miniforge/pkgs
```

### 2. Create the Environment

```bash
conda create -n mypython python=3.9 numpy=1.22 scipy=1.6
```

### 3. Activate / Deactivate

```bash
conda activate mypython
python -V

conda deactivate
```

### 4. List Environments

```bash
conda info --envs
```

## Package Management

```bash
conda list                     # List packages in active environment
conda search matplotlib        # Search for a package
conda install matplotlib=3.6.0 # Install a package
```

### Using pip

```bash
conda install pip
pip install <package_name>
```

## Cleaning Up

```bash
conda clean --all
```

Do not manually delete `~/.conda/pkgs`.

## Example Job Script

```bash
#!/bin/bash
#$ -N test_conda
#$ -cwd
#$ -l h_rt=00:30:00
#$ -l h_rss=8G
#$ -m bea
#$ -M <uun>@ed.ac.uk

. /etc/profile.d/modules.sh
module load miniforge/24.7.1
conda activate mypython
python my_python_script.py
```

## Additional Documentation

- conda: <https://docs.conda.io/projects/conda/en/latest/user-guide/>
- pip: <https://pip.pypa.io/en/latest/index.html>
