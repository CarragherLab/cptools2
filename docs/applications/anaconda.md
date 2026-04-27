# Anaconda

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/Anaconda>

Anaconda is an enterprise-ready Python distribution containing 330+ packages for large-scale data processing, predictive analytics, and scientific computing. It makes it easy to create bespoke Python environments.

> **Note:** Consider using [Miniforge](miniforge/README.md) for new environments — Anaconda had licensing restrictions that made it unsuitable for Eddie. Miniforge is the recommended replacement.

The default Anaconda version is **2024.02** (contains Python 3.11.7). All commands on this page should be run in an interactive session with at least 4 GB of memory.

```bash
qlogin
module load anaconda

# Or a specific version
module available | grep anaconda
```

## Creating a Conda Environment

### 1. Choose a Storage Location

Use an Eddie group space — conda environments can get very large:

```
/exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP NAME>/anaconda/envs
```

**Do not** use your home directory (10 GB quota) or scratch (files deleted after 28 days).

### 2. Configure Environment and Package Directories

```bash
qlogin
module load anaconda

# Configure envs directory
conda config --add envs_dirs /exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP NAME>/anaconda/envs

# Configure packages cache directory
conda config --add pkgs_dirs /exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP NAME>/anaconda/pkgs
```

Or edit `~/.condarc` directly:

```yaml
envs_dirs:
  - /exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP NAME>/anaconda/envs
pkgs_dirs:
  - /exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP NAME>/anaconda/pkgs
```

### 3. Create the Environment

```bash
conda create -n mypython python=3.9 numpy=1.22 scipy=1.6
```

### 4. Activate / Deactivate

```bash
conda activate mypython
python -V

conda deactivate
```

### 5. List Environments

```bash
conda info --envs
```

## Package Management

```bash
conda list                     # List packages in active environment
conda search matplotlib        # Search for a package
conda install matplotlib=3.6.0 # Install a package (dependencies also installed)
```

### Using pip

If a package is not in Anaconda repositories, install with `pip`. **Always install `pip` into your environment first:**

```bash
conda install pip
pip install <package_name>
```

## Cleaning Up

If over-quota in your home directory, clean unused packages:

```bash
conda clean --all
```

**Do not** manually delete `~/.conda/pkgs` — it will break your Anaconda installation.

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
module load anaconda/2024.02     # Load specific version
conda activate mypython          # Activate environment
python my_python_script.py
```

## Additional Documentation

- conda: <http://conda.pydata.org/docs/using/index.html>
- pip: <https://pip.pypa.io/en/latest/index.html>
