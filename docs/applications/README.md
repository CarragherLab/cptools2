# Applications

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/Applications>

For documentation conventions, please see [Conventions](../conventions.md).

## Application Modules

Applications are provided by Research Services and local Schools/Institutes in 'community' areas, all accessible via **Environment Modules**.

Local School/Institute modules are prefixed with the School/Institute name (e.g. `igmm/`, `roslin/`, `physics/`).

> **Note:** All module commands should be run in an interactive (`qlogin`) session for accurate results.

```bash
# List all available modules
module available

# Load a specific module
module load <MODULENAME/MODULEVERSION>

# Search for a module (Research Services modules only)
module available <MODULENAME>

# Search across all modules (including community modules)
module available | grep <MODULENAME>

# List currently loaded modules
module list

# Get help on a module
module whatis <MODULENAME/MODULEVERSION>
module help <MODULENAME/MODULEVERSION>

# General help
module --help
```

## Specific Applications

The following applications have dedicated documentation:

| Application | Description |
|-------------|-------------|
| [AlphaFold (ParaFold)](alphafold-parafold.md) | Protein structure prediction with ParaFold |
| [AlphaFold3](alphafold3.md) | AlphaFold3 for proteins, DNA, RNA, ligands |
| [Anaconda](anaconda.md) | Python distribution and environment manager |
| [Java](java.md) | Java via environment modules |
| [Jupyter Notebook/Lab](jupyter-notebook-lab.md) | Web-based interactive computing |
| [Matlab](matlab.md) | MATLAB with Parallel Computing Toolbox |
| [Miniforge](miniforge/README.md) | Lightweight conda environment manager |
| [Python](python.md) | Python (standalone and via Anaconda) |
| [R](r/README.md) | R statistical computing environment |
| [Singularity](singularity.md) | Container platform for HPC |
| [Spatial analysis](spatial-analysis.md) | Geospatial tools (GDAL, GRASS, sf, geopandas) |
| [TensorFlow](tensorflow.md) | TensorFlow machine learning framework |

For Bioinformatics applications, see the [Bioinformatics](https://www.wiki.ed.ac.uk/display/ResearchServices/Bioinformatics) page.

## Rocky Linux Packages

The OS provides additional packages:

```bash
# List all installed packages
dnf list installed

# Search for additional packages
dnf list search <SEARCH_STRING>
```

Request package installations via the IS Helpline.

## Installing Your Own Applications

If we cannot install an application for you, you can install it yourself:

1. **Use group storage:** `/exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP NAME>/...` — sufficient space and persistent. Avoid home (too small) and scratch (files deleted after 28 days).
2. **Use a `qlogin` session** for the installation — do not build on login nodes.
3. **Check available compilers and cmake:** Use system versions or search with `module avail | grep cmake` and `module avail | grep <COMPILER_NAME>`.

Contact the IS Helpline for further assistance.
