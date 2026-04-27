# Python

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/Python>

Python on Eddie is available as a standalone application or as part of [Anaconda](anaconda.md) / [Miniforge](miniforge/README.md).

## System Python

Python 3.9.18 is available without loading a module:

```bash
qlogin
python
```

## Other Standalone Python Versions

```bash
module available | grep python
```

## Python via Anaconda

The default Anaconda module (2024.02) contains Python 3.11.7:

```bash
qlogin
module load anaconda
python
```

```bash
# List available Anaconda versions
module available | grep anaconda
```

## Custom Python Environments

For specific Python versions or package combinations, create your own conda environment. See [Anaconda](anaconda.md) or [Miniforge](miniforge/README.md) for full instructions.
