# R

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/R>

R is a free software environment for statistical computing and graphics. The default version on Eddie is **4.4**.

```bash
module load R

# Load a specific version
module load R/<VERSION>
```

See [Applications](../README.md) for information on how to search for and load installed versions.

## Sub-pages

- [RStudio](rstudio.md) — GUI front end for R

## Installing R Packages

### 1. Choose a Storage Location

Use a directory in an Eddie group space — package installs can get large:

```
/exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP NAME>/rlibs
```

If you don't have a group space, request one. See [Storage](../../storage.md) for more information.

### 2. Configure the Package Library Path

Add the following to `~/.Rprofile` in your home directory:

```r
.libPaths("/exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP NAME>/rlibs")
```

If `~/.Rprofile` doesn't exist, start R and quit to create it first.

### 3. Build Packages in an Interactive Session

Never build R packages on a login node — compilation will likely fail.

```bash
qlogin
module load R/<version>
R
```

```r
install.packages("package_name", "/exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP NAME>/rlibs")
```

### 4. Avoid Version Mixing

A package built under one version of R may fail to load in a different major version. Always ensure your library directory contains only packages built for the currently running version:

```r
.libPaths()   # Check where R is looking for packages
```

## Submitting R Jobs

Prepare:
1. An **R script** using `Rscript` (no user interaction)
2. A **job submission script**

```bash
#!/bin/bash
#$ -N example_R
#$ -cwd
#$ -l h_rt=01:00:00,h_rss=8G
#$ -M <your_email_address>
#$ -m ea

. /etc/profile.d/modules.sh
module load R/4.3.0
Rscript your_R_script.R
```
