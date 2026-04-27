# Singularity

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/Singularity>

Singularity is a container platform that allows you to create and run portable, reproducible containers. It fixes security issues introduced by Docker on HPC platforms like Eddie.

> **Note:** Do not use Singularity to run containers for applications already provided as modules (e.g. RStudio, bowtie).

The default version on Eddie is **4.1**.

## Batch Job

```bash
#!/bin/bash
#$ -N singularity
#$ -cwd
#$ -l h_rt=00:10:00

. /etc/profile.d/modules.sh
module load singularity

# Set SINGULARITY_TMPDIR (important — avoids filling /tmp)
export SINGULARITY_TMPDIR=$TMPDIR

# Set cache directory (use group space or scratch — not home)
export SINGULARITY_CACHEDIR=/exports/eddie/scratch/<USER>/singularity

singularity -vvv run library://sylabsed/examples/lolcow
```

## Interactive Session

```bash
qlogin
module load singularity
source /exports/applications/support/set_qlogin_environment.sh
export SINGULARITY_TMPDIR=$TMPDIR
export SINGULARITY_CACHEDIR=/exports/eddie/scratch/<USER>/singularity
singularity run library://crown421/default/juliabase
```

## Building Images

If you get a `mksquashfs not found` error when building:

```bash
qlogin -pe interactivemem 2
export OLDPATH=$PATH
source /exports/applications/support/set_qlogin_environment.sh
export PATH=$PATH:$OLDPATH
. /etc/profile.d/modules.sh
module load singularity
export SINGULARITY_TMPDIR=$TMPDIR
export SINGULARITY_CACHEDIR=<your_cache_dir>
singularity build <image> <definition>
```

## Singularity 4 — Shared Memory Issues

Singularity 4 can suffer from shared memory issues (Bus Errors). Fix by isolating shared memory:

```bash
export SINGULARITY_TMPDIR=$TMPDIR
singularity exec --scratch /dev/shm <image> <command>
```

This provides ~1 TB of shared memory if `$TMPDIR` points to `/local`.

## Mounting /exports

If you start a container inside a directory on `/exports` and can't find it:

```bash
# For a shell
singularity shell --bind /exports <image>

# For a pipeline
singularity --bind /exports <image>
```
