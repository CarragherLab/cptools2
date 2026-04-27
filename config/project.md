# Project Configuration

This file gives Eddie agents the project-specific paths and conventions for cptools2.

## Project Identity

```text
PROJECT_NAME=cptools2
COLLEGE=cmvm
USER=mharvey2
GROUP=ChandranLabs
```

## Standard Eddie Paths

```text
EDDIE_HOST=eddie.ecdf.ed.ac.uk
EDDIE_GROUP_ROOT=/exports/cmvm/eddie/smgphs/groups/ChandranLabs
DATASTORE_ROOT=/exports/cmvm/datastore/smgphs/groups/ChandranLabs
SCRATCH_ROOT=/exports/eddie/scratch/$USER
```

## Project Paths

```text
PROJECT_ROOT=$EDDIE_GROUP_ROOT/cptools2
CONTAINER_DIR=$PROJECT_ROOT/containers
ENV_DIR=$PROJECT_ROOT/env
NEXTFLOW_DIR=$PROJECT_ROOT/nextflow
DATASTORE_PROJECT=$DATASTORE_ROOT/cptools2
SCRATCH_PROJECT=$SCRATCH_ROOT/cptools2
```

## Repository Context

cptools2 is being revamped into a Nextflow-first high-content imaging workflow platform for Eddie. The legacy SGE/CellProfiler implementation remains a reference for proven behavior such as plate discovery, LoadData generation, chunking, batching, command construction, and result joining.

Current execution direction:

- Local or login-node Python CLI prepares configuration and invokes Nextflow.
- Nextflow runs on Eddie through the SGE executor.
- Singularity containers hold analysis engines.
- DataStore access should happen through staging jobs.
- Scratch-aware batching should keep runs within Eddie scratch limits.

## Notes For Eddie Agents

- Use `h_rss`, not legacy `h_vmem`.
- DataStore staging jobs must use the `staging` queue.
- Do not submit `qsub` from inside compute jobs.
- Prefer Nextflow modules for new work; treat hand-written SGE scripts as reference patterns unless explicitly requested.
