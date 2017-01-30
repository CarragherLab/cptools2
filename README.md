# cptools2

Running CellProfiler on computing clusters. This is aimed towards the University of Edinburgh's Eddie3 cluster with the staging workflow.

## Installation:
`python setup.py install --user`

## Example

cptools2 can use a configuration file to list the jobs parameters.

For example if we have a a file named `awesome_experiment-1.yml` containing:

```yaml
experiment : path/to/imageXpress/experiment
remove plate:
    - plate_-1
    - plate_0
chunk : 46
pipeline : /path/to/cellprofiler/pipeline.cppipe
location : /path/to/scratch/space
commands location : /home/user
```

We could run this as `python -m cptools2 awesome_experiment-1.yml`


This produces a directory containing a loaddata file for each task, and three text files containing staging commands, cellprofiler commands, and de-staging commands that can be run as three concurrent array jobs.

--------------------------

Previous version for the AFM filesystem is available [here](https://github.com/swarchal/CP_tools).


