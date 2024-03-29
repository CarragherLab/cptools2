# cptools2

[![BuildStatus](https://travis-ci.org/CarragherLab/cptools2.svg?branch=master)](https://travis-ci.org/CarragherLab/cptools2)

Running CellProfiler on computing clusters. This is aimed towards the University of Edinburgh's Eddie3 cluster with the staging workflow.

## Installation:
`python setup.py install --user`




## Wiki:
For more details see the [wiki](https://github.com/CarragherLab/cptools2/wiki).

## Example:

cptools2 can use a configuration file to list the jobs parameters.

For example if we have a a file named `awesome_experiment-1.yml` containing:

```yaml
experiment : path/to/imageXpress/experiment
chunk : 46
pipeline : /path/to/cellprofiler/pipeline.cppipe
location : /path/to/scratch/space
commands location : /home/user
```

We could run this as `cptools2 awesome_experiment-1.yml`


This produces a directory containing a loaddata file for each task, and three text files containing staging commands, cellprofiler commands, and de-staging commands that can be run as three concurrent array jobs.

A script named `YYYY-MM-DD-h:m:s_SUBMIT_JOBS.sh` is created which will submit
the three jobs in the correct order. This can be run with
`./YYYY-MM-DD-h:m:s_SUBMIT_JOBS.sh` or `bash YYYY-MM-DD-h:m:s_SUBMIT_JOBS.sh`
on a login node (the ones that can submit jobs to the queue).

### yaml config options

There are configuration details you can add to a job:

- `experiment` : path to an ImageXpress experiment
- `chunk` number of imagesets per job
- `pipeline` : path to a cellprofiler pipeline
- `location` : path to where to store the loaddata modules, staged data and output
- `commands location` : path to where to store the qsub array commands
- `remove plate` : plate names in `experiment` to be removed
- `add plate` :
    - `experiment` : path to another ImageXpress experiment
    - `plates` : plate name(s) in the above experiment
- `new_ix`: if true/yes then will treat filepaths as from the new ImageXpress

i.e we could remove some plates from an experiment, and also include some plates from a different experiment

```yaml
experiment: /path/to/imageXpress/experiment

# remove two plates from current experiment
remove plate:
    - plate_1
    - plate_2

# add plates from a separate experiment
add plate:
    - experiment : /path/to/different/experiment
    - plates:
        - plate_exp2_1
        - plate_exp2_2

chunk: 96

pipeline: /pipelines/pipeline_1.cppipe

location: /path/to/scratch/space

commands location: /path/to/scratch space
```


We can also use `add plate` without a top-level `experiment` tag to add a few plates
from a large experiment.
```yaml
add plate:
    - experiment: /path/to/large/experiment
    - plates:
        - plate_1
        - plate_2
        - plate_3
chunk : 46
pipeline : /path/to/cellprofiler/pipeline.cppipe
location : /path/to/scratch/space
commands location : /home/user
```

**NOTE:** The default is expecting old ImageXpress filepaths (for backwarsd
compatibility), in order to correctly parse metadata from the new IX paths you
can specify in the config file with `new_ix`.
```yaml
experiment : path/to/imageXpress/experiment
chunk : 46
pipeline : /path/to/cellprofiler/pipeline.cppipe
location : /path/to/scratch/space
commands location : /home/user
new_ix: true
```


--------------------------

Previous version for the AFM filesystem is available [here](https://github.com/swarchal/CP_tools).


