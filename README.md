# cptools2

Running CellProfiler on computing clusters. This is aimed towards the University of Edinburgh's Eddie3 cluster with the staging workflow.

## Installation:
`python setup.py install --user`

## Example
```python
from cptools2.job import Job

job = Job()

job.add_experiment("path/to/imageXpress/experiment")

job.remove_plate(["plate_1", "plate_2"])

# number of images per task
job.chunk(job_size=48)

job.create_commands(pipeline="/path/to/cellprofiler/pipeline.cppipe",
                    location="/path/to/scratch/space",
                    commands_location="/home/user")
```

This produces a directory containing a loaddata file for each task, and three text files containing staging commands, cellprofiler commands, and de-staging commands that can be run as three concurrent array jobs.


## From a yaml file

cptools2 can also take input from a yaml file containing the configuration for a job.

For example if we have a a file named `awesome_experiment1.yml` containing:
```yaml
experiment : path/to/imageXpress/experiment
remove plate:
    - plate_1
    - plate_2
chunk : 48
pipeline : /path/to/cellprofiler/pipeline.cppipe
location : /path/to/scratch/space
commands location : /home/user
```

We could run this as `python -m cptools2 awesome_experiment1.yml`

--------------------------

Previous version for the AFM filesystem is available [here](https://github.com/swarchal/CP_tools).


