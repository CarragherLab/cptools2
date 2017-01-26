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

# number of images per sub-job
job.chunk(job_size=48)

job.create_commands(pipeline="/path/to/cellprofiler/pipeline.cppipe",
                    location="/path/to/scratch/space",
                    commands_location="/home/user")
```

Previous version for the AFM filesystem is available [here](https://github.com/swarchal/CP_tools).


