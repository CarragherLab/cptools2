# Submitting Jobs on Eddie

**Source:** University of Edinburgh Research Services wiki  
**Applies to:** Eddie HPC cluster, Rocky Linux 9

---

## Overview

Rather than executing programs directly, you submit jobs to a batch scheduling system
(Grid Engine) which distributes the work across the cluster's compute nodes. Job scripts
are submitted with `qsub` from a login node.

---

## Step 1: Understand Your Program's Requirements

Before writing a job script, establish:

- How many CPU cores can it use effectively?
- What is its peak RAM requirement?
- How long does it typically run?
- Does it read/write many small files, or a few large ones?

Use `qacct -j <JOBID>` on previous runs to answer these questions empirically.

---

## Step 2: Write the Job Script

A job script contains four parts:

1. Shebang line — `#!/bin/sh` (must be first)
2. `#$` directives — scheduler resource requests
3. Environment setup — module initialisation and loading
4. Commands — the actual work

### Required Directives

```bash
#!/bin/sh
#$ -N JobName              # Job name (no spaces)
#$ -cwd                    # Run in current directory
#$ -l h_rt=HH:MM:SS        # Hard runtime limit
#$ -l h_rss=<SIZE>         # RSS memory per slot (e.g. 16G)
```

### Optional Directives

```bash
#$ -pe sharedmem <N>       # N CPU cores on one node
#$ -t 1-100                # Array job: 100 tasks
#$ -tc 50                  # Max 50 concurrent array tasks
#$ -hold_jid <name/id>     # Wait for another job to finish
#$ -q staging              # Use staging queue (DataStore access)
#$ -o logs/job.o$JOB_ID    # Custom stdout log path
#$ -e logs/job.e$JOB_ID    # Custom stderr log path
#$ -j y                    # Merge stdout and stderr
#$ -R y                    # Resource reservation (use with mpi-32)
```

### Complete Template

```bash
#!/bin/sh
#$ -N my_analysis
#$ -cwd
#$ -pe sharedmem 8
#$ -l h_rt=04:00:00
#$ -l h_rss=16G
#$ -o logs/my_analysis.o$JOB_ID
#$ -e logs/my_analysis.e$JOB_ID

# Initialise module system
. /etc/profile.d/modules.sh

# Load required software
module load python/3.11.4

# Set thread count for parallel libraries
export OMP_NUM_THREADS=$NSLOTS

# Validate inputs
INPUT=/exports/cmvm/eddie/scs/groups/chandranlabs/myproject/data
if [ ! -d "$INPUT" ]; then
    echo "ERROR: Input directory not found: $INPUT"
    exit 1
fi

# Run
python my_analysis.py --threads "$NSLOTS" --input "$INPUT"

echo "Job complete: $(date)"
```

---

## Step 3: Submit the Job

```bash
qsub myjob.sh
# Your job 1234567 ("my_analysis") has been submitted
```

Note the job ID — it's used for monitoring and dependency chaining.

---

## Step 4: Monitor the Job

```bash
# All your jobs
qstat

# Detailed info on a specific job (including why it's queued)
qstat -j 1234567

# Cancel a job
qdel 1234567
```

**Job state codes:**

| Code | Meaning |
|---|---|
| `qw` | Queued, waiting for resources |
| `r` | Running |
| `hqw` | Held, waiting for dependency |
| `Eqw` | Error — check `qstat -j <JOBID>` |

---

## Step 5: Check Job Output and Performance

Log files are written to the working directory by default:
- `JobName.o1234567` — stdout
- `JobName.e1234567` — stderr

Check actual resource usage after completion:

```bash
qacct -j 1234567
```

**Key fields:**

| Field | Meaning |
|---|---|
| `failed` | 0 = success, non-zero = failure |
| `exit_status` | Return code from your program |
| `wallclock` | Actual wall-clock runtime |
| `maxrss` | Peak RSS memory used — use this to tune `h_rss` |
| `slots` | Number of cores allocated |
| `cpu` | Total CPU time across all slots |

---

## Common Variables Available Inside Job Scripts

```bash
$JOB_ID          # Numeric job ID (same for all array tasks)
$JOB_NAME        # Job name from -N directive
$NSLOTS          # Number of allocated cores
$SGE_TASK_ID     # Array task index (array jobs only)
$TMPDIR          # Job-specific temp directory (auto-cleaned on exit)
$HOSTNAME        # Compute node name
```

---

## Advanced Topics

### Array Jobs

Run many similar tasks in parallel, parameterised by `$SGE_TASK_ID`:

```bash
#$ -t 1-100
#$ -tc 20              # Max 20 concurrent tasks

INPUT=$(sed -n "${SGE_TASK_ID}p" samples.txt)
python process.py --sample "$INPUT" --out "results/${SGE_TASK_ID}.txt"
```

**Non-sequential filenames:** Generate the file list first, then index by task ID:

```bash
# Before submitting: create the file list
ls data/*.fastq.gz > filestoprocess.txt
wc -l filestoprocess.txt   # count → use as -t upper bound

# In the job script: look up filename by line number
FILE=$(sed -n "${SGE_TASK_ID}p" < filestoprocess.txt)
process.sh "$FILE"
```

### Job Chaining with Dependencies

Submit a chain of jobs that execute in sequence:

```bash
# All submitted upfront from the login node
qsub -N stage_in    stage_in.sh
qsub -N process     -hold_jid stage_in   process.sh
qsub -N stage_out   -hold_jid process    stage_out.sh
```

Use job names (not IDs) for `-hold_jid` — they're more robust and self-documenting.

**Critical rule:** Never call `qsub` from inside a job script. Compute nodes cannot
submit jobs. All chaining must be set up from the login node before any jobs run.

### Interactive Sessions

```bash
# Standard interactive session
qlogin -pe interactivemem 4 -l h_rss=32G -l h_rt=02:00:00

# Staging node (DataStore access)
qlogin -q staging
```

---

## Related Documentation

- [memory-specification.md](memory-specification.md) — how to size `h_rss`
- [parallel-environments.md](parallel-environments.md) — sharedmem, mpi-32, gpu-a100, scatter
- [storage-and-staging.md](storage-and-staging.md) — staging queue, DataStore paths
