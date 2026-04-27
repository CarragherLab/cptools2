# Storage Systems and Data Staging on Eddie

**Source:** University of Edinburgh Research Services wiki  
**Applies to:** Eddie HPC cluster, Rocky Linux 9

---

## The Two Storage Systems

### DataStore — Long-term Archive

The university's central, long-term storage system:

- Full backups and disaster recovery
- Designed for "golden copy" master datasets and final results
- **Not directly accessible from compute jobs** — staging nodes only
- Accessible from desktops via UNC paths (Windows) or NFS mount

### Eddie Filesystem — High-Performance Compute Storage

Optimised for HPC I/O. Not backed up. Three areas:

| Area | Path | Quota | Backed up | Purpose |
|---|---|---|---|---|
| **Home** | `/home/<UUN>` | 10 GB | Daily | Config files, job scripts |
| **Group space** | `/exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP>` | 200 GB+ | No | Shared project data |
| **Scratch** | `/exports/eddie/scratch/<UUN>` | 2 TB | No | Active job I/O — auto-purged after 1 month |

---

## Chandran Lab Paths

| Location | Path |
|---|---|
| **Group space (Eddie)** | `/exports/cmvm/eddie/scs/groups/chandranlabs/` |
| **DataStore (Eddie mount)** | `/exports/cmvm/datastore/scs/groups/chandranlabs/` |
| **DataStore (Windows UNC)** | `\\cmvm.datastore.ed.ac.uk\cmvm\scs\groups\chandranlabs\` |
| **Scratch** | `/exports/eddie/scratch/mharvey2/` |

The `DRUGSEQ_DATA_ROOT` environment variable pattern is used in lab pipelines
to abstract between Windows UNC and Linux mount paths:

```bash
# In .env file:
DRUGSEQ_DATA_ROOT=/exports/cmvm/datastore/scs/groups/chandranlabs/drugseq
# or on Windows:
DRUGSEQ_DATA_ROOT=\\cmvm.datastore.ed.ac.uk\cmvm\scs\groups\chandranlabs\drugseq
```

---

## Why Data Staging is Necessary

DataStore is not tuned for heavy computation. The Eddie filesystem is not
suitable for long-term retention. Staging bridges the two:

```
Desktop → DataStore → [staging node] → Eddie filesystem → [compute node]
                                                        ↓
DataStore ← [staging node] ← Eddie filesystem ← results
```

---

## Staging Nodes

Staging nodes have dedicated DataStore access via NFS. They are the **only**
nodes from which DataStore paths resolve.

### Accessing Staging Nodes

```bash
# Interactive
qlogin -q staging

# Batch
qsub -q staging my_staging_script.sh
```

### Critical Staging Queue Rules

> ⚠️ **`-pe sharedmem` is incompatible with the staging queue.** Omit it from
> all staging scripts. This is a common, silent failure mode.

> ⚠️ **Never reference `/exports/.../datastore/` paths from compute jobs.**
> They will fail — DataStore is not mounted on compute nodes.

### DataStore Mount Points on Staging Nodes

| College / Institute | Staging path |
|---|---|
| Medicine and Veterinary Medicine | `/exports/cmvm/datastore` |
| Science and Engineering | `/exports/csce/datastore` |
| Arts, Humanities, Social Sciences | `/exports/chss/datastore` |
| Genetics and Cancer (IGC/IGMM) | `/exports/igmm/datastore` |

---

## Example Staging Scripts

### Stage In (DataStore → Eddie)

```bash
#!/bin/sh
#$ -N stage_in
#$ -cwd
#$ -q staging
#$ -l h_rt=02:00:00
#$ -l h_rss=4G
# No -pe sharedmem — incompatible with staging queue

SRC="/exports/cmvm/datastore/scs/groups/chandranlabs/myproject/data"
DST="/exports/cmvm/eddie/scs/groups/chandranlabs/myproject/data"

rsync -av --progress "${SRC}/" "${DST}/"
```

### Stage Out (Eddie → DataStore)

```bash
#!/bin/sh
#$ -N stage_out
#$ -cwd
#$ -q staging
#$ -l h_rt=02:00:00
#$ -l h_rss=4G

SRC="/exports/cmvm/eddie/scs/groups/chandranlabs/myproject/results"
DST="/exports/cmvm/datastore/scs/groups/chandranlabs/myproject/results"

rsync -av --progress "${SRC}/" "${DST}/"
```

### Full Chained Pipeline with Staging

```bash
# All submitted from login node upfront — compute nodes cannot submit jobs
qsub -N stage_in   scripts/01_stage_in.sh
qsub -N process    -hold_jid stage_in    scripts/02_process.sh
qsub -N stage_out  -hold_jid process     scripts/03_stage_out.sh
```

---

## Using `$TMPDIR` for Temporary Files

Every job gets a private `$TMPDIR` that is automatically cleaned up on exit.
Use it for intermediate files to avoid cluttering scratch:

```bash
#!/bin/sh
#$ -N my_job
#$ -cwd
#$ -l h_rss=16G

RESULTS="/exports/cmvm/eddie/scs/groups/chandranlabs/myproject/results"

# Intermediate work in $TMPDIR — auto-cleaned
process_data.py --temp "$TMPDIR" --output "$RESULTS/"
```

---

## Scratch Space Management

- **Quota:** 2 TB per user
- **Purge:** Files auto-deleted after 1 month of inactivity
- **Never** rely on scratch for final results — always copy to group space or DataStore

```bash
# Check usage
du -sh /exports/eddie/scratch/$USER/

# Find files older than 20 days
find /exports/eddie/scratch/$USER/ -mtime +20 -type f | head -20
```

---

## Staging Performance Tips

1. **Use rsync not cp** — only transfers changed files, compresses in transit,
   resumes interrupted transfers
2. **Avoid many small files** — per-file overhead is significant; tar before staging
   ```bash
   tar -czf data.tar.gz data/
   rsync -av data.tar.gz "${DST}/"
   ```
3. **Note trailing slashes** — `rsync "${SRC}/"` copies the *contents* of SRC;
   without the slash it copies the directory itself
4. **Minimise round trips** — combine processing steps to reduce staging operations
5. **Use `$TMPDIR`** for intermediate files — auto-managed, no cleanup required

---

## File Permissions on DataStore (NFS)

DataStore uses **Windows permissions** natively. The NFSv3 mounts on Eddie staging
nodes present these in a Linux format that is **misleading** — do not trust the
output of `ls -l` on DataStore mounts.

**Critical rules:**
- Always check and update permissions **on Windows**, not via Linux commands
- **Never use `chmod` or `chgrp`** on NFS-mounted DataStore paths — this strips
  inherited Windows permissions (including Service Delivery recovery access) and
  breaks shared access
- **Never use `cp -p` or `rsync -a`** when copying to/from DataStore — the `-p`
  (preserve permissions) and `-a` (archive, implies `-p`) flags can destroy
  destination ACLs. Use `rsync -rtl` instead.
- File sharing configured on DataStore via Windows will generally work on staging
  nodes without needing manual Linux permission changes

> **Why `ls -l` is misleading:** A file with full group access in Windows may show
> `-rw------- 1 user group` on the NFS mount — the group permissions appear missing
> even though the group has full control. Only after running `chmod` (which damages
> the underlying permissions) does the Linux view become accurate.

---

## Advanced Staging Patterns

### Automatic Resubmission for Large Transfers

For large datasets that may exceed the staging job runtime, use `-r yes` with
signal trapping. If the job runs out of time, SGE resubmits it and `rsync`
continues from where it left off:

**Stage in (DataStore → Eddie):**

```bash
#!/bin/bash
#$ -N stagein
#$ -cwd
#$ -q staging
#$ -l h_rt=12:00:00
#$ -r yes
#$ -notify
trap 'exit 99' sigusr1 sigusr2 sigterm

SOURCE=/exports/igmm/datastore/<SOURCE_DIR>
DESTINATION=/exports/eddie/scratch/$USER/<DESTINATION_DIR>

# Note: do NOT use -p or -a — can break file ACLs
rsync -rtl ${SOURCE} ${DESTINATION}
```

**Stage out (Eddie → DataStore):**

```bash
#!/bin/bash
#$ -N stageout
#$ -cwd
#$ -q staging
#$ -l h_rt=12:00:00
#$ -r yes
#$ -notify
trap 'exit 99' sigusr1 sigusr2 sigterm

SOURCE=/exports/eddie/scratch/$USER/<SOURCE_DIR>
DESTINATION=/exports/<COLLEGE>/datastore/<DESTINATION_DIR>

# Note: do NOT use -p or -a — can break file ACLs at the destination
rsync -rtl ${SOURCE} ${DESTINATION}
```

**Key flags:**
- `-r yes` — resubmit if job exceeds runtime
- `-notify` — SGE sends SIGUSR1/SIGUSR2 before killing the job
- `trap 'exit 99'` — clean exit on signal so SGE can resubmit
- `rsync -rtl` — recursive, preserve times, copy symlinks (no `-p` or `-a`)

---

## Related Documentation

- [submitting-jobs.md](submitting-jobs.md) — full job script guide including `-q staging`
- [parallel-environments.md](parallel-environments.md) — staging queue PE constraints
- [getting-started.md](getting-started.md) — Eddie overview and key paths
