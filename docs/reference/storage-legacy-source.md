# Storage Systems on Eddie — Legacy Source Document

**Status:** Superseded — retained as original source document  
**Replaced by:** [storage-and-staging.md](storage-and-staging.md)

---

> This is the original University of Edinburgh wiki text for storage systems
> and data staging on Eddie. It is preserved as a primary source document.
> Note the staging script examples use `-pe sharedmem 1` which is actually
> incompatible with the staging queue — this was corrected in the current guide.

---

## Original Text

Before we dive into the specifics of data staging, it's important to understand
the different storage systems available and their roles in the HPC environment.

The two main storage systems you'll work with are:

1. **DataStore** — the university's central, long-term storage system. Designed
   for safely storing large volumes of data with full backups and disaster
   recovery capability. DataStore is where you should keep your "golden copy"
   master datasets and final analysis results. It's not directly accessible from
   compute jobs running on Eddie.

2. **Eddie Filesystem** — the high-performance parallel filesystem directly
   attached to the Eddie compute cluster. Optimised for the input/output (I/O)
   patterns of HPC workloads. Not backed up and should not be used for long-term
   storage.

### Eddie Filesystem Areas

- **Home**: `/home/<UUN>` — 10 GB, backed up daily
- **Group Space**: `/exports/<COLLEGE>/eddie/<SCHOOL>/groups/<GROUP NAME>` —
  200 GB+, not backed up
- **Scratch**: `/exports/eddie/scratch/<UUN>` — 2 TB, auto-purged after 1 month,
  not backed up

### Staging Node Mounts

| College | Mount path |
|---|---|
| Science and Engineering | `/exports/csce/datastore` |
| Medicine and Veterinary Medicine | `/exports/cmvm/datastore` |
| Arts, Humanities and Social Sciences | `/exports/chss/datastore` |
| Institute of Genetics and Cancer | `/exports/igmm/datastore` |

### Example Staging Scripts (Original)

> ⚠️ Note: These original examples include `-pe sharedmem 1` in staging jobs.
> This is actually incompatible with the staging queue and should be omitted.
> See [storage-and-staging.md](storage-and-staging.md) for corrected versions.

**Stage in:**
```bash
#!/bin/sh
#$ -N stagein
#$ -cwd
#$ -l h_rt=01:00:00
#$ -pe sharedmem 1   # ← omit this in staging scripts
#$ -q staging

rsync -av --progress "${DATASTORE_DIR}/" "${EDDIE_GROUP_DIR}/data/"
```

**Stage out:**
```bash
#!/bin/sh
#$ -N stageout
#$ -cwd
#$ -l h_rt=01:00:00
#$ -pe sharedmem 1   # ← omit this in staging scripts
#$ -q staging

rsync -av --progress "${EDDIE_GROUP_DIR}/results/" "${DATASTORE_DIR}/"
```

**Chained pipeline:**
```bash
qsub -N stagein stagein.sh
qsub -N compute -hold_jid stagein compute.sh
qsub -N stageout -hold_jid compute stageout.sh
```

### Performance Tips (Original)

1. Use rsync instead of cp
2. Avoid many small files — combine into archives
3. Use parallel rsync transfers with care
4. Minimise back-and-forth staging
5. Use named pipes for streaming where possible
6. Monitor staging progress with `--progress`
