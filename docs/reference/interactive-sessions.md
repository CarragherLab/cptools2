# Interactive Sessions

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/Interactive+Sessions>

There are a limited number of nodes that accept interactive login sessions, allowing you to run interactive jobs or graphical applications.

## Starting an Interactive Session

```bash
qlogin
```

The default and maximum runtime is **48 hours**. To specify a shorter runtime (recommended):

```bash
qlogin -l h_rt=HH:MM:SS
```

If you finish before the runtime is reached, please run `exit` to release the resource for other users.

## Setting the Scheduler Environment

The scheduler does not automatically set its environment (e.g. `$TMPDIR`, `$NSLOTS`) in qlogin sessions. Set it manually once you have an interactive session:

```bash
source /exports/applications/support/set_qlogin_environment.sh
```

Always run this at the start of your interactive session.

## Requesting Memory

If you do not specify memory, you will get 1 CPU core and typically 16–32 GB memory per core.

To request a specific amount (single core, up to 32 GB):

```bash
qlogin -l h_rss=<MEMORY>
```

## Requesting Multiple Slots (CPU Cores / Large Memory)

Use the `interactivemem` parallel environment to request multiple CPU cores or to aggregate memory:

```bash
# Multiple CPU cores with default memory
qlogin -pe interactivemem <NUMBER_OF_SLOTS>

# Multiple slots with specified memory per slot
qlogin -pe interactivemem <NUMBER_OF_SLOTS> -l h_rss=<MEMORY_PER_SLOT>
```

Total memory = `<NUMBER_OF_SLOTS>` × `<MEMORY_PER_SLOT>`. Maximum: up to 32 GB per core; up to 1 TB total.

See [Memory Specification](memory-specification.md) for more details.

## Reconnecting to an Interactive Session

Use `screen` or `tmux` to reconnect to a session after a network drop.

### Using screen

```bash
# On the login node, start screen and note which login node you are on (login01 or login02)
screen

# Start your interactive session
qlogin

# If disconnected, reconnect to the same login node:
ssh login01-ext.ecdf.ed.ac.uk

# List your screen sessions
screen -ls

# Re-attach
screen -r <pid.tty.host>
```

## Submitting Jobs from an Interactive Session

You can submit jobs with `qsub` from an interactive session. First load the UGE module:

```bash
module load uge
qsub jobscript.sh
```

## Further Information

- `man qlogin` — full qlogin command options
- [GPU Interactive Sessions](gpus.md#interactive-sessions)
- [Data Staging interactive sessions](data-staging/README.md#interactive-staging)
