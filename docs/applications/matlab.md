# Matlab

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/Matlab>

Matlab on Eddie, in conjunction with the **Parallel Computing Toolbox**, enables multi-core parallel processing. Multiple versions are installed; default is **R2021b**, most recent is **R2024a**.

## Running Matlab Interactively

### Step 1: Log In with X11 Forwarding

- **Windows:** Download and install [MobaXterm](https://mobaxterm.mobatek.net/) (X11 forwarding is automatic).
- **macOS:** Download [XQuartz](https://www.xquartz.org/) and use the `-X` flag:
  ```bash
  ssh -X eddie.ecdf.ed.ac.uk
  ```
- **Linux:**
  ```bash
  ssh -X eddie.ecdf.ed.ac.uk
  ```

### Step 2: Start an Interactive Session

```bash
qlogin -l h_rss=16G
source /exports/applications/support/set_qlogin_environment.sh
```

### Step 3: Load Matlab

```bash
module load matlab
# Or a specific version — see available versions with: module available matlab
```

### Step 4: Start Matlab

```bash
matlab &
```

If the GUI doesn't open, check X11 forwarding is enabled and your `$DISPLAY` variable is set.

### Step 5: Exit When Finished

```bash
exit
```

## Running Matlab as a Batch Job

### Method 1: Compile First, Then Run

```bash
# In a qlogin session:
module load matlab
mcc -m matlabscript.m
# This produces: matlabscript and run_matlabscript.sh
```

Job script:

```bash
#!/bin/bash
. /etc/profile.d/modules.sh
module load matlab
./matlabscript
```

### Method 2: Run Without Compiling

```bash
#!/bin/bash
. /etc/profile.d/modules.sh
module load matlab
matlab -nodesktop < matlabscript.m
```

For support, contact the IS Helpline.
