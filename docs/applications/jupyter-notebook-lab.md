# Jupyter Notebook/Lab

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/Jupyter+Notebook+Lab>

The Jupyter notebook/lab is a web-based notebook environment for interactive computing. On Eddie it is installed as part of the **Anaconda module**.

- Default notebook version: **7.0.8**
- Default lab version: **4.0.11**
- Includes an **IPython kernel** for Python; additional kernels (e.g. R, Julia) can be installed in your own storage.

## Running Jupyter on Eddie

### Step 1: Start an Interactive Session

```bash
qlogin
source /exports/applications/support/set_qlogin_environment.sh
```

For more CPU cores or memory, also use a parallel environment.

### Step 2: Load Anaconda (and Optionally Activate Your Environment)

```bash
module load anaconda
conda activate env_name   # optional
```

### Step 3: Run the Eddie Jupyter Script

```bash
jupyter.sh        # for Jupyter Notebook
# OR
jupyter-lab.sh    # for Jupyter Lab
```

The script will generate instructions to follow (also detailed below).

### Step 4: Create an SSH Tunnel (on Your Local Machine)

Open a new terminal on your local computer and run the `ssh` command provided in the script output:

```bash
ssh -N -L 8739:192.41.105.50:8739 your_username@eddie.ecdf.ed.ac.uk
```

- Replace `8739` with the port number allocated for you
- Replace `192.41.105.50` with the IP address shown in the script output
- Replace `your_username` with your University username
- Enter your password if prompted and **leave the terminal open**

### Step 5: Access Jupyter via a Web Browser

Copy and paste the URL from the script output into your browser:

```
http://127.0.0.1:8739/tree?token=b97af8b03efe896ced552f0b7f07e14258c2643239d3c3b1
```

> **Note:** You cannot click these URLs — you must copy/paste them.

## Notes

- Keep **both terminals** open while using Jupyter.
- Use `Ctrl+C` in both terminals to shut down when finished.
