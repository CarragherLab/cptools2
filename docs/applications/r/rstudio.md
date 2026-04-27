# RStudio

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/RStudio>

RStudio is a GUI front end to the R statistical modelling software. It is run in an **interactive session** on Eddie.

You can run either **RStudio Server** (recommended) or **RStudio Desktop** (requires X11 forwarding).

## RStudio Server (Recommended)

```bash
qlogin
source /exports/applications/support/set_qlogin_environment.sh
module load R
module load rstudio
rstudio-server
```

Follow the on-screen instructions and **close your sessions when done**.

## RStudio Desktop

Requires logging into Eddie with X11 forwarding first (see [Matlab](../matlab.md) for X11 setup).

```bash
qlogin
source /exports/applications/support/set_qlogin_environment.sh
module load R
module load rstudio
rstudio
```

> **Important:** The `source /exports/applications/support/set_qlogin_environment.sh` line is essential, especially if you are installing your own R packages.

## Running RStudio Desktop on a Mac

Modern Macs (and some Linux machines with Nvidia drivers) may have rendering issues with X11. Force software rendering by editing `$HOME/.config/RStudio/desktop.ini`:

```ini
[General]
desktop.renderingEngine=software
```

Save the file and restart RStudio.

## Notes

- Use the **module version** of RStudio — do not use the system version.
- If an "update" button appears in RStudio, **do not click it** — it will not work. Contact the IS Helpline if you need a newer version.
