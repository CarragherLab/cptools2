# AlphaGenome API

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/AlphaGenome+API>

The AlphaGenome API is available on Eddie as a Conda environment in `igmm/apps/miniforge`.

- **GitHub:** <https://github.com/google-deepmind/alphagenome>

## Loading the Environment

```bash
module load igmm/apps/miniforge/24.3.0
eval "$(conda shell.bash hook)"
conda activate alphagenome
```

## Notes

Unlike AlphaFold, AlphaGenome is currently only available as a **client-side API** — it connects to a remote AlphaGenome server. Details can be found in the GitHub repository linked above.
