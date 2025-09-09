# cptools2 (package)

cptools2 is a lightweight Python package to generate CellProfiler analysis jobs
for HPC clusters. It provides utilities to build image lists, split work into
batchable jobs, generate submission scripts, and join the resulting CSV outputs.

Key features
- Generate loaddata files and CellProfiler commands from YAML configs
- Create space-optimized plate batching for cluster submission
- Create master submission scripts and per-batch command files
- Join chunked CSV result files after analysis (e.g., `Image.csv`, `Cells.csv`)
- Optional hooks for transferring results to external datastores after analysis

Install for development

```bash
cd cptools2
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

Quick example YAML (legacy syntax retained; this project accepts additional
arguments for datastore transfer and batching):

```yaml
experiment : /path/to/imageXpress/experiment
chunk : 46
pipeline : /path/to/cellprofiler/pipeline.cppipe
location : /path/to/scratch/space
commands location : /home/user
# Optional: define post-analysis transfer
transfer:
  provider: s3
  bucket: my-results-bucket
  path: results/{{date}}
# Optional: batching overrides
batching:
  enable: true
  max_batch_size_gb: 200
```

Usage
- Generate a full workflow (creates loaddata, commands and master submit script):

```bash
cptools2 generate config.yml
```

- Join chunked CSV outputs after analysis:

```bash
cptools2 join --location /path/to/location --patterns Image.csv Cells.csv
```

Notes
- The companion `cptools_project_app` repository provides a GUI/web app to
  prepare and submit projects to cptools workflows; see that repo for
  step-by-step instructions to create YAMLs and upload datasets.
