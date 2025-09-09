# Large datasets (NOT tracked in git)

This project uses large image datasets for integration tests and real runs. Do NOT store
these datasets in the repository. Instead, place them in an external storage location and
set the path in your local `.env` or configuration file.

Suggested location (example):
`/mnt/data/cptools_datasets/`

Update your `.env`:
`DATASETS_PATH=/mnt/data/cptools_datasets`

For CI, store required subsets in an artifact store or use environment variables to fetch data.
