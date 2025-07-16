# Scripts

This directory contains all of the command-line scripts used in the Bluesky AI Analysis project.

Each script can be run manually from the command line, assuming the appropriate virtual environment is activated and required arguments are provided.

## 🧭 Operational Usage

While all scripts are runnable manually, some are run automatically as part of scheduled or persistent services:

- **Systemd Services** (e.g., live ingestion, embedding generation):  
  See [`services/`](../services/) for service definitions and setup instructions.

- **Cron Jobs** (e.g., export to Parquet, index consolidation):  
  See [`crontabs/`](../crontabs/) for scheduling details and setup.

## ✅ Running Manually

Activate the relevant virtual environment, then run the desired script:

```bash
source .venv/bin/activate
python scripts/your_script.py [--options]
```

Some scripts (e.g., store_embeddings.py) require a different environment with Torch:

```bash
source .venv-torch/bin/activate
python scripts/store_embeddings.py
```

📦 Notes
Each script has inline documentation or help text via --help.

Arguments such as database path and output directory are usually configurable.
