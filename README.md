# DataHub Omni Connector (V1 Scaffold)

This repository contains a V1 scaffold of a custom DataHub ingestion connector for Omni.

## What is included

- `omni_source/config.py`: DataHub source config model.
- `omni_source/report.py`: Source report with lineage counters.
- `omni_source/omni_api.py`: Omni API client with pagination and retries.
- `omni_source/lineage_parser.py`: Field reference parser for column-level lineage.
- `omni_source/source.py`: DataHub source implementation with V1 ingestion flow.
- `recipes/omni_recipe.yml`: Sample ingestion recipe.
- `tests/test_lineage_parser.py`: Unit tests for parser behavior.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Run tests

```bash
pip install pytest
pytest
```

## Sample recipe

Update `recipes/omni_recipe.yml` with your Omni URL and API key, then run:

```bash
datahub ingest -c recipes/omni_recipe.yml
```

## Notes

- This is a V1 baseline focused on correctness and extension points.
- It emits semantic and physical datasets, native dashboards/charts, and dataset-level lineage.
- It emits fine-grained lineage through DataHub `upstreamLineage` fine-grained entries when `enable_column_lineage` is set to true.
