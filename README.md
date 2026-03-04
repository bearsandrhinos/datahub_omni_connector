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
- `tests/test_source_behavior.py`: Unit tests for fallback and lineage behavior.

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

## Omni hierarchy emitted by this connector

This connector models Omni objects as a BI lineage graph in DataHub:

1. Omni **Document** with dashboard content
   - Emitted as a native DataHub `Dashboard`
   - Also emitted as a dataset projection for compatibility with dataset-based lineage views
2. Omni **Query Presentation** (workbook tab / dashboard tile)
   - Emitted as a native DataHub `Chart`
3. Omni **Topic**
   - Emitted as Omni dataset (`model_id.topic.topic_name`)
4. Omni **View** inside topic
   - Emitted as Omni dataset (`model_id.view_name`)
5. Physical warehouse table (`database.schema.table`)
   - Emitted as dataset on the mapped platform/dialect from Omni connection metadata

In short:

- `Dashboard -> Chart -> Topic -> View -> Physical Table`

## Lineage behavior

### Coarse lineage

The connector emits dataset-level upstream lineage for:

- Topic upstreams: all views in that topic
- View upstreams: mapped physical tables (`database.schema.table`)
- Dashboard dataset projection upstreams: semantic views, topics, and resolved physical tables
- Chart inputs: topic/view inputs plus resolved physical tables

### Fine-grained (column-level) lineage

When `enable_column_lineage: true`, the connector emits field-level lineage using `UpstreamLineage.fineGrainedLineages`:

- `semantic_view.field -> dashboard_projection.view.field`
- `physical_table.field -> dashboard_projection.view.field`

When `enable_column_lineage: false`, field-level edges are skipped, but coarse physical upstream lineage is still emitted.

### Fallbacks for incomplete Omni metadata

If model YAML does not expose topic names, the connector still attempts lineage hydration from dashboard metadata:

- Uses dashboard/query `topicName` and query field references
- Fetches topic payload directly
- Backfills topic/view/physical-table lineage where possible

If `/v1/connections` is forbidden, ingestion continues with config overrides and defaults instead of hard-failing.

## Warehouse stitching guidance

To stitch Omni lineage to already-ingested warehouse/database assets, table URNs must match exactly:

- `platform`
- `platform_instance` (if used by your upstream source)
- `env`
- dataset name format (`database.schema.table`)

Use these config mappings when needed:

- `connection_to_platform`
- `connection_to_platform_instance`
- `connection_to_database`
- `normalize_snowflake_names` (applies only when platform is `snowflake`)

## Notes

- This is a V1 baseline focused on correctness and extension points.
- It emits semantic and physical datasets, native dashboards/charts, and dataset-level lineage.
- It emits fine-grained lineage through DataHub `upstreamLineage` fine-grained entries when `enable_column_lineage` is set to true.
