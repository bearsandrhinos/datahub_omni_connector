# DataHub Omni Connector

A DataHub ingestion connector for [Omni](https://omni.co/) — extracts folders, dashboards, charts, semantic models, topics, views, and physical warehouse tables with full upstream lineage.

## Repository layout

```
omni_source/
  config.py           # Pydantic V2 config model (AllowDenyPattern, SecretStr)
  report.py           # SourceReport with lineage and entity counters
  omni_api.py         # Omni REST API client (pagination, rate limiting, retries)
  lineage_parser.py   # Field reference parser for column-level lineage
  source.py           # DataHub source (StatefulIngestionSourceBase + TestableSource)

tests/
  test_lineage_parser.py              # Unit tests: lineage parser
  test_source_behavior.py             # Unit tests: fallback and lineage behavior
  integration/omni/
    fixtures.py                       # Deterministic fake Omni API client
    test_omni_integration.py          # Integration tests + golden file comparison
    omni_mces_golden.json             # Expected output snapshot

docs/sources/omni.md                  # OSS-format documentation page
recipes/omni_recipe.yml               # Sample ingestion recipe
```

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## Configure credentials

Edit `recipes/omni_recipe.yml` and set your values:

```yaml
source:
  type: omni
  config:
    base_url: "https://your-org.omni.co"
    api_key: "your-api-key-here"
```

Or export the key as an environment variable and reference it:

```yaml
api_key: "${OMNI_API_KEY}"
```

## Run ingestion

```bash
datahub ingest -c recipes/omni_recipe.yml
```

## Run tests

```bash
# All tests
pytest

# With coverage
pytest --cov=omni_source --cov-report=term-missing

# Regenerate the golden file after source changes
pytest tests/integration/omni/test_omni_integration.py --update-golden-files
```

## Omni object hierarchy

The connector emits the following five-hop lineage chain:

```
Folder
  └── Dashboard  (native DataHub Dashboard + dataset projection)
        └── Chart  (dashboard tile / workbook query)
              └── Topic  (Omni semantic topic)
                    └── Semantic View  (Omni view with fields)
                          └── Physical Table  (warehouse: Snowflake, BigQuery, Redshift, …)
```

### Entity mapping

| Omni Object | DataHub Type | subType |
|---|---|---|
| Folder | Dataset | `Folder` |
| Dashboard document | Dashboard + Dataset | `Dashboard` |
| Workbook document | Dataset | `Workbook` |
| Query / tile | Chart | — |
| Topic | Dataset | `Topic` |
| Semantic View | Dataset | `View` |
| Warehouse table | Dataset (native platform) | — |

## Metadata coverage

- **Folders**: name, path, scope, owner metadata, URL
- **Dashboards / workbooks**: name, URL, owner, folder, updated timestamp, embed URL
- **Models**: model ID/name, `modelKind` (`SHARED` / `WORKBOOK` / `SCHEMA`), `modelLayer`, `baseModelId`, connection ID
- **Views / topics**: view names, dimension and measure fields, field types, SQL expressions
- **Lineage**: coarse dataset-level + optional fine-grained column-level
- **Ownership**: document owner → Dashboard and Chart entities

## Shared vs workbook model layers

The connector distinguishes Omni model layers via `modelKind` and emits:

- `modelKind` — raw Omni value (`SHARED`, `WORKBOOK`, `SCHEMA`)
- `modelLayer` — normalised (`shared`, `workbook`, `schema`, `branch`, `unknown`)
- `baseModelId` — for workbook models that extend an upstream shared model

## Column-level lineage

When `include_column_lineage: true` (default), the connector emits `FineGrainedLineage` entries:

```
physical_table.column  →  semantic_view.field
```

Set `include_column_lineage: false` to emit only coarse dataset-level lineage.

## Warehouse stitching

To align Omni physical table URNs with those already ingested from your warehouse source:

| Config key | Purpose |
|---|---|
| `connection_to_platform` | Map Omni connection ID → DataHub platform (e.g. `snowflake`) |
| `connection_to_platform_instance` | Map Omni connection ID → platform instance name |
| `connection_to_database` | Override the database name inferred from the connection |
| `normalize_snowflake_names` | Uppercase `database.schema.table` for Snowflake matching |

If `/v1/connections` returns `403 Forbidden`, ingestion continues using config overrides and defaults.

## OSS wiring notes (for DataHub PR)

In the DataHub OSS repository (`metadata-ingestion/setup.py`), add:

```python
# Under extras:
"omni": ["PyYAML>=6.0.1", "requests>=2.31.0"],

# Under entry_points["datahub.ingestion.source"]:
"omni = datahub.ingestion.source.omni.omni:OmniSource",
```

See [`docs/sources/omni.md`](docs/sources/omni.md) for the full OSS documentation page.
