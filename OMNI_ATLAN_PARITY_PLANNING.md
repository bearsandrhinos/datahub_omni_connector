# Omni -> DataHub Parity Plan

Created: 2026-03-05  
Status: IN_PROGRESS

## 1) Source Classification

- Category: **BI tool**
- Source type: **API**
- Similar connector pattern: Tableau / Looker style metadata graph

This matches the planning-skill classification for Omni.

## 2) Research Summary (Atlan extractor baseline)

Reference implementation reviewed: [bearsandrhinos/omni-atlan-metadata](https://github.com/bearsandrhinos/omni-atlan-metadata)

Observed extraction in that repo:

- Connections (`id`, `name`, `dialect`, `database`)
- Models (`id`, `name`, `modelKind`, `connectionId`, `baseModelId`, `updatedAt`)
- Topics (from model YAML `.topic` files)
- Folders (`id`, `name`, `path`, `scope`, `owner`)
- Documents/dashboards (`identifier`, `name`, `scope`, `url`, `updatedAt`, `connectionId`, folder + owner)

Reference metadata target set:

- [Secoda Omni metadata extracted](https://docs.secoda.co/integrations/data-visualization-tools/omni/omni-metadata-extracted)
- [Omni model layers (shared/workbook/schema)](https://docs.omni.co/modeling)

## 3) Current DataHub Connector Coverage vs Atlan Baseline

### Already covered

- Models, topics, folders, documents/dashboards
- Dashboard/chart entities and lineage
- View-level schema + lineage to physical tables
- Shared/workbook distinction via `modelKind` / normalized `modelLayer`

### Missing or weaker vs Atlan-style parity

1. No first-class **connection entities** in DataHub graph
2. Limited explicit relationship modeling for:
   - model -> connection
   - model -> base model
   - document -> folder / connection
3. Labels/tags are not consistently extracted (depends on list endpoint payload shape)
4. Document/folder enrichment fields vary by endpoint permissions and includes

## 4) Implementation Plan

### Phase A: Parity-critical metadata entities

1. Add logical Omni **connection entities** (dataset projection) with:
   - connection id, name, dialect, database, scope/deleted flags if present
2. Emit connection dataset properties before models/documents.

### Phase B: Relationship parity

3. Add coarse lineage relationships:
   - model -> connection
   - model -> base model (when available)
   - document -> folder
   - document -> connection
4. Keep existing dashboard/chart/topic/view lineage unchanged.

### Phase C: Optional enrichments (if API payload exposes fields)

5. Update folder/document list calls to request richer includes where supported.
6. Map labels to custom properties and optionally `globalTags` aspect.
7. Add ownership aspect emission (where owner data exists) for model/document/folder projections.

### Phase D: Validation

8. Unit tests for:
   - connection entity emission
   - relationship edges
   - modelKind/modelLayer propagation
9. Local ingest verification:
   - connection/model/document parity checks
   - columns + lineage regression checks

## 5) Scope Clarification Needed

Before implementing, choose target scope:

- **Option A (strict Atlan parity):** only match Atlan extractor fields/relationships.
- **Option B (parity+):** keep Atlan parity and retain/extend DataHub-native extras (dashboards/charts/fine-grained lineage).

Recommended: **Option B**.

## 6) Expected Outcome

After implementation, the DataHub connector will match the Atlan extractor metadata coverage for Omni core objects (connections, models, topics, folders, documents) while preserving DataHub-native lineage depth.
