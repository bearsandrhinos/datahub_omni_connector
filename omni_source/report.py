from dataclasses import dataclass

from datahub.ingestion.api.source import SourceReport


@dataclass
class OmniSourceReport(SourceReport):
    connections_scanned: int = 0
    models_scanned: int = 0
    topics_scanned: int = 0
    documents_scanned: int = 0
    dashboards_scanned: int = 0
    semantic_datasets_emitted: int = 0
    physical_datasets_emitted: int = 0
    dataset_lineage_edges_emitted: int = 0
    fine_grained_lineage_edges_exact: int = 0
    fine_grained_lineage_edges_derived: int = 0
    fine_grained_lineage_edges_unresolved: int = 0
