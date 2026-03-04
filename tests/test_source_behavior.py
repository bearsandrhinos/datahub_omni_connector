from omni_source.config import OmniSourceConfig
from omni_source.source import OmniSource, SemanticField
from datahub.ingestion.api.common import PipelineContext


class _FakeClientModelYamlFailure:
    def list_connections(self, include_deleted=False):
        return [
            {
                "id": "conn-1",
                "dialect": "snowflake",
                "database": "ANALYTICS",
            }
        ]

    def list_models(self, page_size=50):
        yield {"id": "model-1", "connectionId": "conn-1"}

    def get_model_yaml(self, model_id):
        raise RuntimeError("boom")


class _FakeClientDocumentOnly:
    def list_documents(self, page_size=50, include_deleted=False):
        yield {"identifier": "doc-1", "hasDashboard": False, "name": "Doc 1"}

    def get_document_queries(self, document_id):
        return [
            {
                "query": {
                    "modelId": "model-1",
                    "fields": ["orders.amount"],
                }
            }
        ]

    def get_dashboard_document(self, document_id):
        return {}

    def list_folders(self, page_size=50):
        return []


def _build_source(enable_column_lineage: bool) -> OmniSource:
    config = OmniSourceConfig.model_validate(
        {
            "base_url": "https://example.omniapp.co/api",
            "api_key": "test-key",
            "include_workbook_only": True,
            "enable_column_lineage": enable_column_lineage,
        }
    )
    return OmniSource(config, PipelineContext(run_id="test"))


def test_model_context_is_cached_when_model_yaml_fails() -> None:
    source = _build_source(enable_column_lineage=True)
    source.client = _FakeClientModelYamlFailure()

    list(source._ingest_semantic_model())

    assert source._model_context_by_id["model-1"]["connection_id"] == "conn-1"
    assert source._model_context_by_id["model-1"]["platform"] == "snowflake"
    assert source._model_context_by_id["model-1"]["database"] == "ANALYTICS"


def test_column_lineage_disabled_still_emits_physical_upstream() -> None:
    source = _build_source(enable_column_lineage=False)
    source.client = _FakeClientDocumentOnly()

    physical_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,ANALYTICS.PUBLIC.ORDERS,PROD)"
    source._semantic_fields["model-1:orders.amount"] = SemanticField(
        model_id="model-1",
        view_name="orders",
        field_name="amount",
        upstream_physical_urns={physical_urn},
    )

    lineage_workunits = []
    for wu in source._ingest_documents():
        mcp = wu.metadata
        if getattr(mcp, "aspectName", None) == "upstreamLineage":
            lineage_workunits.append(mcp)

    assert lineage_workunits
    upstreams = {u.dataset for u in lineage_workunits[0].aspect.upstreams}
    assert physical_urn in upstreams
