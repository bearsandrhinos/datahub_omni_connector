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

    def list_folders(self, page_size=50):
        return []

    def list_documents(self, page_size=50, include_deleted=False):
        return iter([])


class _FakeClientDocumentOnly:
    def list_connections(self, include_deleted=False):
        return []

    def list_models(self, page_size=50):
        return iter([])

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


class _FakeClientModelRelationships:
    def list_connections(self, include_deleted=False):
        return [{"id": "conn-1", "name": "Warehouse", "dialect": "snowflake", "database": "ANALYTICS"}]

    def list_models(self, page_size=50):
        yield {"id": "model-1", "name": "Workbook model", "connectionId": "conn-1", "baseModelId": "model-0"}

    def get_model_yaml(self, model_id):
        return {"files": {}}

    def list_folders(self, page_size=50):
        return []

    def list_documents(self, page_size=50, include_deleted=False):
        return iter([])


class _FakeClientDocumentRelationships:
    def list_connections(self, include_deleted=False):
        return []

    def list_models(self, page_size=50):
        return iter([])

    def list_documents(self, page_size=50, include_deleted=False):
        yield {
            "identifier": "doc-1",
            "hasDashboard": False,
            "name": "Doc 1",
            "connectionId": "conn-2",
            "folder": {"id": "folder-1", "name": "Finance", "path": "/Finance"},
        }

    def get_document_queries(self, document_id):
        return []

    def get_dashboard_document(self, document_id):
        return {
            "modelId": "model-1",
            "queryPresentations": [
                {
                    "id": "tile-1",
                    "name": "Tile",
                    "topicName": "orders",
                    "query": {"fields": ["orders.amount"]},
                }
            ],
        }

    def list_folders(self, page_size=50):
        return []


class _FakeClientYamlTopicFallback:
    def list_connections(self, include_deleted=False):
        return [{"id": "conn-1", "dialect": "snowflake", "database": "ANALYTICS"}]

    def list_models(self, page_size=50):
        yield {"id": "model-1", "connectionId": "conn-1"}

    def get_model_yaml(self, model_id):
        return {
            "files": {
                "topics/orders.topic": """
type: topic
name: orders
base_view_name: orders
""",
                "views/orders.view": """
type: view
name: orders
schema: PUBLIC
table_name: ORDERS
dimensions:
  - name: order_id
    sql_type: STRING
measures:
  count:
    sql_type: NUMBER
""",
            }
        }

    def get_topic(self, model_id, topic_name):
        raise RuntimeError("403 forbidden")

    def list_folders(self, page_size=50):
        return []

    def list_documents(self, page_size=50, include_deleted=False):
        return iter([])


class _FakeClientDashboardTopicOnly:
    def list_connections(self, include_deleted=False):
        return []

    def list_models(self, page_size=50):
        return iter([])

    def list_documents(self, page_size=50, include_deleted=False):
        yield {"identifier": "doc-1", "hasDashboard": True, "name": "Doc 1"}

    def get_dashboard_document(self, document_id):
        return {
            "modelId": "model-1",
            "queryPresentations": [
                {
                    "id": "tile-1",
                    "name": "Tile",
                    "topicName": "orders",
                    "query": {"fields": ["orders.amount"]},
                }
            ],
        }

    def get_document_queries(self, document_id):
        return []

    def get_topic(self, model_id, topic_name):
        return {
            "views": [
                {
                    "name": "orders",
                    "dimensions": [{"field_name": "amount", "sql_type": "NUMBER"}],
                    "measures": [],
                }
            ]
        }

    def list_folders(self, page_size=50):
        return []


class _FakeClientInferredTopicWithQueryFields:
    def list_connections(self, include_deleted=False):
        return []

    def list_models(self, page_size=50):
        return iter([])

    def list_documents(self, page_size=50, include_deleted=False):
        yield {"identifier": "doc-1", "hasDashboard": True, "name": "Doc 1"}

    def get_dashboard_document(self, document_id):
        return {
            "modelId": "model-1",
            "queryPresentations": [
                {
                    "id": "tile-1",
                    "name": "Tile",
                    "topicName": "orders",
                    "query": {"fields": ["orders.amount"]},
                }
            ],
        }

    def get_document_queries(self, document_id):
        return []

    def get_topic(self, model_id, topic_name):
        raise RuntimeError("404 not found")

    def list_folders(self, page_size=50):
        return []


def _build_source(enable_column_lineage: bool = True) -> OmniSource:
    config = OmniSourceConfig.model_validate(
        {
            "base_url": "https://example.omniapp.co/api",
            "api_key": "test-key",
            "include_workbook_only": True,
            "include_column_lineage": enable_column_lineage,
        }
    )
    return OmniSource(config, PipelineContext(run_id="test"))


def test_model_context_is_cached_when_model_yaml_fails() -> None:
    source = _build_source()
    source.client = _FakeClientModelYamlFailure()

    list(source._ingest_semantic_model())

    assert source._model_context_by_id["model-1"]["connection_id"] == "conn-1"
    assert source._model_context_by_id["model-1"]["platform"] == "snowflake"
    assert source._model_context_by_id["model-1"]["database"] == "ANALYTICS"


def test_column_lineage_disabled_does_not_attach_physical_to_dashboard() -> None:
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

    if lineage_workunits:
        upstreams = {u.dataset for u in lineage_workunits[0].aspect.upstreams}
        assert physical_urn not in upstreams


def test_model_emits_connection_and_base_model_relationships() -> None:
    source = _build_source()
    source.client = _FakeClientModelRelationships()

    lineage_workunits = []
    for wu in source._ingest_semantic_model():
        mcp = wu.metadata
        if getattr(mcp, "entityUrn", None) == source._model_dataset_urn("model-1"):
            if getattr(mcp, "aspectName", None) == "upstreamLineage":
                lineage_workunits.append(mcp)

    assert lineage_workunits
    upstreams = {u.dataset for u in lineage_workunits[0].aspect.upstreams}
    assert source._connection_dataset_urn("conn-1") in upstreams
    assert source._model_dataset_urn("model-0") in upstreams


def test_document_emits_folder_upstream_and_connection_downstream() -> None:
    source = _build_source(enable_column_lineage=True)
    source.client = _FakeClientDocumentRelationships()

    lineage_workunits = []
    dashboard_urn = "urn:li:dataset:(urn:li:dataPlatform:omni,doc-1,PROD)"
    for wu in source._ingest_documents():
        mcp = wu.metadata
        if getattr(mcp, "entityUrn", None) == dashboard_urn:
            if getattr(mcp, "aspectName", None) == "upstreamLineage":
                lineage_workunits.append(mcp)

    assert lineage_workunits
    upstreams = {u.dataset for u in lineage_workunits[0].aspect.upstreams}
    assert source._folder_dataset_urn("folder-1") in upstreams


def test_model_yaml_fallback_emits_view_columns_when_topic_fetch_fails() -> None:
    source = _build_source()
    source.client = _FakeClientYamlTopicFallback()

    schema_workunits = []
    for wu in source._ingest_semantic_model():
        mcp = wu.metadata
        if getattr(mcp, "entityUrn", None) == source._semantic_dataset_urn("model-1", "orders"):
            if getattr(mcp, "aspectName", None) == "schemaMetadata":
                schema_workunits.append(mcp)

    assert schema_workunits
    fields = {f.fieldPath for f in schema_workunits[0].aspect.fields}
    assert "order_id" in fields
    assert "count" in fields


def test_topic_view_physical_hierarchy_direction() -> None:
    source = _build_source(enable_column_lineage=True)
    topic_urn = source._topic_dataset_urn("model-1", "orders")
    view_urn = source._semantic_dataset_urn("model-1", "orders_view")
    physical_urn = source._physical_dataset_urn("snowflake", "ANALYTICS", "PUBLIC", "ORDERS")

    lineage_by_entity = {}
    for wu in source._ingest_topic_payload(
        model_id="model-1",
        topic_name="orders",
        topic={
            "views": [
                {
                    "name": "orders_view",
                    "schema": "PUBLIC",
                    "table_name": "ORDERS",
                    "dimensions": [{"field_name": "order_id", "sql_type": "NUMBER"}],
                    "measures": [],
                }
            ]
        },
        platform="snowflake",
        database="ANALYTICS",
        connection_id="conn-1",
        platform_instance=None,
    ):
        mcp = wu.metadata
        if getattr(mcp, "aspectName", None) == "upstreamLineage":
            lineage_by_entity[getattr(mcp, "entityUrn", "")] = mcp.aspect

    assert topic_urn in lineage_by_entity
    topic_upstreams = {u.dataset for u in lineage_by_entity[topic_urn].upstreams}
    assert not topic_upstreams

    assert view_urn in lineage_by_entity
    view_upstreams = {u.dataset for u in lineage_by_entity[view_urn].upstreams}
    assert topic_urn in view_upstreams

    assert physical_urn in lineage_by_entity
    physical_upstreams = {u.dataset for u in lineage_by_entity[physical_urn].upstreams}
    assert view_urn in physical_upstreams


def test_dashboard_fine_grained_lineage_does_not_directly_reference_physical_fields() -> None:
    source = _build_source()
    source.client = _FakeClientDocumentOnly()

    physical_urn = "urn:li:dataset:(urn:li:dataPlatform:snowflake,ANALYTICS.PUBLIC.ORDERS,PROD)"
    source._semantic_fields["model-1:orders.amount"] = SemanticField(
        model_id="model-1",
        view_name="orders",
        field_name="amount",
        upstream_physical_urns={physical_urn},
    )

    fine_grained_upstreams = []
    for wu in source._ingest_documents():
        mcp = wu.metadata
        if getattr(mcp, "aspectName", None) != "upstreamLineage":
            continue
        for edge in (mcp.aspect.fineGrainedLineages or []):
            fine_grained_upstreams.extend(edge.upstreams or [])

    assert fine_grained_upstreams
    assert all("dataPlatform:snowflake" not in urn for urn in fine_grained_upstreams)


def test_dashboard_does_not_have_direct_topic_upstream() -> None:
    source = _build_source(enable_column_lineage=False)
    source.client = _FakeClientDashboardTopicOnly()

    dashboard_upstreams = set()
    for wu in source._ingest_documents():
        mcp = wu.metadata
        if (
            getattr(mcp, "entityUrn", None) == "urn:li:dataset:(urn:li:dataPlatform:omni,doc-1,PROD)"
            and getattr(mcp, "aspectName", None) == "upstreamLineage"
        ):
            dashboard_upstreams = {u.dataset for u in mcp.aspect.upstreams}

    assert all(".topic." not in urn for urn in dashboard_upstreams)


def test_inferred_view_links_to_inferred_topic() -> None:
    source = _build_source(enable_column_lineage=False)
    source.client = _FakeClientInferredTopicWithQueryFields()

    inferred_view_urn = source._semantic_dataset_urn("model-1", "orders")
    inferred_topic_urn = source._topic_dataset_urn("model-1", "orders")
    lineage_upstreams = set()
    for wu in source._ingest_documents():
        mcp = wu.metadata
        if (
            getattr(mcp, "entityUrn", None) == inferred_view_urn
            and getattr(mcp, "aspectName", None) == "upstreamLineage"
        ):
            lineage_upstreams = {u.dataset for u in mcp.aspect.upstreams}

    assert inferred_topic_urn in lineage_upstreams


def test_chart_inputs_include_topic_urn() -> None:
    source = _build_source(enable_column_lineage=False)
    source.client = _FakeClientDashboardTopicOnly()

    saw_chart = False
    for wu in source._ingest_documents():
        mcp = wu.metadata
        if getattr(mcp, "aspectName", None) != "chartInfo":
            continue
        saw_chart = True
        inputs = set(getattr(mcp.aspect, "inputs", []) or [])
        assert any(".topic." in urn for urn in inputs)
    assert saw_chart
