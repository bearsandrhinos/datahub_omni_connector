from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Iterable, Iterator, List, Optional, Set

import yaml

from datahub.emitter.mce_builder import (
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.emitter.mce_builder import make_chart_urn, make_dashboard_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeAuditStampsClass,
    ChartInfoClass,
    ChartTypeClass,
    DataPlatformInfoClass,
    DashboardInfoClass,
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    PlatformTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

from omni_source.config import OmniSourceConfig
from omni_source.lineage_parser import FieldRef, extract_field_refs, parse_field_list
from omni_source.omni_api import OmniAPIClient
from omni_source.report import OmniSourceReport


@dataclass
class SemanticField:
    model_id: str
    view_name: str
    field_name: str
    expression: str = ""
    confidence: str = "unresolved"
    upstream_physical_urns: Set[str] = field(default_factory=set)


@platform_name("Omni")
@support_status(SupportStatus.INCUBATING)
@config_class(OmniSourceConfig)
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.LINEAGE_FINE, "Prepared in V1 with extensible mappings")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
class OmniSource(Source):
    DEFAULT_LOGO_URL = "https://avatars.githubusercontent.com/u/100505341?s=200&v=4"

    def __init__(self, config: OmniSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = OmniSourceReport()
        self.client = OmniAPIClient(
            base_url=config.base_url,
            api_key=config.api_key,
            timeout_seconds=config.timeout_seconds,
            max_requests_per_minute=config.max_requests_per_minute,
        )
        self._semantic_fields: Dict[str, SemanticField] = {}
        self._semantic_dataset_urns: Set[str] = set()
        self._topic_dataset_urns: Set[str] = set()
        self._topic_urn_by_key: Dict[str, str] = {}
        self._physical_dataset_urns: Set[str] = set()
        self._model_context_by_id: Dict[str, Dict[str, Optional[str]]] = {}

    @classmethod
    def create(cls, config_dict, ctx):
        config = OmniSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_report(self) -> OmniSourceReport:
        return self.report

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return self.get_workunits_internal()

    def _as_workunit(self, mcp: MetadataChangeProposalWrapper) -> MetadataWorkUnit:
        return mcp.as_workunit()

    def _emit_dataset_properties(
        self,
        dataset_urn: str,
        name: str,
        description: str,
        custom_properties: Dict[str, str],
        external_url: Optional[str] = None,
    ) -> Iterator[MetadataWorkUnit]:
        props = DatasetPropertiesClass(
            name=name,
            description=description,
            customProperties=custom_properties,
            externalUrl=external_url,
        )
        yield self._as_workunit(
            MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=props)
        )

    def _emit_platform_metadata(
        self, platform_name: str, display_name: str
    ) -> Iterator[MetadataWorkUnit]:
        platform_info = DataPlatformInfoClass(
            name=platform_name,
            type=PlatformTypeClass.OTHERS,
            datasetNameDelimiter=".",
            displayName=display_name,
            logoUrl=self.DEFAULT_LOGO_URL,
        )
        yield self._as_workunit(
            MetadataChangeProposalWrapper(
                entityUrn=make_data_platform_urn(platform_name),
                aspect=platform_info,
            )
        )

    def _emit_upstream_lineage(
        self,
        dataset_urn: str,
        upstreams: Set[str],
        fine_grained_lineages: Optional[List[FineGrainedLineageClass]] = None,
    ) -> Iterator[MetadataWorkUnit]:
        if not upstreams and not fine_grained_lineages:
            return
        lineage = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(dataset=upstream_urn, type=DatasetLineageTypeClass.TRANSFORMED)
                for upstream_urn in sorted(upstreams)
            ],
            fineGrainedLineages=fine_grained_lineages or None,
        )
        self.report.dataset_lineage_edges_emitted += len(upstreams)
        yield self._as_workunit(
            MetadataChangeProposalWrapper(entityUrn=dataset_urn, aspect=lineage)
        )

    def _topic_names_from_yaml(self, model_yaml: Dict[str, str]) -> Set[str]:
        names: Set[str] = set()
        for _, file_text in model_yaml.items():
            try:
                parsed = yaml.safe_load(file_text) or {}
            except Exception:
                continue
            if not isinstance(parsed, dict):
                continue
            if parsed.get("type") == "topic" and parsed.get("name"):
                names.add(parsed["name"])
        return names

    def _canonical_semantic_field_key(self, model_id: str, view_name: str, field_name: str) -> str:
        return f"{model_id}:{view_name}.{field_name}"

    def _semantic_dataset_urn(self, model_id: str, view_name: str) -> str:
        return make_dataset_urn("omni", f"{model_id}.{view_name}", self.config.env)

    def _physical_dataset_urn(
        self,
        platform: str,
        database: str,
        schema: str,
        table: str,
        platform_instance: Optional[str] = None,
    ) -> str:
        db_name = database or ""
        schema_name = schema or ""
        table_name = table or ""
        if self.config.normalize_snowflake_names and platform.lower() == "snowflake":
            db_name = db_name.upper()
            schema_name = schema_name.upper()
            table_name = table_name.upper()
        full_name = ".".join([part for part in [db_name, schema_name, table_name] if part])
        if platform_instance:
            return make_dataset_urn_with_platform_instance(
                platform=platform,
                name=full_name,
                platform_instance=platform_instance,
                env=self.config.env,
            )
        return make_dataset_urn(platform, full_name, self.config.env)

    def _topic_dataset_urn(self, model_id: str, topic_name: str) -> str:
        return make_dataset_urn("omni", f"{model_id}.topic.{topic_name}", self.config.env)

    def _default_audit_stamps(self, updated_at: Optional[str] = None) -> ChangeAuditStampsClass:
        now_ms = int(datetime.utcnow().timestamp() * 1000)
        event_ms = now_ms
        if updated_at:
            try:
                event_ms = int(
                    datetime.fromisoformat(updated_at.replace("Z", "+00:00")).timestamp() * 1000
                )
            except Exception:
                event_ms = now_ms
        actor = make_user_urn("omni_ingestion")
        stamp = AuditStampClass(time=event_ms, actor=actor)
        return ChangeAuditStampsClass(created=stamp, lastModified=stamp)

    def _ingest_topic_payload(
        self,
        model_id: str,
        topic_name: str,
        topic: Dict[str, object],
        platform: str,
        database: str,
        connection_id: str,
        platform_instance: Optional[str],
        inferred: bool = False,
    ) -> Iterator[MetadataWorkUnit]:
        if not topic:
            return
        self.report.topics_scanned += 1
        topic_urn = self._topic_dataset_urn(model_id, topic_name)
        self._topic_dataset_urns.add(topic_urn)
        self._topic_urn_by_key[f"{model_id}:{topic_name}"] = topic_urn
        yield from self._emit_dataset_properties(
            dataset_urn=topic_urn,
            name=topic_name,
            description="Omni topic entity.",
            custom_properties={
                "modelId": model_id,
                "topicName": topic_name,
                "entityType": "topic",
                "inferred": "true" if inferred else "false",
            },
        )
        self.report.semantic_datasets_emitted += 1
        topic_view_upstreams: Set[str] = set()
        for view in topic.get("views", []):  # type: ignore[union-attr]
            view_name = (view or {}).get("name") if isinstance(view, dict) else None
            if not view_name:
                continue
            semantic_urn = self._semantic_dataset_urn(model_id, view_name)
            self._semantic_dataset_urns.add(semantic_urn)
            topic_view_upstreams.add(semantic_urn)
            yield from self._emit_dataset_properties(
                dataset_urn=semantic_urn,
                name=view_name,
                description=f"Omni semantic view from topic {topic_name}.",
                custom_properties={
                    "modelId": model_id,
                    "topicName": topic_name,
                    "viewName": view_name,
                },
            )
            self.report.semantic_datasets_emitted += 1

            schema = view.get("schema") or ""
            table = view.get("table_name") or ""
            if table:
                physical_urn = self._physical_dataset_urn(
                    platform, database, schema, table, platform_instance
                )
                self._physical_dataset_urns.add(physical_urn)
                yield from self._emit_dataset_properties(
                    dataset_urn=physical_urn,
                    name=table,
                    description="Physical source table referenced by Omni model.",
                    custom_properties={
                        "platform": platform,
                        "database": database,
                        "schema": schema,
                        "table": table,
                        "connectionId": connection_id,
                        "platformInstance": platform_instance or "",
                    },
                )
                self.report.physical_datasets_emitted += 1
                yield from self._emit_upstream_lineage(semantic_urn, {physical_urn})

            for dimension in view.get("dimensions", []):
                field_name = dimension.get("field_name")
                if not field_name:
                    continue
                key = self._canonical_semantic_field_key(model_id, view_name, field_name)
                field_info = SemanticField(
                    model_id=model_id,
                    view_name=view_name,
                    field_name=field_name,
                    expression=dimension.get("dialect_sql") or dimension.get("display_sql") or "",
                    confidence="unresolved",
                )
                if table:
                    field_info.upstream_physical_urns.add(physical_urn)
                self._semantic_fields[key] = field_info

            for measure in view.get("measures", []):
                field_name = measure.get("field_name")
                if not field_name:
                    continue
                key = self._canonical_semantic_field_key(model_id, view_name, field_name)
                expression = measure.get("dialect_sql") or measure.get("display_sql") or ""
                refs = extract_field_refs(expression)
                confidence = "exact" if refs else "derived" if expression else "unresolved"
                field_info = SemanticField(
                    model_id=model_id,
                    view_name=view_name,
                    field_name=field_name,
                    expression=expression,
                    confidence=confidence,
                )
                if table:
                    field_info.upstream_physical_urns.add(physical_urn)
                self._semantic_fields[key] = field_info

        if topic_view_upstreams:
            yield from self._emit_upstream_lineage(topic_urn, topic_view_upstreams)

    def _emit_dashboard_info(
        self,
        dashboard_urn: str,
        title: str,
        description: str,
        external_url: str,
        chart_urns: List[str],
        dataset_urns: List[str],
        custom_properties: Dict[str, str],
        updated_at: Optional[str] = None,
    ) -> Iterator[MetadataWorkUnit]:
        info = DashboardInfoClass(
            title=title,
            description=description,
            lastModified=self._default_audit_stamps(updated_at),
            externalUrl=external_url,
            dashboardUrl=external_url,
            charts=chart_urns or None,
            datasets=dataset_urns or None,
            customProperties=custom_properties,
        )
        yield self._as_workunit(MetadataChangeProposalWrapper(entityUrn=dashboard_urn, aspect=info))

    def _emit_chart_info(
        self,
        chart_urn: str,
        title: str,
        description: str,
        external_url: str,
        input_urns: List[str],
        custom_properties: Dict[str, str],
        updated_at: Optional[str] = None,
    ) -> Iterator[MetadataWorkUnit]:
        info = ChartInfoClass(
            title=title,
            description=description,
            lastModified=self._default_audit_stamps(updated_at),
            externalUrl=external_url,
            chartUrl=external_url,
            inputs=input_urns or None,
            customProperties=custom_properties,
            type=ChartTypeClass.TABLE,
        )
        yield self._as_workunit(MetadataChangeProposalWrapper(entityUrn=chart_urn, aspect=info))

    def _ingest_semantic_model(self) -> Iterator[MetadataWorkUnit]:
        connections = {c.get("id"): c for c in self.client.list_connections(self.config.include_deleted)}
        for model in self.client.list_models(page_size=self.config.page_size):
            model_id = model.get("id")
            if not model_id:
                continue
            if self.config.model_allowlist and model_id not in self.config.model_allowlist:
                continue
            self.report.models_scanned += 1

            try:
                model_yaml_payload = self.client.get_model_yaml(model_id)
            except Exception as exc:
                self.report.report_warning(
                    "model-yaml-fetch",
                    f"Failed to fetch model YAML for {model_id}: {exc}",
                )
                continue
            connection_id = model.get("connectionId") or ""
            connection = connections.get(connection_id)
            platform = (connection or {}).get("dialect") or "database"
            if self.config.connection_to_platform:
                platform = self.config.connection_to_platform.get(connection_id, platform)
            database = (connection or {}).get("database") or ""
            if self.config.connection_to_database:
                database = self.config.connection_to_database.get(connection_id, database)
            platform_instance: Optional[str] = None
            if self.config.connection_to_platform_instance:
                platform_instance = self.config.connection_to_platform_instance.get(connection_id)
            self._model_context_by_id[model_id] = {
                "connection_id": connection_id,
                "platform": platform,
                "database": database,
                "platform_instance": platform_instance,
            }

            topic_names = self._topic_names_from_yaml(model_yaml_payload.get("files", {}))
            if not topic_names:
                continue

            for topic_name in sorted(topic_names):
                try:
                    topic = self.client.get_topic(model_id, topic_name)
                except Exception as exc:
                    self.report.report_warning(
                        "topic-fetch",
                        f"Failed to fetch topic {topic_name} for model {model_id}: {exc}",
                    )
                    continue
                if not topic:
                    continue
                yield from self._ingest_topic_payload(
                    model_id=model_id,
                    topic_name=topic_name,
                    topic=topic,
                    platform=platform,
                    database=database,
                    connection_id=connection_id,
                    platform_instance=platform_instance,
                )

    def _ingest_documents(self) -> Iterator[MetadataWorkUnit]:
        for document in self.client.list_documents(
            page_size=self.config.page_size, include_deleted=self.config.include_deleted
        ):
            doc_id = document.get("identifier")
            if not doc_id:
                continue
            if self.config.document_allowlist and doc_id not in self.config.document_allowlist:
                continue
            self.report.documents_scanned += 1

            has_dashboard = bool(document.get("hasDashboard"))
            if not has_dashboard and not self.config.include_workbook_only:
                continue

            dashboard_dataset_urn = make_dataset_urn("omni", doc_id, self.config.env)
            dashboard_urn = make_dashboard_urn("omni", doc_id)
            dashboard_url = (
                document.get("url")
                or f"{self.config.base_url.removesuffix('/api')}/dashboards/{doc_id}"
            )
            embed_url = dashboard_url
            dashboard_title = document.get("name") or doc_id

            # Keep dataset projection for compatibility with existing views.
            yield from self._emit_dataset_properties(
                dataset_urn=dashboard_dataset_urn,
                name=dashboard_title,
                description="Omni dashboard/workbook document represented as logical BI asset.",
                custom_properties={
                    "documentId": doc_id,
                    "url": dashboard_url,
                    "ownerName": ((document.get("owner") or {}).get("name") or ""),
                    "omniEmbedUrl": embed_url,
                    "omniEmbedIframe": f'<iframe src="{embed_url}"></iframe>',
                },
                external_url=dashboard_url,
            )
            self.report.dashboards_scanned += 1

            upstream_semantic_urns: Set[str] = set()
            upstream_physical_urns: Set[str] = set()
            fields_by_dashboard: Set[FieldRef] = set()
            fine_grained_lineages: List[FineGrainedLineageClass] = []
            fine_grained_dedupe: Set[tuple] = set()
            chart_urns: List[str] = []
            dashboard_topics: Set[str] = set()
            model_id_from_dashboard: Optional[str] = None
            chart_inputs: Dict[str, Set[str]] = {}
            chart_titles: Dict[str, str] = {}
            chart_urls: Dict[str, str] = {}

            if has_dashboard:
                try:
                    dashboard = self.client.get_dashboard_document(doc_id)
                    model_id_from_dashboard = dashboard.get("modelId")
                    query_presentations = dashboard.get("queryPresentations", [])
                    for idx, qp in enumerate(query_presentations):
                        qp_id = qp.get("id") or f"{doc_id}:{idx}"
                        chart_urn = make_chart_urn("omni", qp_id)
                        chart_urns.append(chart_urn)
                        chart_inputs[chart_urn] = set()
                        chart_titles[chart_urn] = qp.get("name") or f"{dashboard_title} - tile {idx + 1}"

                        query = qp.get("query") or {}
                        fields_by_dashboard.update(parse_field_list(query.get("fields", [])))
                        topic_name = qp.get("topicName") or query.get("join_paths_from_topic_name")
                        if topic_name:
                            dashboard_topics.add(topic_name)
                            if model_id_from_dashboard:
                                topic_key = f"{model_id_from_dashboard}:{topic_name}"
                                topic_urn = self._topic_urn_by_key.get(
                                    topic_key, self._topic_dataset_urn(model_id_from_dashboard, topic_name)
                                )
                                if topic_key not in self._topic_urn_by_key:
                                    context = self._model_context_by_id.get(model_id_from_dashboard, {})
                                    try:
                                        topic_payload = self.client.get_topic(model_id_from_dashboard, topic_name)
                                        if topic_payload:
                                            yield from self._ingest_topic_payload(
                                                model_id=model_id_from_dashboard,
                                                topic_name=topic_name,
                                                topic=topic_payload,
                                                platform=str(context.get("platform") or "database"),
                                                database=str(context.get("database") or ""),
                                                connection_id=str(context.get("connection_id") or ""),
                                                platform_instance=(
                                                    str(context.get("platform_instance"))
                                                    if context.get("platform_instance")
                                                    else None
                                                ),
                                                inferred=True,
                                            )
                                            topic_urn = self._topic_urn_by_key.get(topic_key, topic_urn)
                                    except Exception as exc:
                                        self.report.report_warning(
                                            "topic-fetch-from-dashboard",
                                            f"Failed to fetch topic {topic_name} for model {model_id_from_dashboard}: {exc}",
                                        )
                                if topic_urn not in self._topic_dataset_urns:
                                    self._topic_dataset_urns.add(topic_urn)
                                    yield from self._emit_dataset_properties(
                                        dataset_urn=topic_urn,
                                        name=topic_name,
                                        description="Omni topic inferred from dashboard metadata.",
                                        custom_properties={
                                            "modelId": model_id_from_dashboard,
                                            "topicName": topic_name,
                                            "inferred": "true",
                                        },
                                    )
                                    self.report.semantic_datasets_emitted += 1
                                chart_inputs[chart_urn].add(topic_urn)
                                upstream_semantic_urns.add(topic_urn)

                        chart_url = f"{dashboard_url}?queryPresentationId={qp_id}"
                        chart_urls[chart_urn] = chart_url
                except Exception as exc:
                    self.report.report_warning(
                        "dashboard-document-fetch",
                        f"Failed to fetch dashboard payload for {doc_id}: {exc}",
                    )

            try:
                for query in self.client.get_document_queries(doc_id):
                    if not model_id_from_dashboard:
                        model_id_from_dashboard = ((query.get("query") or {}).get("modelId"))
                    fields_by_dashboard.update(
                        parse_field_list((query.get("query") or {}).get("fields", []))
                    )
            except Exception as exc:
                self.report.report_warning(
                    "document-queries-fetch",
                    f"Failed to fetch queries for {doc_id}: {exc}",
                )

            for field_ref in fields_by_dashboard:
                if not model_id_from_dashboard:
                    continue
                # Fallback: always connect dashboard to semantic view datasets inferred from query fields.
                semantic_view_urn = self._semantic_dataset_urn(model_id_from_dashboard, field_ref.view)
                upstream_semantic_urns.add(semantic_view_urn)
                for chart_urn in chart_urns:
                    chart_inputs.setdefault(chart_urn, set()).add(semantic_view_urn)
                if semantic_view_urn not in self._semantic_dataset_urns:
                    self._semantic_dataset_urns.add(semantic_view_urn)
                    yield from self._emit_dataset_properties(
                        dataset_urn=semantic_view_urn,
                        name=field_ref.view,
                        description="Omni semantic view inferred from dashboard query fields.",
                        custom_properties={
                            "modelId": model_id_from_dashboard,
                            "viewName": field_ref.view,
                            "inferred": "true",
                        },
                    )
                    self.report.semantic_datasets_emitted += 1

                downstream_field_urn = make_schema_field_urn(
                    dashboard_dataset_urn, f"{field_ref.view}.{field_ref.field}"
                )
                key = self._canonical_semantic_field_key(
                    model_id_from_dashboard, field_ref.view, field_ref.field
                )
                semantic_field = self._semantic_fields.get(key)
                if not semantic_field:
                    self.report.fine_grained_lineage_edges_unresolved += 1
                    continue
                semantic_urn = self._semantic_dataset_urn(
                    semantic_field.model_id, semantic_field.view_name
                )
                upstream_semantic_urns.add(semantic_urn)
                semantic_field_urn = make_schema_field_urn(semantic_urn, semantic_field.field_name)
                semantic_edge = (
                    tuple([semantic_field_urn]),
                    tuple([downstream_field_urn]),
                )
                if semantic_edge not in fine_grained_dedupe:
                    fine_grained_dedupe.add(semantic_edge)
                    fine_grained_lineages.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            upstreams=[semantic_field_urn],
                            downstreams=[downstream_field_urn],
                            transformOperation="OMNI_QUERY_FIELD_MAPPING",
                        )
                    )
                if semantic_field.confidence == "exact":
                    self.report.fine_grained_lineage_edges_exact += 1
                elif semantic_field.confidence == "derived":
                    self.report.fine_grained_lineage_edges_derived += 1
                else:
                    self.report.fine_grained_lineage_edges_unresolved += 1

                for physical_urn in semantic_field.upstream_physical_urns:
                    upstream_physical_urns.add(physical_urn)
                    for chart_urn in chart_urns:
                        chart_inputs.setdefault(chart_urn, set()).add(physical_urn)
                    physical_field_urn = make_schema_field_urn(physical_urn, semantic_field.field_name)
                    physical_edge = (
                        tuple([physical_field_urn]),
                        tuple([downstream_field_urn]),
                    )
                    if physical_edge in fine_grained_dedupe:
                        continue
                    fine_grained_dedupe.add(physical_edge)
                    fine_grained_lineages.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            upstreams=[physical_field_urn],
                            downstreams=[downstream_field_urn],
                            transformOperation="OMNI_SEMANTIC_TO_PHYSICAL_MAPPING",
                        )
                    )

            all_upstreams = set(upstream_semantic_urns)
            all_upstreams.update(upstream_physical_urns)
            if all_upstreams or fine_grained_lineages:
                # Dataset lineage projection for compatibility.
                yield from self._emit_upstream_lineage(
                    dashboard_dataset_urn,
                    all_upstreams,
                    fine_grained_lineages=fine_grained_lineages,
                )

            # Re-emit chart info with resolved inputs.
            for chart_urn in chart_urns:
                input_urns = sorted(chart_inputs.get(chart_urn, set()))
                yield from self._emit_chart_info(
                    chart_urn=chart_urn,
                    title=chart_titles.get(chart_urn, "Omni tile"),
                    description="Omni workbook tab or dashboard tile.",
                    external_url=chart_urls.get(chart_urn, dashboard_url),
                    input_urns=input_urns,
                    custom_properties={"documentId": doc_id},
                    updated_at=document.get("updatedAt"),
                )

            yield from self._emit_dashboard_info(
                dashboard_urn=dashboard_urn,
                title=dashboard_title,
                description="Omni dashboard entity.",
                external_url=dashboard_url,
                chart_urns=chart_urns,
                dataset_urns=sorted(all_upstreams),
                custom_properties={
                    "documentId": doc_id,
                    "ownerName": ((document.get("owner") or {}).get("name") or ""),
                    "omniEmbedUrl": embed_url,
                    "omniEmbedIframe": f'<iframe src="{embed_url}"></iframe>',
                    "topicNames": ",".join(sorted(dashboard_topics)),
                },
                updated_at=document.get("updatedAt"),
            )

    def get_workunits_internal(self) -> Iterator[MetadataWorkUnit]:
        try:
            yield from self._emit_platform_metadata("omni", "Omni")
            yield from self._ingest_semantic_model()
            yield from self._ingest_documents()
            for folder in self.client.list_folders(page_size=self.config.page_size):
                _ = folder
        except Exception as exc:
            self.report.report_failure("omni-source", str(exc))
            return
