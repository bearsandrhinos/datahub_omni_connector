from typing import Dict, List, Optional

from pydantic import Field

from datahub.configuration.common import ConfigModel


class OmniSourceConfig(ConfigModel):
    env: str = Field(default="PROD", description="DataHub environment name.")
    base_url: str = Field(
        description="Omni instance URL including /api, for example https://myorg.omniapp.co/api."
    )
    api_key: str = Field(description="Omni Organization API key.")
    page_size: int = Field(
        default=50, ge=1, le=100, description="Page size for paginated Omni endpoints."
    )
    max_requests_per_minute: int = Field(
        default=50, ge=1, le=60, description="Client-side throttle cap for Omni API requests."
    )
    timeout_seconds: int = Field(
        default=30, ge=5, le=120, description="HTTP timeout for Omni API calls."
    )
    include_deleted: bool = Field(
        default=False, description="Include deleted Omni entities when supported."
    )
    include_workbook_only: bool = Field(
        default=False,
        description="Include workbook-only documents without dashboards.",
    )
    model_allowlist: Optional[List[str]] = Field(
        default=None, description="Optional list of model IDs to ingest."
    )
    document_allowlist: Optional[List[str]] = Field(
        default=None, description="Optional list of document identifiers to ingest."
    )
    enable_column_lineage: bool = Field(
        default=True,
        description="Enable column-level lineage resolution in the source pipeline.",
    )
    connection_to_platform: Optional[Dict[str, str]] = Field(
        default=None,
        description="Map Omni connection ID to DataHub platform name, for example {'<conn-id>': 'snowflake'}.",
    )
    connection_to_platform_instance: Optional[Dict[str, str]] = Field(
        default=None,
        description="Map Omni connection ID to DataHub platform instance name.",
    )
    connection_to_database: Optional[Dict[str, str]] = Field(
        default=None,
        description="Map Omni connection ID to canonical database name used in DataHub URNs.",
    )
    normalize_snowflake_names: bool = Field(
        default=True,
        description="Upper-case database, schema, and table names when platform is snowflake for URN matching.",
    )