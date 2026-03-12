from typing import Dict, List, Optional

import pydantic
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import EnvConfigMixin, PlatformInstanceConfigMixin
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


class OmniSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    base_url: str = Field(
        description="Omni instance URL including /api, e.g. https://myorg.omniapp.co/api."
    )
    api_key: pydantic.SecretStr = Field(description="Omni Organization API key.")
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
        description="Include workbook-only documents that do not have a published dashboard.",
    )
    # Standard AllowDenyPattern filters replace the old flat allowlists.
    model_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex allow/deny patterns for Omni model IDs to ingest.",
    )
    document_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex allow/deny patterns for Omni document identifiers to ingest.",
    )
    include_column_lineage: bool = Field(
        default=True,
        description="Include column-level (fine-grained) lineage from dashboard fields back to Omni semantic view fields.",
    )
    connection_to_platform: Optional[Dict[str, str]] = Field(
        default=None,
        description="Map Omni connection ID to DataHub platform name, e.g. {'<conn-id>': 'snowflake'}.",
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
