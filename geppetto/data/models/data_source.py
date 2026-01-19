from pydantic import BaseModel, Field
from typing import Literal, Union
from enum import Enum


class DataSourceType(str, Enum):
    SQL = "sql"
    API = "api"
    MANUAL = "manual"  # User uploads data via API


class SqlConfig(BaseModel):
    type: Literal[DataSourceType.SQL] = DataSourceType.SQL
    connection_string: str = Field(
        ..., description="SQL database connection string", min_length=1
    )
    query: str = Field(..., description="SQL query to fetch data", min_length=1)
    batch_size: int = Field(default=1000, ge=1, description="Batch size for fetching")
    start_date_column: str = Field(
        default="created_at", description="Column to filter by start date"
    )
    end_date_column: str = Field(
        default="created_at", description="Column to filter by end date"
    )


class ApiConfig(BaseModel):
    type: Literal[DataSourceType.API] = DataSourceType.API.value
    api_url: str = Field(..., description="API endpoint URL", min_length=1)
    api_page_size: int = Field(
        default=100, ge=1, description="Page size for API requests"
    )
    auth_token: str | None = Field(
        default=None, description="Optional authentication token"
    )


class ManualConfig(BaseModel):
    type: Literal[DataSourceType.MANUAL] = DataSourceType.MANUAL
    # No additional config needed - data comes via API


DataSourceConfig = Union[SqlConfig, ApiConfig, ManualConfig]
