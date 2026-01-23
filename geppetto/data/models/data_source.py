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
        default=None, description="Optional static authentication token"
    )
    # Login credentials for dynamic token fetching
    login_url: str | None = Field(
        default="https://api.frink.com.tr/api/panel/auth/login", description="Login endpoint URL for fetching access token"
    )
    login_email: str | None = Field(
        default="harun.uz@loodos.com", description="Email for login authentication"
    )
    login_password: str | None = Field(
        default="I19Ws3", description="Password for login authentication"
    )


class ManualConfig(BaseModel):
    type: Literal[DataSourceType.MANUAL] = DataSourceType.MANUAL
    # No additional config needed - data comes via API


DataSourceConfig = Union[SqlConfig, ApiConfig, ManualConfig]
