"""
CDN configuration models for report uploads.
"""
from pydantic import BaseModel, Field


class CdnConfig(BaseModel):
    """Configuration for CDN/S3-compatible storage uploads using boto3."""

    cdn_url: str = Field(
        ...,
        description="S3-compatible endpoint URL (e.g., Cloudflare R2 endpoint)",
    )
    access_key: str = Field(
        ...,
        description="Access key for S3-compatible storage",
    )
    secret_key: str = Field(
        ...,
        description="Secret key for S3-compatible storage",
    )
    bucket_name: str = Field(
        ...,
        description="Name of the bucket to upload to",
    )
    enable_ssl: bool = Field(
        default=True,
        description="Whether to use SSL for uploads",
    )
