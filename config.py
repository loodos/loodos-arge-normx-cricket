"""
Configuration for the runner manager.
"""
from pydantic_settings import BaseSettings
from typing import Optional

from geppetto.data.models.cdn import CdnConfig


class Settings(BaseSettings):
    """Runner manager configuration."""
    
    # Database
    DATABASE_URL: str = "postgresql://localhost:5432/cricket"
    
    # CDN Configuration for report uploads
    CDN_URL: str = ""
    CDN_ACCESS_KEY: str = ""
    CDN_SECRET_KEY: str = ""
    CDN_BUCKET_NAME: str = ""
    CDN_ENABLE_SSL: bool = True
    
    # Report callback URL (notified after CDN upload)
    ENLIQ_REPORT_CALLBACK_URL: str = ""
    
    # Scheduler settings
    MAX_QUEUE_SIZE: int = 10
    SCHEDULER_CHECK_INTERVAL: float = 60.0  # seconds
    
    # Executor settings
    EXECUTION_TIMEOUT: int = 600  # 10 minutes
    WORK_DIR: Optional[str] = None  # Default: temp directory
    
    # API settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8080
    
    class Config:
        env_file = ".env"
        extra = "ignore"

    def get_cdn_config(self) -> Optional[CdnConfig]:
        """
        Get CDN configuration if all required fields are set.
        
        Returns:
            CdnConfig if configured, None otherwise
        """
        if not all([self.CDN_URL, self.CDN_ACCESS_KEY, self.CDN_SECRET_KEY, self.CDN_BUCKET_NAME]):
            return None
        
        return CdnConfig(
            cdn_url=self.CDN_URL,
            access_key=self.CDN_ACCESS_KEY,
            secret_key=self.CDN_SECRET_KEY,
            bucket_name=self.CDN_BUCKET_NAME,
            enable_ssl=self.CDN_ENABLE_SSL,
        )


settings = Settings()
