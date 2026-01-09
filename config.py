"""
Configuration for the runner manager.
"""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Runner manager configuration."""
    
    # Database
    DATABASE_URL: str = "postgresql://localhost:5432/cricket"
    
    # Report destination URL (passed to child projects)
    ENLIQ_REPORT_URL: str = ""
    
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


settings = Settings()
