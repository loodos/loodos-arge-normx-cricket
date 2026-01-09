"""
Models for project scheduling and execution tracking.
"""
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class ExecutionStatus(str, Enum):
    """Status of a project execution."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"


class ProjectSchedule(BaseModel):
    """Project schedule configuration from the database."""
    id: int
    project_id: str
    cron_expression: str
    timezone: str = "UTC"
    allow_concurrent: bool = False
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ProjectConfig(BaseModel):
    """Project configuration loaded from the database."""
    id: str
    name: str
    config: Dict[str, Any]  # Contains data_source config and other settings
    cron_expression: str
    timezone: str = "UTC"
    allow_concurrent: bool = False


class ProjectExecution(BaseModel):
    """Execution record for a project run."""
    id: Optional[int] = None
    project_id: str
    status: ExecutionStatus
    scheduled_for: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    exit_code: Optional[int] = None
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None


class ScheduledProject(BaseModel):
    """A project with its next scheduled run time."""
    project: ProjectConfig
    next_run: datetime
    execution_id: Optional[int] = None
    
    class Config:
        arbitrary_types_allowed = True


class RunnerStatus(BaseModel):
    """Overall status of the runner manager."""
    is_running: bool = False
    projects_in_queue: int = 0
    currently_executing: Optional[str] = None
    total_executions: int = 0
    successful_executions: int = 0
    failed_executions: int = 0
    last_check_time: Optional[datetime] = None
