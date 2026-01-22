"""
Monitoring API for the runner manager.
Provides endpoints to check project status, execution history, and queue state.
"""
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query, Path
from pydantic import BaseModel, Field

from geppetto.data.models.execution import (
    ExecutionStatus,
    ProjectExecution,
    RunnerStatus,
)
from geppetto.db.client import DatabaseClient
from geppetto.scheduler import ProjectScheduler


# =============================================================================
# API Response Models with Swagger Documentation
# =============================================================================


class ExecutionResponse(BaseModel):
    """
    Detailed execution record for a project run.
    
    Contains all information about a single execution including timing,
    status, and any error details.
    """
    id: int = Field(..., description="Unique execution identifier", example=12345)
    project_id: str = Field(
        ..., 
        description="Project identifier this execution belongs to",
        example="order-validation"
    )
    status: str = Field(
        ..., 
        description="Current execution status",
        example="success",
        json_schema_extra={"enum": ["pending", "running", "success", "failed", "cancelled", "timeout"]}
    )
    scheduled_for: datetime = Field(
        ..., 
        description="Originally scheduled execution time (UTC)",
        example="2026-01-09T10:00:00Z"
    )
    started_at: Optional[datetime] = Field(
        None, 
        description="Actual start time of execution (UTC)",
        example="2026-01-09T10:00:02Z"
    )
    finished_at: Optional[datetime] = Field(
        None, 
        description="Completion time of execution (UTC)",
        example="2026-01-09T10:02:45Z"
    )
    duration_seconds: Optional[float] = Field(
        None, 
        description="Total execution duration in seconds",
        example=163.5
    )
    exit_code: Optional[int] = Field(
        None, 
        description="Process exit code (0 = success)",
        example=0
    )
    error_message: Optional[str] = Field(
        None, 
        description="Error details if execution failed",
        example=None
    )
    created_at: Optional[datetime] = Field(
        None, 
        description="Record creation timestamp (UTC)",
        example="2026-01-09T10:00:00Z"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": 12345,
                "project_id": "order-validation",
                "status": "success",
                "scheduled_for": "2026-01-09T10:00:00Z",
                "started_at": "2026-01-09T10:00:02Z",
                "finished_at": "2026-01-09T10:02:45Z",
                "duration_seconds": 163.5,
                "exit_code": 0,
                "error_message": None,
                "created_at": "2026-01-09T10:00:00Z"
            }
        }
    }


class ProjectStatusResponse(BaseModel):
    """
    Status information for a specific project.
    
    Includes scheduling details and the most recent execution.
    """
    project_id: str = Field(
        ..., 
        description="Unique project identifier",
        example="order-validation"
    )
    project_name: str = Field(
        ..., 
        description="Human-readable project name",
        example="Order Validation Rules"
    )
    is_scheduled: bool = Field(
        ..., 
        description="Whether the project is currently scheduled for execution",
        example=True
    )
    next_run: Optional[datetime] = Field(
        None, 
        description="Next scheduled execution time (UTC)",
        example="2026-01-09T11:00:00Z"
    )
    cron_expression: str = Field(
        ..., 
        description="Cron expression defining the schedule",
        example="0 * * * *"
    )
    timezone: str = Field(
        ..., 
        description="IANA timezone for schedule interpretation",
        example="Europe/Istanbul"
    )
    last_execution: Optional[ExecutionResponse] = Field(
        None, 
        description="Most recent execution record, if any"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "project_id": "order-validation",
                "project_name": "Order Validation Rules",
                "is_scheduled": True,
                "next_run": "2026-01-09T11:00:00Z",
                "cron_expression": "0 * * * *",
                "timezone": "Europe/Istanbul",
                "last_execution": {
                    "id": 12345,
                    "project_id": "order-validation",
                    "status": "success",
                    "scheduled_for": "2026-01-09T10:00:00Z",
                    "started_at": "2026-01-09T10:00:02Z",
                    "finished_at": "2026-01-09T10:02:45Z",
                    "duration_seconds": 163.5,
                    "exit_code": 0,
                    "error_message": None,
                    "created_at": "2026-01-09T10:00:00Z"
                }
            }
        }
    }


class QueueItemResponse(BaseModel):
    """
    A project currently in the scheduling queue.
    
    Represents a pending scheduled execution waiting to be run.
    """
    project_id: str = Field(
        ..., 
        description="Unique project identifier",
        example="order-validation"
    )
    project_name: str = Field(
        ..., 
        description="Human-readable project name",
        example="Order Validation Rules"
    )
    next_run: datetime = Field(
        ..., 
        description="Scheduled execution time (UTC)",
        example="2026-01-09T11:00:00Z"
    )
    cron_expression: str = Field(
        ..., 
        description="Cron expression defining the schedule",
        example="0 * * * *"
    )
    timezone: str = Field(
        ..., 
        description="IANA timezone for schedule interpretation",
        example="Europe/Istanbul"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "project_id": "order-validation",
                "project_name": "Order Validation Rules",
                "next_run": "2026-01-09T11:00:00Z",
                "cron_expression": "0 * * * *",
                "timezone": "Europe/Istanbul"
            }
        }
    }


class StatsResponse(BaseModel):
    """
    Aggregate execution statistics.
    
    Provides counts of executions by status and overall success rate.
    """
    total: int = Field(..., description="Total number of executions", example=1250)
    pending: int = Field(..., description="Executions waiting to start", example=2)
    running: int = Field(..., description="Currently executing projects", example=1)
    success: int = Field(..., description="Successfully completed executions", example=1180)
    failed: int = Field(..., description="Failed executions", example=45)
    cancelled: int = Field(..., description="Cancelled executions", example=12)
    timeout: int = Field(..., description="Executions that timed out", example=10)
    success_rate: float = Field(
        ..., 
        description="Percentage of successful executions (0-100)",
        example=94.40
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "total": 1250,
                "pending": 2,
                "running": 1,
                "success": 1180,
                "failed": 45,
                "cancelled": 12,
                "timeout": 10,
                "success_rate": 94.40
            }
        }
    }


class HealthResponse(BaseModel):
    """
    Service health status.
    
    Used for container orchestration health checks and monitoring.
    """
    status: str = Field(
        ..., 
        description="Overall health status",
        example="healthy"
    )
    runner_active: bool = Field(
        ..., 
        description="Whether the scheduler is actively running",
        example=True
    )
    projects_in_queue: int = Field(
        ..., 
        description="Number of projects currently in the scheduling queue",
        example=5
    )
    currently_executing: Optional[str] = Field(
        None, 
        description="Project ID currently being executed, if any",
        example=None
    )
    last_check: Optional[str] = Field(
        None, 
        description="ISO timestamp of last scheduler check",
        example="2026-01-09T10:30:00Z"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "status": "healthy",
                "runner_active": True,
                "projects_in_queue": 5,
                "currently_executing": None,
                "last_check": "2026-01-09T10:30:00Z"
            }
        }
    }


class RefreshResponse(BaseModel):
    """Response after refreshing the project queue."""
    message: str = Field(..., description="Status message", example="Projects refreshed")
    queue_size: int = Field(..., description="New queue size after refresh", example=8)

    model_config = {
        "json_schema_extra": {
            "example": {
                "message": "Projects refreshed",
                "queue_size": 8
            }
        }
    }


class CleanupResponse(BaseModel):
    """Response after cleaning up a project's temporary files."""
    message: str = Field(
        ..., 
        description="Status message describing the cleanup result",
        example="Successfully cleaned up temporary directory for project 'order-validation'"
    )
    cleaned: bool = Field(
        ..., 
        description="Whether files were actually removed",
        example=True
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "message": "Successfully cleaned up temporary directory for project 'order-validation'",
                "cleaned": True
            }
        }
    }


class RunProjectRequest(BaseModel):
    """Request body for running a project on-demand."""
    start_date: str = Field(
        ...,
        description="Start date in ISO format (YYYY-MM-DD or full ISO datetime)",
        example="2026-01-17"
    )
    end_date: str = Field(
        ...,
        description="End date in ISO format (YYYY-MM-DD or full ISO datetime)",
        example="2026-01-17"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "start_date": "2026-01-17",
                "end_date": "2026-01-17"
            }
        }
    }


class RunProjectResponse(BaseModel):
    """Response after triggering a project execution."""
    message: str = Field(
        ...,
        description="Status message",
        example="Execution started for project 'order-validation'"
    )
    execution_id: int = Field(
        ...,
        description="The ID of the created execution record",
        example=12345
    )
    project_id: str = Field(
        ...,
        description="The project that was executed",
        example="order-validation"
    )
    status: str = Field(
        ...,
        description="Final execution status",
        example="success"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "message": "Execution completed for project 'order-validation'",
                "execution_id": 12345,
                "project_id": "order-validation",
                "status": "success"
            }
        }
    }


def execution_to_response(exec: ProjectExecution) -> ExecutionResponse:
    """Convert ProjectExecution to API response."""
    duration = None
    if exec.started_at and exec.finished_at:
        duration = (exec.finished_at - exec.started_at).total_seconds()
    
    return ExecutionResponse(
        id=exec.id,
        project_id=exec.project_id,
        status=exec.status.value,
        scheduled_for=exec.scheduled_for,
        started_at=exec.started_at,
        finished_at=exec.finished_at,
        duration_seconds=duration,
        exit_code=exec.exit_code,
        error_message=exec.error_message,
        created_at=exec.created_at,
    )


def create_monitoring_api(
    db_client: DatabaseClient,
    scheduler: ProjectScheduler,
    lifespan=None,
    executor=None,
    version: str = "1.0.0",
) -> FastAPI:
    """
    Create the monitoring FastAPI application.
    
    Args:
        db_client: Database client instance
        scheduler: Project scheduler instance
        lifespan: Optional lifespan context manager
        executor: Optional project executor instance for cleanup operations
        
    Returns:
        Configured FastAPI application
    """
    app = FastAPI(
        title="Cricket Runner Manager",
        description="""
## 🦗 Cricket Runner Manager API

Cricket is a cron-based scheduler that dynamically synthesizes and executes discrepancy detection projects.

### Features

- **Cron-based Scheduling** - Full cron expression support with timezone awareness
- **Dynamic Code Generation** - Synthesize Python detector projects from templates
- **Execution Monitoring** - Track execution history and statistics
- **Queue Management** - View and refresh the scheduling queue

### Quick Start

1. Check service health: `GET /health`
2. View queue status: `GET /queue`
3. Get execution stats: `GET /stats`
4. View project history: `GET /projects/{project_id}/executions`

### Execution States

| Status | Description |
|--------|-------------|
| `pending` | Execution scheduled, waiting to start |
| `running` | Currently executing |
| `success` | Completed successfully |
| `failed` | Execution failed with error |
| `cancelled` | Execution was cancelled |
| `timeout` | Execution exceeded time limit |
        """,
        version=version,
        lifespan=lifespan,
        openapi_tags=[
            {
                "name": "Health",
                "description": "Service health and liveness checks",
            },
            {
                "name": "Queue",
                "description": "Scheduling queue management and status",
            },
            {
                "name": "Projects",
                "description": "Project-specific status and operations",
            },
            {
                "name": "Executions",
                "description": "Execution history and details",
            },
            {
                "name": "Statistics",
                "description": "Aggregate execution metrics",
            },
        ],
        docs_url="/docs",
        redoc_url="/redoc",
    )

    @app.get(
        "/health",
        response_model=HealthResponse,
        tags=["Health"],
        summary="Health check",
        description="""
Check the health and liveness of the Cricket Runner Manager service.

This endpoint is designed for container orchestration health checks (e.g., Kubernetes, Docker).

Returns the current state of the scheduler including:
- Whether the scheduler is actively running
- Number of projects in the queue
- Current execution status
- Last scheduler check timestamp
        """,
        responses={
            200: {
                "description": "Service is healthy",
                "content": {
                    "application/json": {
                        "example": {
                            "status": "healthy",
                            "runner_active": True,
                            "projects_in_queue": 5,
                            "currently_executing": None,
                            "last_check": "2026-01-09T10:30:00Z"
                        }
                    }
                }
            }
        }
    )
    def health():
        """Health check endpoint."""
        status = scheduler.get_status()
        return {
            "status": "healthy",
            "runner_active": status.is_running,
            "projects_in_queue": status.projects_in_queue,
            "currently_executing": status.currently_executing,
            "last_check": status.last_check_time.isoformat() if status.last_check_time else None,
        }

    @app.get(
        "/status",
        response_model=RunnerStatus,
        tags=["Health"],
        summary="Get runner status",
        description="""
Get the comprehensive status of the Cricket Runner Manager.

Provides detailed information about:
- Scheduler running state
- Queue size and current execution
- Cumulative execution counts (total, successful, failed)
- Last check timestamp
        """,
        responses={
            200: {
                "description": "Current runner status",
                "content": {
                    "application/json": {
                        "example": {
                            "is_running": True,
                            "projects_in_queue": 5,
                            "currently_executing": None,
                            "total_executions": 1250,
                            "successful_executions": 1180,
                            "failed_executions": 70,
                            "last_check_time": "2026-01-09T10:30:00Z"
                        }
                    }
                }
            }
        }
    )
    def get_runner_status():
        """Get the overall runner status."""
        return scheduler.get_status()

    @app.get(
        "/queue",
        response_model=List[QueueItemResponse],
        tags=["Queue"],
        summary="Get scheduling queue",
        description="""
Retrieve the current scheduling queue showing all pending project executions.

The queue is ordered by next scheduled execution time (earliest first).
Each item includes:
- Project identification
- Next scheduled run time
- Cron expression and timezone
        """,
        responses={
            200: {
                "description": "List of queued projects",
                "content": {
                    "application/json": {
                        "example": [
                            {
                                "project_id": "order-validation",
                                "project_name": "Order Validation Rules",
                                "next_run": "2026-01-09T11:00:00Z",
                                "cron_expression": "0 * * * *",
                                "timezone": "Europe/Istanbul"
                            },
                            {
                                "project_id": "delivery-checks",
                                "project_name": "Delivery Time Checks",
                                "next_run": "2026-01-09T12:00:00Z",
                                "cron_expression": "0 */2 * * *",
                                "timezone": "UTC"
                            }
                        ]
                    }
                }
            }
        }
    )
    def get_queue():
        """Get the current scheduling queue."""
        queue_items = scheduler.get_queue_status()
        return [
            QueueItemResponse(
                project_id=item["project_id"],
                project_name=item["project_name"],
                next_run=datetime.fromisoformat(item["next_run"]),
                cron_expression=item["cron_expression"],
                timezone=item["timezone"],
            )
            for item in queue_items
        ]

    @app.get(
        "/projects/{project_id}",
        response_model=ProjectStatusResponse,
        tags=["Projects"],
        summary="Get project status",
        description="""
Get detailed status information for a specific project.

Returns:
- Project identification and name
- Whether it's currently scheduled
- Next scheduled run time
- Cron expression and timezone
- Most recent execution details
        """,
        responses={
            200: {
                "description": "Project status retrieved successfully",
            },
            404: {
                "description": "Project not found",
                "content": {
                    "application/json": {
                        "example": {"detail": "Project not found"}
                    }
                }
            }
        }
    )
    def get_project_status(
        project_id: str = Path(
            ...,
            description="Unique project identifier",
            example="order-validation"
        )
    ):
        """Get status for a specific project."""
        # Check if project is in the queue
        queue_items = scheduler.get_queue_status()
        queue_item = next(
            (item for item in queue_items if item["project_id"] == project_id),
            None
        )
        
        if not queue_item:
            # Project might exist but not be scheduled
            executions = db_client.get_project_executions(project_id, limit=1)
            if not executions:
                raise HTTPException(status_code=404, detail="Project not found")
            
            last_exec = executions[0] if executions else None
            return ProjectStatusResponse(
                project_id=project_id,
                project_name="Unknown",
                is_scheduled=False,
                cron_expression="",
                timezone="UTC",
                last_execution=execution_to_response(last_exec) if last_exec else None,
            )
        
        # Get last execution
        executions = db_client.get_project_executions(project_id, limit=1)
        last_exec = executions[0] if executions else None
        
        return ProjectStatusResponse(
            project_id=project_id,
            project_name=queue_item["project_name"],
            is_scheduled=True,
            next_run=datetime.fromisoformat(queue_item["next_run"]),
            cron_expression=queue_item["cron_expression"],
            timezone=queue_item["timezone"],
            last_execution=execution_to_response(last_exec) if last_exec else None,
        )

    @app.get(
        "/projects/{project_id}/executions",
        response_model=List[ExecutionResponse],
        tags=["Executions"],
        summary="Get project execution history",
        description="""
Retrieve the execution history for a specific project.

Returns a list of executions ordered by most recent first.
Use the `limit` parameter to control how many records to retrieve.

Each execution includes:
- Timing information (scheduled, started, finished)
- Status and exit code
- Duration calculation
- Error message if failed
        """,
        responses={
            200: {
                "description": "Execution history retrieved successfully",
            }
        }
    )
    def get_project_executions(
        project_id: str = Path(
            ...,
            description="Unique project identifier",
            example="order-validation"
        ),
        limit: int = Query(
            50,
            ge=1,
            le=500,
            description="Maximum number of executions to return (1-500)"
        )
    ):
        """Get execution history for a project."""
        executions = db_client.get_project_executions(project_id, limit=limit)
        return [execution_to_response(e) for e in executions]

    @app.get(
        "/executions/{execution_id}",
        response_model=ExecutionResponse,
        tags=["Executions"],
        summary="Get execution details",
        description="""
Retrieve detailed information about a specific execution by its ID.

Returns complete execution information including:
- Status and timing
- Exit code and duration
- Error message if applicable
        """,
        responses={
            200: {
                "description": "Execution details retrieved successfully",
            },
            404: {
                "description": "Execution not found",
                "content": {
                    "application/json": {
                        "example": {"detail": "Execution not found"}
                    }
                }
            }
        }
    )
    def get_execution(
        execution_id: int = Path(
            ...,
            ge=1,
            description="Unique execution identifier",
            example=12345
        )
    ):
        """Get a specific execution record."""
        execution = db_client.get_execution(execution_id)
        if not execution:
            raise HTTPException(status_code=404, detail="Execution not found")
        return execution_to_response(execution)

    @app.get(
        "/stats",
        response_model=StatsResponse,
        tags=["Statistics"],
        summary="Get execution statistics",
        description="""
Retrieve aggregate execution statistics across all projects.

Provides:
- Total execution count
- Breakdown by status (pending, running, success, failed, cancelled, timeout)
- Overall success rate percentage
        """,
        responses={
            200: {
                "description": "Statistics retrieved successfully",
                "content": {
                    "application/json": {
                        "example": {
                            "total": 1250,
                            "pending": 2,
                            "running": 1,
                            "success": 1180,
                            "failed": 45,
                            "cancelled": 12,
                            "timeout": 10,
                            "success_rate": 94.40
                        }
                    }
                }
            }
        }
    )
    def get_stats():
        """Get execution statistics."""
        stats = db_client.get_execution_stats()
        
        total = sum(stats.values())
        success = stats.get("success", 0)
        success_rate = (success / total * 100) if total > 0 else 0.0
        
        return StatsResponse(
            total=total,
            pending=stats.get("pending", 0),
            running=stats.get("running", 0),
            success=success,
            failed=stats.get("failed", 0),
            cancelled=stats.get("cancelled", 0),
            timeout=stats.get("timeout", 0),
            success_rate=round(success_rate, 2),
        )

    @app.post(
        "/projects/refresh",
        response_model=RefreshResponse,
        tags=["Queue"],
        summary="Refresh project queue",
        description="""
Trigger a refresh of the project scheduling queue from the database.

This operation:
1. Fetches the latest active projects from the database
2. Updates the scheduling queue with any new or modified projects
3. Removes projects that are no longer active

Use this endpoint after:
- Adding new projects to the database
- Modifying project schedules
- Activating/deactivating projects

**Note:** Existing projects preserve their current next_run time; only new projects get recalculated.
        """,
        responses={
            200: {
                "description": "Queue refreshed successfully",
                "content": {
                    "application/json": {
                        "example": {
                            "message": "Projects refreshed",
                            "queue_size": 8
                        }
                    }
                }
            }
        }
    )
    def refresh_projects():
        """Trigger a refresh of the project queue from the database."""
        scheduler.refresh_projects()
        return RefreshResponse(
            message="Projects refreshed",
            queue_size=scheduler.get_status().projects_in_queue
        )

    @app.delete(
        "/projects/{project_id}/cleanup",
        response_model=CleanupResponse,
        tags=["Projects"],
        summary="Clean up project files",
        description="""
Remove the generated temporary directory for an inactive project.

This operation:
1. Verifies the project is not currently active
2. Removes the generated detector code directory
3. Frees up disk space

**Prerequisites:**
- Project must be deactivated (not in the scheduling queue)
- Executor must be configured

**Use cases:**
- After permanently disabling a project
- When reclaiming disk space
- Before removing a project from the database
        """,
        responses={
            200: {
                "description": "Cleanup completed",
                "content": {
                    "application/json": {
                        "examples": {
                            "cleaned": {
                                "summary": "Files removed",
                                "value": {
                                    "message": "Successfully cleaned up temporary directory for project 'order-validation'",
                                    "cleaned": True
                                }
                            },
                            "not_found": {
                                "summary": "No files to clean",
                                "value": {
                                    "message": "No temporary directory found for project 'order-validation'",
                                    "cleaned": False
                                }
                            }
                        }
                    }
                }
            },
            400: {
                "description": "Project is still active",
                "content": {
                    "application/json": {
                        "example": {
                            "detail": "Project 'order-validation' is still active. Deactivate it first before cleanup."
                        }
                    }
                }
            },
            500: {
                "description": "Cleanup failed",
                "content": {
                    "application/json": {
                        "example": {
                            "detail": "Failed to remove directory: Permission denied"
                        }
                    }
                }
            },
            503: {
                "description": "Executor not available",
                "content": {
                    "application/json": {
                        "example": {
                            "detail": "Cleanup operation not available - executor not configured"
                        }
                    }
                }
            }
        }
    )
    def cleanup_project(
        project_id: str = Path(
            ...,
            description="Unique project identifier to clean up",
            example="order-validation"
        )
    ):
        """
        Clean up the temporary directory of an inactive project.
        
        This removes the generated code directory for the specified project.
        Should be called when a project is deactivated to free up disk space.
        """
        if executor is None:
            raise HTTPException(
                status_code=503,
                detail="Cleanup operation not available - executor not configured"
            )
        
        # Check if project is in the active queue
        queue_items = scheduler.get_queue_status()
        is_active = any(item["project_id"] == project_id for item in queue_items)
        
        if is_active:
            raise HTTPException(
                status_code=400,
                detail=f"Project '{project_id}' is still active. Deactivate it first before cleanup."
            )
        
        # Check if directory exists
        if not executor.project_dir_exists(project_id):
            return CleanupResponse(
                message=f"No temporary directory found for project '{project_id}'",
                cleaned=False
            )
        
        try:
            executor.cleanup_project(project_id)
            return CleanupResponse(
                message=f"Successfully cleaned up temporary directory for project '{project_id}'",
                cleaned=True
            )
        except RuntimeError as e:
            raise HTTPException(status_code=500, detail=str(e))

    @app.post(
        "/projects/{project_id}/run",
        response_model=RunProjectResponse,
        tags=["Projects"],
        summary="Run project on-demand",
        description="""
Execute a project immediately with a specified date range, bypassing the scheduler.

This is useful for:
- Testing project configurations
- Ad-hoc analysis for specific date ranges
- Debugging rule logic
- Manual re-runs after failures

**Note:** This runs synchronously and may take several minutes depending on the data volume.

The execution will:
1. Generate detector code from templates
2. Run the detector with the specified date range
3. Upload results to CDN (if configured)
4. Send callback notification (if configured)
5. Record execution in the database
        """,
        responses={
            200: {
                "description": "Execution completed",
                "content": {
                    "application/json": {
                        "examples": {
                            "success": {
                                "summary": "Successful execution",
                                "value": {
                                    "message": "Execution completed for project 'order-validation'",
                                    "execution_id": 12345,
                                    "project_id": "order-validation",
                                    "status": "success"
                                }
                            },
                            "discrepancies_found": {
                                "summary": "Discrepancies found",
                                "value": {
                                    "message": "Execution completed for project 'order-validation'",
                                    "execution_id": 12346,
                                    "project_id": "order-validation",
                                    "status": "success"
                                }
                            }
                        }
                    }
                }
            },
            404: {
                "description": "Project not found",
                "content": {
                    "application/json": {
                        "example": {"detail": "Project 'invalid-project' not found"}
                    }
                }
            },
            503: {
                "description": "Executor not available",
                "content": {
                    "application/json": {
                        "example": {"detail": "Run operation not available - executor not configured"}
                    }
                }
            }
        }
    )
    def run_project(
        project_id: str = Path(
            ...,
            description="Unique project identifier to execute",
            example="order-validation"
        ),
        request: RunProjectRequest = ...,
    ):
        """
        Run a project on-demand with specified date range.
        
        This executes the project immediately without waiting for the scheduler.
        The execution is synchronous and will block until complete.
        """
        if executor is None:
            raise HTTPException(
                status_code=503,
                detail="Run operation not available - executor not configured"
            )
        
        # Check if project exists
        project = db_client.get_project(project_id)
        if not project:
            raise HTTPException(
                status_code=404,
                detail=f"Project '{project_id}' not found"
            )
        
        # Execute the project
        execution = executor.execute_standalone(
            project_id=project_id,
            start_date=request.start_date,
            end_date=request.end_date,
        )
        
        return RunProjectResponse(
            message=f"Execution completed for project '{project_id}'",
            execution_id=execution.id,
            project_id=project_id,
            status=execution.status.value,
        )

    return app
