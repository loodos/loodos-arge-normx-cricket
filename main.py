"""
Cricket Runner Manager
A cron-based scheduler that runs discrepancy detection projects.
"""
import signal
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from config import settings
from geppetto.api import create_monitoring_api
from geppetto.db.client import DatabaseClient
from geppetto.executor import ProjectExecutor
from geppetto.scheduler import ProjectScheduler
from synthesizer import CodeSynthesizer


# Global instances for the application
_db_client: DatabaseClient = None
_scheduler: ProjectScheduler = None
_executor: ProjectExecutor = None


def create_components():
    """Create and configure the runner components."""
    global _db_client, _scheduler, _executor
    
    # Initialize components
    _db_client = DatabaseClient(settings.DATABASE_URL)
    synthesizer = CodeSynthesizer()
    
    work_dir = Path(settings.WORK_DIR) if settings.WORK_DIR else None
    _executor = ProjectExecutor(
        db_client=_db_client,
        synthesizer=synthesizer,
        work_dir=work_dir,
        timeout=settings.EXECUTION_TIMEOUT,
        enliq_report_url=settings.ENLIQ_REPORT_URL,
    )
    
    _scheduler = ProjectScheduler(
        db_client=_db_client,
        max_queue_size=settings.MAX_QUEUE_SIZE,
        check_interval=settings.SCHEDULER_CHECK_INTERVAL,
    )
    
    # Set execution callback
    _scheduler.set_on_execute(_executor.execute)
    
    return _db_client, _scheduler, _executor


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler - starts/stops scheduler."""
    print("Starting Cricket Runner Manager...")
    
    # Load projects and start scheduler
    _scheduler.load_projects()
    _scheduler.start()
    
    print(f"Scheduler started with {_scheduler.get_status().projects_in_queue} projects in queue")
    
    yield
    
    # Shutdown
    print("Shutting down scheduler...")
    _scheduler.stop()
    print("Scheduler stopped")


# Create components on module load
create_components()

# Create the FastAPI application
app = create_monitoring_api(_db_client, _scheduler, lifespan=lifespan, executor=_executor)


def main():
    """CLI entry point for local development."""
    import uvicorn
    
    print(f"Starting Cricket Runner Manager on http://{settings.API_HOST}:{settings.API_PORT}")
    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        log_level="info",
        reload=False,
    )


if __name__ == "__main__":
    main()

