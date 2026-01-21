"""
Scheduler for managing project execution based on cron expressions.
Uses a priority queue to determine which project should run next.
"""
import heapq
import threading
import time
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

from croniter import croniter
import pytz

from geppetto.data.models.execution import (
    ProjectConfig,
    RunnerStatus,
    ScheduledProject,
)
from geppetto.db.client import DatabaseClient


class ProjectScheduler:
    """
    Manages scheduling of project executions based on cron expressions.
    Maintains a priority queue of up to max_queue_size projects.
    """

    def __init__(
        self,
        db_client: DatabaseClient,
        max_queue_size: int = 10,
        check_interval: float = 1.0,
    ):
        """
        Initialize the scheduler.
        
        Args:
            db_client: Database client for fetching projects and updating status
            max_queue_size: Maximum number of projects to keep in the queue
            check_interval: How often to check for due projects (seconds)
        """
        self.db_client = db_client
        self.max_queue_size = max_queue_size
        self.check_interval = check_interval
        
        # Priority queue: (next_run_timestamp, project_id, ScheduledProject)
        self._queue: List[tuple] = []
        self._projects: Dict[str, ProjectConfig] = {}
        self._lock = threading.Lock()
        
        self._running = False
        self._scheduler_thread: Optional[threading.Thread] = None
        self._on_execute: Optional[Callable[[ScheduledProject], None]] = None
        
        # Status tracking
        self._status = RunnerStatus()

    def _get_next_run(self, project: ProjectConfig, base_time: Optional[datetime] = None) -> datetime:
        """
        Calculate the next run time for a project based on its cron expression.
        
        Args:
            project: The project configuration
            base_time: Base time to calculate from (default: now)
            
        Returns:
            Next scheduled run time (UTC)
        """
        if base_time is None:
            base_time = datetime.now(timezone.utc)
        
        # Handle timezone
        tz = pytz.timezone(project.timezone)
        local_time = base_time.astimezone(tz)
        
        # Calculate next run using croniter
        cron = croniter(project.cron_expression, local_time)
        next_local = cron.get_next(datetime)
        
        # Convert back to UTC
        return next_local.astimezone(timezone.utc)

    def load_projects(self) -> None:
        """
        Load active projects from the database and populate the queue.
        """
        with self._lock:
            # Fetch projects
            projects = self.db_client.fetch_active_projects(limit=self.max_queue_size)
            
            # Clear current state
            self._queue = []
            self._projects = {}
            
            # Add each project to the queue
            now = datetime.now(timezone.utc)
            for project in projects:
                self._projects[project.id] = project
                next_run = self._get_next_run(project, now)
                
                scheduled = ScheduledProject(
                    project=project,
                    next_run=next_run,
                )
                
                # Push to priority queue (heapq uses min-heap)
                heapq.heappush(
                    self._queue,
                    (next_run.timestamp(), project.id, scheduled)
                )
            
            self._status.projects_in_queue = len(self._queue)
            print(f"Loaded {len(projects)} projects into scheduler queue")

    def refresh_projects(self) -> None:
        """
        Refresh projects from the database, updating the queue.
        Preserves next_run times for existing projects.
        """
        with self._lock:
            # Fetch fresh project list
            projects = self.db_client.fetch_active_projects(limit=self.max_queue_size)
            new_project_ids = {p.id for p in projects}
            
            # Build map of current scheduled projects
            current_scheduled = {item[1]: item[2] for item in self._queue}
            
            # Rebuild queue
            self._queue = []
            self._projects = {}
            
            now = datetime.now(timezone.utc)
            for project in projects:
                self._projects[project.id] = project
                
                # Preserve next_run if project already scheduled
                if project.id in current_scheduled:
                    scheduled = current_scheduled[project.id]
                    # Update project config but keep next_run
                    scheduled.project = project
                else:
                    # New project, calculate next run
                    next_run = self._get_next_run(project, now)
                    scheduled = ScheduledProject(
                        project=project,
                        next_run=next_run,
                    )
                
                heapq.heappush(
                    self._queue,
                    (scheduled.next_run.timestamp(), project.id, scheduled)
                )
            
            self._status.projects_in_queue = len(self._queue)

    def _reschedule_project(self, project_id: str) -> None:
        """
        Reschedule a project for its next run after execution.
        
        Args:
            project_id: The project to reschedule
        """
        with self._lock:
            if project_id not in self._projects:
                return
            
            project = self._projects[project_id]
            now = datetime.now(timezone.utc)
            next_run = self._get_next_run(project, now)
            
            scheduled = ScheduledProject(
                project=project,
                next_run=next_run,
            )
            
            heapq.heappush(
                self._queue,
                (next_run.timestamp(), project_id, scheduled)
            )
            
            self._status.projects_in_queue = len(self._queue)

    def get_next_scheduled(self) -> Optional[ScheduledProject]:
        """
        Get the next scheduled project without removing it from the queue.
        
        Returns:
            The next scheduled project, or None if queue is empty
        """
        with self._lock:
            if not self._queue:
                return None
            return self._queue[0][2]

    def pop_if_due(self) -> Optional[ScheduledProject]:
        """
        Pop and return the next project if it's due for execution.
        
        Returns:
            ScheduledProject if one is due, None otherwise
        """
        with self._lock:
            if not self._queue:
                return None
            
            now = datetime.now(timezone.utc)
            next_ts, project_id, scheduled = self._queue[0]
            
            if next_ts <= now.timestamp():
                heapq.heappop(self._queue)
                self._status.projects_in_queue = len(self._queue)
                return scheduled
            
            return None

    def set_on_execute(self, callback: Callable[[ScheduledProject], None]) -> None:
        """
        Set the callback to be called when a project is due for execution.
        
        Args:
            callback: Function to call with the scheduled project
        """
        self._on_execute = callback

    def _scheduler_loop(self) -> None:
        """Main scheduler loop that checks for due projects."""
        print("Scheduler loop started")

        while self._running:
            try:
                self._status.last_check_time = datetime.now(timezone.utc)

                # Check if any project is due
                scheduled = self.pop_if_due()

                if scheduled and self._on_execute:
                    project_id = scheduled.project.id

                    # Refresh projects from database before execution to ensure
                    # we have the latest config and the project is still active
                    print(f"Refreshing projects before executing {project_id}")
                    self.refresh_projects()

                    # Check if project is still active after refresh
                    if project_id not in self._projects:
                        print(f"Project {project_id} is no longer active, skipping execution")
                        continue

                    # Get the latest project config after refresh
                    updated_project = self._projects[project_id]
                    scheduled.project = updated_project

                    self._status.currently_executing = project_id

                    try:
                        # Execute the project with latest config
                        self._on_execute(scheduled)
                        self._status.successful_executions += 1
                    except Exception as e:
                        print(f"Error executing project {project_id}: {e}")
                        self._status.failed_executions += 1
                    finally:
                        self._status.total_executions += 1
                        self._status.currently_executing = None

                        # Reschedule for next run (only if still active)
                        if project_id in self._projects:
                            self._reschedule_project(project_id)

                # Sleep before next check
                time.sleep(self.check_interval)

            except Exception as e:
                print(f"Scheduler error: {e}")
                time.sleep(self.check_interval)

    def start(self) -> None:
        """Start the scheduler in a background thread."""
        if self._running:
            print("Scheduler is already running")
            return
        
        self._running = True
        self._status.is_running = True
        
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            daemon=True,
            name="project-scheduler",
        )
        self._scheduler_thread.start()
        print("Scheduler started")

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        self._status.is_running = False
        
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5.0)
            self._scheduler_thread = None
        
        print("Scheduler stopped")

    def get_status(self) -> RunnerStatus:
        """Get the current runner status."""
        return self._status

    def get_queue_status(self) -> List[Dict]:
        """
        Get the current state of the scheduling queue.
        
        Returns:
            List of projects with their next scheduled run times
        """
        with self._lock:
            result = []
            for ts, project_id, scheduled in sorted(self._queue):
                result.append({
                    "project_id": project_id,
                    "project_name": scheduled.project.name,
                    "next_run": scheduled.next_run.isoformat(),
                    "cron_expression": scheduled.project.cron_expression,
                    "timezone": scheduled.project.timezone,
                })
            return result
