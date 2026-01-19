"""
Project executor that synthesizes and runs child detector projects.
"""
import subprocess
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from geppetto.data.models.cdn import CdnConfig
from geppetto.data.models.data_source import DataSourceConfig, ApiConfig, SqlConfig, ManualConfig
from geppetto.data.models.execution import (
    ExecutionStatus,
    ProjectExecution,
    ScheduledProject,
)
from geppetto.data.models.rule import DiscrepancyRule
from geppetto.db.client import DatabaseClient
from synthesizer import CodeSynthesizer


class ProjectExecutor:
    """
    Executes scheduled projects by:
    1. Fetching rules from the database
    2. Synthesizing the detector code
    3. Running the child script
    4. Recording execution results
    """

    def __init__(
        self,
        db_client: DatabaseClient,
        synthesizer: CodeSynthesizer,
        work_dir: Optional[Path] = None,
        timeout: int = 300,  # 5 minutes default
        cdn_config: Optional[CdnConfig] = None,
        callback_url: str = "",
    ):
        """
        Initialize the executor.
        
        Args:
            db_client: Database client for fetching rules and recording executions
            synthesizer: Code synthesizer for generating child projects
            work_dir: Directory to use for generated projects (default: temp dir)
            timeout: Maximum execution time in seconds
            cdn_config: CDN configuration for uploading reports
            callback_url: URL to notify after report upload
        """
        self.db_client = db_client
        self.synthesizer = synthesizer
        self.work_dir = work_dir or Path(tempfile.gettempdir()) / "cricket-projects"
        self.timeout = timeout
        self.cdn_config = cdn_config
        self.callback_url = callback_url
        
        # Ensure work directory exists
        self.work_dir.mkdir(parents=True, exist_ok=True)

    def _parse_data_source_config(self, config: dict) -> DataSourceConfig:
        """
        Parse the data source configuration from project config.
        
        Args:
            config: Project configuration dictionary
            
        Returns:
            DataSourceConfig object
        """
        # data_source = config.get("data_source", {})
        source_type = config.get("type", "manual")
        
        if source_type == "sql":
            return SqlConfig(
                connection_string=config.get("connection_string", ""),
                query=config.get("query", ""),
                batch_size=config.get("batch_size", 1000),
                start_date_column=config.get("start_date_column", "created_at"),
                end_date_column=config.get("end_date_column", "created_at"),
            )
        elif source_type == "api":
            return ApiConfig(
                api_url=config.get("api_url", ""),
                api_page_size=config.get("api_page_size", 100),
                auth_token=config.get("auth_token"),
            )
        else:
            return ManualConfig()

    def _calculate_date_range(self, config: dict) -> tuple[str, str]:
        """
        Calculate the date range for the execution.
        
        Args:
            config: Project configuration
            
        Returns:
            Tuple of (start_date, end_date) in ISO format
        """
        # Default: yesterday to today
        now = datetime.now(timezone.utc)
        
        # Check if config specifies a lookback period
        lookback_days = config.get("lookback_days", 1)
        
        from datetime import timedelta
        start_date = (now - timedelta(days=lookback_days)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        end_date = now.replace(hour=23, minute=59, second=59, microsecond=0)
        
        return start_date.isoformat(), end_date.isoformat()

    def execute(self, scheduled: ScheduledProject) -> ProjectExecution:
        """
        Execute a scheduled project.
        
        Args:
            scheduled: The scheduled project to execute
            
        Returns:
            ProjectExecution with results
        """
        project = scheduled.project
        project_id = project.id
        
        print(f"Starting execution for project: {project_id} ({project.name})")
        
        # Create execution record
        execution_id = self.db_client.create_execution(
            project_id=project_id,
            scheduled_for=scheduled.next_run,
            status=ExecutionStatus.PENDING,
        )
        
        # Check for concurrent execution
        if not project.allow_concurrent:
            running = self.db_client.get_running_execution(project_id)
            if running:
                print(f"Project {project_id} already has a running execution, skipping")
                self.db_client.update_execution_status(
                    execution_id=execution_id,
                    status=ExecutionStatus.CANCELLED,
                    error_message="Concurrent execution not allowed",
                )
                return self.db_client.get_execution(execution_id)
        
        # Mark as running
        started_at = datetime.now(timezone.utc)
        self.db_client.update_execution_status(
            execution_id=execution_id,
            status=ExecutionStatus.RUNNING,
            started_at=started_at,
        )
        
        try:
            # Fetch rules for this project
            rules = self.db_client.fetch_project_rules(project_id)
            
            if not rules:
                raise ValueError(f"No rules found for project {project_id}")
            
            print(f"Found {len(rules)} rules for project {project_id}")
            
            # Parse data source configuration
            data_source_config = self._parse_data_source_config(project.config)
            
            # Calculate date range
            start_date, end_date = self._calculate_date_range(project.config)
            
            # Generate project directory
            project_dir = self.work_dir / project_id
            
            # Synthesize the detector code
            self.synthesizer.generate_codebase(
                project_id=project_id,
                rule_set=rules,
                data_source_config=data_source_config,
                output_dir=project_dir,
            )
            
            print(f"Generated detector code at: {project_dir}")
            
            # Build command with arguments
            # Use sys.executable to run with the same Python interpreter
            # This works both locally and in Docker containers
            cmd = [
                sys.executable, "main.py",
                "--start-date", start_date,
                "--end-date", end_date,
            ]
            
            # Add CDN config if configured
            if self.cdn_config:
                cmd.extend(["--cdn-url", self.cdn_config.cdn_url])
                cmd.extend(["--cdn-access-key", self.cdn_config.access_key])
                cmd.extend(["--cdn-secret-key", self.cdn_config.secret_key])
                cmd.extend(["--cdn-bucket", self.cdn_config.bucket_name])
                if not self.cdn_config.enable_ssl:
                    cmd.append("--cdn-no-ssl")
            
            # Add callback URL if configured
            if self.callback_url:
                cmd.extend(["--callback-url", self.callback_url])
            
            # Run the detector script
            print(f"Running detector with date range: {start_date} to {end_date}")
            result = subprocess.run(
                cmd,
                cwd=project_dir,
                capture_output=True,
                text=True,
                timeout=self.timeout,
            )
            
            finished_at = datetime.now(timezone.utc)
            
            # Determine status based on exit code
            if result.returncode == 0:
                status = ExecutionStatus.SUCCESS
                error_message = None
            elif result.returncode == 1:
                # Exit code 1 means discrepancies were found (expected)
                status = ExecutionStatus.SUCCESS
                error_message = None
            else:
                status = ExecutionStatus.FAILED
                error_message = result.stderr or result.stdout
            
            print(f"Execution finished with status: {status.value}")
            if result.stdout:
                print(f"Output:\n{result.stdout}")
            
            # Update execution record
            self.db_client.update_execution_status(
                execution_id=execution_id,
                status=status,
                finished_at=finished_at,
                exit_code=result.returncode,
                error_message=error_message[:1000] if error_message else None,
            )
            
        except subprocess.TimeoutExpired:
            finished_at = datetime.now(timezone.utc)
            print(f"Execution timed out after {self.timeout} seconds")
            
            self.db_client.update_execution_status(
                execution_id=execution_id,
                status=ExecutionStatus.TIMEOUT,
                finished_at=finished_at,
                error_message=f"Execution timed out after {self.timeout} seconds",
            )
            
        except Exception as e:
            finished_at = datetime.now(timezone.utc)
            print(f"Execution failed with error: {e}")
            
            self.db_client.update_execution_status(
                execution_id=execution_id,
                status=ExecutionStatus.FAILED,
                finished_at=finished_at,
                error_message=str(e)[:1000],
            )
        
        return self.db_client.get_execution(execution_id)

    def execute_standalone(
        self,
        project_id: str,
        start_date: str,
        end_date: str,
    ) -> ProjectExecution:
        """
        Execute a project on-demand without scheduling.
        
        This method runs a project immediately with the specified date range,
        bypassing the scheduler. Useful for testing and ad-hoc executions.
        
        Args:
            project_id: The project identifier to execute
            start_date: Start date in ISO format (e.g., "2026-01-01")
            end_date: End date in ISO format (e.g., "2026-01-31")
            
        Returns:
            ProjectExecution with results
        """
        print(f"Starting standalone execution for project: {project_id}")
        print(f"Date range: {start_date} to {end_date}")
        
        # Create execution record
        scheduled_for = datetime.now(timezone.utc)
        execution_id = self.db_client.create_execution(
            project_id=project_id,
            scheduled_for=scheduled_for,
            status=ExecutionStatus.PENDING,
        )
        
        # Mark as running
        started_at = datetime.now(timezone.utc)
        self.db_client.update_execution_status(
            execution_id=execution_id,
            status=ExecutionStatus.RUNNING,
            started_at=started_at,
        )
        
        try:
            # Fetch project config from database
            project = self.db_client.get_project(project_id)
            if not project:
                raise ValueError(f"Project not found: {project_id}")
            
            # Fetch rules for this project
            rules = self.db_client.fetch_project_rules(project_id)
            
            if not rules:
                raise ValueError(f"No rules found for project {project_id}")
            
            print(f"Found {len(rules)} rules for project {project_id}")
            
            # Parse data source configuration
            data_source_config = self._parse_data_source_config(project.config)
            
            # Generate project directory
            project_dir = self.work_dir / project_id
            
            # Synthesize the detector code
            self.synthesizer.generate_codebase(
                project_id=project_id,
                rule_set=rules,
                data_source_config=data_source_config,
                output_dir=project_dir,
            )
            
            print(f"Generated detector code at: {project_dir}")
            
            # Build command with arguments
            cmd = [
                sys.executable, "main.py",
                "--start-date", start_date,
                "--end-date", end_date,
            ]
            
            # Add CDN config if configured
            if self.cdn_config:
                cmd.extend(["--cdn-url", self.cdn_config.cdn_url])
                cmd.extend(["--cdn-access-key", self.cdn_config.access_key])
                cmd.extend(["--cdn-secret-key", self.cdn_config.secret_key])
                cmd.extend(["--cdn-bucket", self.cdn_config.bucket_name])
                if not self.cdn_config.enable_ssl:
                    cmd.append("--cdn-no-ssl")
            
            # Add callback URL if configured
            if self.callback_url:
                cmd.extend(["--callback-url", self.callback_url])
            
            # Run the detector script
            print(f"Running detector with date range: {start_date} to {end_date}")
            result = subprocess.run(
                cmd,
                cwd=project_dir,
                capture_output=True,
                text=True,
                timeout=self.timeout,
            )
            
            finished_at = datetime.now(timezone.utc)
            
            # Determine status based on exit code
            if result.returncode == 0:
                status = ExecutionStatus.SUCCESS
                error_message = None
            elif result.returncode == 1:
                # Exit code 1 means discrepancies were found (expected)
                status = ExecutionStatus.SUCCESS
                error_message = None
            else:
                status = ExecutionStatus.FAILED
                error_message = result.stderr or result.stdout
            
            print(f"Execution finished with status: {status.value}")
            if result.stdout:
                print(f"Output:\n{result.stdout}")
            
            # Update execution record
            self.db_client.update_execution_status(
                execution_id=execution_id,
                status=status,
                finished_at=finished_at,
                exit_code=result.returncode,
                error_message=error_message[:1000] if error_message else None,
            )
            
        except subprocess.TimeoutExpired:
            finished_at = datetime.now(timezone.utc)
            print(f"Execution timed out after {self.timeout} seconds")
            
            self.db_client.update_execution_status(
                execution_id=execution_id,
                status=ExecutionStatus.TIMEOUT,
                finished_at=finished_at,
                error_message=f"Execution timed out after {self.timeout} seconds",
            )
            
        except Exception as e:
            finished_at = datetime.now(timezone.utc)
            print(f"Execution failed with error: {e}")
            
            self.db_client.update_execution_status(
                execution_id=execution_id,
                status=ExecutionStatus.FAILED,
                finished_at=finished_at,
                error_message=str(e)[:1000],
            )
        
        return self.db_client.get_execution(execution_id)

    def cleanup_old_projects(self, max_age_hours: int = 24) -> int:
        """
        Clean up old generated project directories.
        
        Args:
            max_age_hours: Maximum age of directories to keep
            
        Returns:
            Number of directories cleaned up
        """
        cleaned = 0
        now = datetime.now()
        
        for project_dir in self.work_dir.iterdir():
            if not project_dir.is_dir():
                continue
            
            # Check directory age
            mtime = datetime.fromtimestamp(project_dir.stat().st_mtime)
            age_hours = (now - mtime).total_seconds() / 3600
            
            if age_hours > max_age_hours:
                try:
                    shutil.rmtree(project_dir)
                    cleaned += 1
                    print(f"Cleaned up old project directory: {project_dir}")
                except Exception as e:
                    print(f"Failed to clean up {project_dir}: {e}")
        
        return cleaned

    def cleanup_project(self, project_id: str) -> bool:
        """
        Clean up the generated directory for a specific project.
        
        Args:
            project_id: The project identifier
            
        Returns:
            True if directory was cleaned up, False if it didn't exist
        """
        project_dir = self.work_dir / project_id
        
        if not project_dir.exists():
            print(f"Project directory does not exist: {project_dir}")
            return False
        
        try:
            shutil.rmtree(project_dir)
            print(f"Cleaned up project directory: {project_dir}")
            return True
        except Exception as e:
            print(f"Failed to clean up {project_dir}: {e}")
            raise RuntimeError(f"Failed to clean up project directory: {e}")

    def project_dir_exists(self, project_id: str) -> bool:
        """
        Check if a project directory exists.
        
        Args:
            project_id: The project identifier
            
        Returns:
            True if directory exists, False otherwise
        """
        return (self.work_dir / project_id).exists()
