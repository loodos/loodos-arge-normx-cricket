"""
Database client for the runner manager.
Handles all database operations for projects, schedules, rules, and executions.
"""
import json
from datetime import datetime
from typing import Any, Dict, List, Optional

import psycopg

from geppetto.data.models.execution import (
    ExecutionStatus,
    ProjectConfig,
    ProjectExecution,
)
from geppetto.data.models.rule import DiscrepancyRule, Severity


class DatabaseClient:
    """Database client for runner manager operations."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def _get_connection(self) -> psycopg.Connection:
        """Create a new database connection."""
        return psycopg.connect(self.connection_string)

    def fetch_active_projects(self, limit: int = 10) -> List[ProjectConfig]:
        """
        Fetch active projects with their schedule configurations.
        
        Args:
            limit: Maximum number of projects to fetch
            
        Returns:
            List of ProjectConfig objects
        """
        query = """
            SELECT
                p.id,
                p.name,
                p.config,
                s.cron_expression,
                s.timezone,
                s.allow_concurrent
            FROM projects p
            JOIN project_schedules s
                ON s.project_id = p.id
            WHERE p.is_active = TRUE
            LIMIT %s
        """
        
        projects = []
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                rows = cur.fetchall()
                
                for row in rows:
                    config = row[2]
                    if isinstance(config, str):
                        config = json.loads(config)
                    
                    projects.append(ProjectConfig(
                        id=row[0],
                        name=row[1],
                        config=config,
                        cron_expression=row[3],
                        timezone=row[4],
                        allow_concurrent=row[5],
                    ))
        
        return projects

    def get_project(self, project_id: str) -> Optional[ProjectConfig]:
        """
        Fetch a single project by ID.
        
        Args:
            project_id: The project identifier
            
        Returns:
            ProjectConfig if found, None otherwise
        """
        query = """
            SELECT
                p.id,
                p.name,
                p.config,
                s.cron_expression,
                s.timezone,
                s.allow_concurrent
            FROM projects p
            JOIN project_schedules s
                ON s.project_id = p.id
            WHERE p.id = %s
        """
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (project_id,))
                row = cur.fetchone()
                
                if not row:
                    return None
                
                config = row[2]
                if isinstance(config, str):
                    config = json.loads(config)
                
                return ProjectConfig(
                    id=row[0],
                    name=row[1],
                    config=config,
                    cron_expression=row[3],
                    timezone=row[4],
                    allow_concurrent=row[5],
                )

    def fetch_project_rules(self, project_id: str) -> List[DiscrepancyRule]:
        """
        Fetch all discrepancy rules for a specific project.
        
        Args:
            project_id: The project identifier
            
        Returns:
            List of DiscrepancyRule objects
        """
        query = """
            SELECT
                rule_id,
                definition_id,
                description,
                category,
                severity,
                logic,
                code,
                explanation,
                parameters,
                dependencies
            FROM discrepancy_rules
            WHERE project_id = %s
        """
        
        rules = []
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (project_id,))
                rows = cur.fetchall()
                
                for row in rows:
                    parameters = row[8]
                    if isinstance(parameters, str):
                        parameters = json.loads(parameters)
                    
                    dependencies = row[9]
                    if isinstance(dependencies, str):
                        dependencies = json.loads(dependencies)
                    
                    rules.append(DiscrepancyRule(
                        rule_id=row[0],
                        definition_id=row[1],
                        description=row[2],
                        category=row[3],
                        severity=Severity(row[4]),
                        logic=row[5],
                        code=row[6],
                        explanation=row[7],
                        parameters=parameters or {},
                        dependencies=dependencies or [],
                    ))
        
        return rules

    def create_execution(
        self,
        project_id: str,
        scheduled_for: datetime,
        status: ExecutionStatus = ExecutionStatus.PENDING,
    ) -> int:
        """
        Create a new execution record.
        
        Args:
            project_id: The project identifier
            scheduled_for: When this execution was scheduled
            status: Initial status (default: PENDING)
            
        Returns:
            The ID of the created execution record
        """
        query = """
            INSERT INTO project_executions (project_id, status, scheduled_for)
            VALUES (%s, %s, %s)
            RETURNING id
        """
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (project_id, status.value, scheduled_for))
                result = cur.fetchone()
                conn.commit()
                return result[0]

    def update_execution_status(
        self,
        execution_id: int,
        status: ExecutionStatus,
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
        exit_code: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        """
        Update an execution record's status and metadata.
        
        Args:
            execution_id: The execution record ID
            status: New status
            started_at: When execution started
            finished_at: When execution finished
            exit_code: Process exit code
            error_message: Error message if failed
        """
        updates = ["status = %s"]
        params: List[Any] = [status.value]
        
        if started_at is not None:
            updates.append("started_at = %s")
            params.append(started_at)
        
        if finished_at is not None:
            updates.append("finished_at = %s")
            params.append(finished_at)
        
        if exit_code is not None:
            updates.append("exit_code = %s")
            params.append(exit_code)
        
        if error_message is not None:
            updates.append("error_message = %s")
            params.append(error_message)
        
        params.append(execution_id)
        
        query = f"""
            UPDATE project_executions
            SET {', '.join(updates)}
            WHERE id = %s
        """
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                conn.commit()

    def get_execution(self, execution_id: int) -> Optional[ProjectExecution]:
        """
        Get a specific execution record.
        
        Args:
            execution_id: The execution record ID
            
        Returns:
            ProjectExecution if found, None otherwise
        """
        query = """
            SELECT
                id, project_id, status, scheduled_for,
                started_at, finished_at, exit_code, error_message, created_at
            FROM project_executions
            WHERE id = %s
        """
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (execution_id,))
                row = cur.fetchone()
                
                if not row:
                    return None
                
                return ProjectExecution(
                    id=row[0],
                    project_id=row[1],
                    status=ExecutionStatus(row[2]),
                    scheduled_for=row[3],
                    started_at=row[4],
                    finished_at=row[5],
                    exit_code=row[6],
                    error_message=row[7],
                    created_at=row[8],
                )

    def get_project_executions(
        self,
        project_id: str,
        limit: int = 50,
    ) -> List[ProjectExecution]:
        """
        Get execution history for a project.
        
        Args:
            project_id: The project identifier
            limit: Maximum number of records to return
            
        Returns:
            List of ProjectExecution objects, most recent first
        """
        query = """
            SELECT
                id, project_id, status, scheduled_for,
                started_at, finished_at, exit_code, error_message, created_at
            FROM project_executions
            WHERE project_id = %s
            ORDER BY created_at DESC
            LIMIT %s
        """
        
        executions = []
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (project_id, limit))
                rows = cur.fetchall()
                
                for row in rows:
                    executions.append(ProjectExecution(
                        id=row[0],
                        project_id=row[1],
                        status=ExecutionStatus(row[2]),
                        scheduled_for=row[3],
                        started_at=row[4],
                        finished_at=row[5],
                        exit_code=row[6],
                        error_message=row[7],
                        created_at=row[8],
                    ))
        
        return executions

    def get_running_execution(self, project_id: str) -> Optional[ProjectExecution]:
        """
        Check if a project has a currently running execution.
        
        Args:
            project_id: The project identifier
            
        Returns:
            ProjectExecution if running, None otherwise
        """
        query = """
            SELECT
                id, project_id, status, scheduled_for,
                started_at, finished_at, exit_code, error_message, created_at
            FROM project_executions
            WHERE project_id = %s AND status = 'running'
            LIMIT 1
        """
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (project_id,))
                row = cur.fetchone()
                
                if not row:
                    return None
                
                return ProjectExecution(
                    id=row[0],
                    project_id=row[1],
                    status=ExecutionStatus(row[2]),
                    scheduled_for=row[3],
                    started_at=row[4],
                    finished_at=row[5],
                    exit_code=row[6],
                    error_message=row[7],
                    created_at=row[8],
                )

    def get_execution_stats(self) -> Dict[str, int]:
        """
        Get overall execution statistics.
        
        Returns:
            Dictionary with execution counts by status
        """
        query = """
            SELECT status, COUNT(*) as count
            FROM project_executions
            GROUP BY status
        """
        
        stats = {status.value: 0 for status in ExecutionStatus}
        
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                
                for row in rows:
                    stats[row[0]] = row[1]
        
        return stats
