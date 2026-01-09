# 🦗 Cricket Runner Manager

A cron-based scheduler that dynamically synthesizes and executes discrepancy detection projects. Cricket acts as a **meta-scheduler** that reads project configurations and rules from a database, generates executable detector code on-the-fly using Jinja2 templates, and runs them according to their cron schedules.

---

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Capabilities](#capabilities)
- [Limitations](#limitations)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Configuration](#configuration)
  - [Running the Service](#running-the-service)
- [API Reference](#api-reference)
- [Database Schema](#database-schema)
- [Dependencies](#dependencies)
- [Docker Deployment](#docker-deployment)
- [Development](#development)

---

## Overview

Cricket is a **discrepancy detection orchestrator** designed to:

1. **Fetch** project configurations and rules from a PostgreSQL database
2. **Schedule** project executions using cron expressions with timezone support
3. **Synthesize** executable Python detector code from templates and rule definitions
4. **Execute** generated detectors as isolated subprocess runs
5. **Report** discrepancy findings to a configured endpoint (Enliq)
6. **Monitor** execution status, history, and statistics via REST API

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Cricket Runner Manager                        │
├──────────────────┬────────────────────┬─────────────────────────────┤
│    Scheduler     │      Executor      │         API                 │
│  (Cron Queue)    │ (Code Synthesizer) │    (FastAPI/Swagger)        │
├──────────────────┼────────────────────┼─────────────────────────────┤
│ - Priority Queue │ - Template Render  │ - Health Check              │
│ - Timezone Aware │ - Dependency Scan  │ - Queue Status              │
│ - Refresh Loop   │ - Subprocess Run   │ - Execution History         │
└──────────────────┴────────────────────┴─────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │   PostgreSQL    │
                    │   Database      │
                    └─────────────────┘
```

---

## Architecture

### Core Components

| Component | Description |
|-----------|-------------|
| **ProjectScheduler** | Manages a priority queue of projects, calculates next run times using `croniter`, and triggers executions |
| **ProjectExecutor** | Synthesizes detector code, runs child processes, records results |
| **CodeSynthesizer** | Generates complete Python projects from Jinja2 templates and rule definitions |
| **DatabaseClient** | Handles all PostgreSQL operations for projects, rules, schedules, and executions |
| **Monitoring API** | FastAPI-based REST API for health checks, status monitoring, and manual operations |

### Data Flow

1. **Startup**: Scheduler loads active projects from database into a min-heap priority queue
2. **Scheduling**: Every `check_interval` seconds, scheduler checks if the next project is due
3. **Execution**:
   - Fetch discrepancy rules for the project
   - Generate detector code using Jinja2 templates
   - Run the generated project via `uv run python main.py`
   - Capture exit code, stdout, stderr
   - Record execution result in database
4. **Rescheduling**: After execution, calculate next run time and re-add to queue

---

## Features

- ✅ **Cron-based Scheduling** - Full cron expression support with timezone awareness
- ✅ **Dynamic Code Generation** - Synthesize Python detector projects from templates
- ✅ **Dependency Detection** - Automatically extract and install required packages
- ✅ **Multiple Data Sources** - SQL databases, REST APIs, or manual data upload
- ✅ **Execution Isolation** - Each detector runs as an isolated subprocess
- ✅ **Concurrent Execution Control** - Configurable per-project concurrency limits
- ✅ **Execution History** - Full audit trail of all executions with timing and status
- ✅ **RESTful Monitoring API** - Swagger-documented endpoints for operations
- ✅ **Health Checks** - Built-in health endpoint for container orchestration
- ✅ **Graceful Shutdown** - Proper signal handling for clean termination

---

## Capabilities

### Scheduling Capabilities

| Feature | Description |
|---------|-------------|
| Cron Expressions | Standard 5-field cron syntax (minute, hour, day, month, weekday) |
| Timezone Support | Projects can specify their timezone (e.g., `Europe/Istanbul`) |
| Priority Queue | Up to `MAX_QUEUE_SIZE` projects maintained in memory |
| Dynamic Refresh | Projects can be reloaded from database without restart |

### Execution Capabilities

| Feature | Description |
|---------|-------------|
| Subprocess Isolation | Each execution runs in a separate process |
| Timeout Control | Configurable maximum execution time per run |
| Date Range Calculation | Automatic lookback period for data retrieval |
| Report Forwarding | Send discrepancy reports to external endpoint |
| Concurrent Blocking | Optional per-project concurrent execution prevention |

### Data Source Support

| Type | Description |
|------|-------------|
| **SQL** | Connect to PostgreSQL, MySQL, etc. with parameterized queries |
| **API** | Fetch data from REST endpoints with pagination and auth |
| **Manual** | Upload data directly via API for ad-hoc analysis |

### Monitoring Capabilities

| Endpoint | Description |
|----------|-------------|
| Health Check | Service liveness and queue status |
| Queue Status | View pending scheduled projects |
| Execution History | Query past executions with filtering |
| Statistics | Aggregate success/failure rates |

---

## Limitations

### Scheduling Limitations

| Limitation | Details |
|------------|---------|
| Queue Size | Maximum `MAX_QUEUE_SIZE` (default: 10) active projects in memory |
| Check Interval | Minimum scheduling precision is `SCHEDULER_CHECK_INTERVAL` seconds |
| Single Instance | No distributed scheduling - run only one instance |

### Execution Limitations

| Limitation | Details |
|------------|---------|
| Timeout | Maximum `EXECUTION_TIMEOUT` seconds (default: 600) per execution |
| Sequential | Only one project executes at a time (queue-based) |
| Resource | No CPU/memory limits enforced on child processes |
| Python Only | Generated detectors are Python-only |

### Data Source Limitations

| Limitation | Details |
|------------|---------|
| SQL | Single query only, no transaction support |
| API | Basic pagination, limited authentication options |
| Manual | Data must be provided at execution time |

### General Limitations

| Limitation | Details |
|------------|---------|
| Database | PostgreSQL only (uses `psycopg`) |
| Restart | Active execution interrupted on restart |
| Persistence | Queue state is not persisted, rebuilt on startup |

---

## Getting Started

### Prerequisites

- **Python** 3.12+
- **PostgreSQL** database
- **uv** package manager ([astral-sh/uv](https://github.com/astral-sh/uv))

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd loodos-arge-normx-cricket

# Install dependencies using uv
uv sync
```

### Configuration

Create a `.env` file in the project root:

```env
# Database connection
DATABASE_URL=postgresql://user:password@localhost:5432/cricket

# Report destination (optional)
ENLIQ_REPORT_URL=https://api.enliq.io/v1/reports

# Scheduler settings
MAX_QUEUE_SIZE=10
SCHEDULER_CHECK_INTERVAL=60.0

# Executor settings
EXECUTION_TIMEOUT=600
WORK_DIR=/tmp/cricket-projects

# API settings
API_HOST=0.0.0.0
API_PORT=8080
```

#### Environment Variables Reference

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `DATABASE_URL` | string | `postgresql://localhost:5432/cricket` | PostgreSQL connection string |
| `ENLIQ_REPORT_URL` | string | `""` | URL to send discrepancy reports to |
| `MAX_QUEUE_SIZE` | int | `10` | Maximum projects in scheduler queue |
| `SCHEDULER_CHECK_INTERVAL` | float | `60.0` | Seconds between queue checks |
| `EXECUTION_TIMEOUT` | int | `600` | Maximum seconds per execution |
| `WORK_DIR` | string | System temp | Directory for generated projects |
| `API_HOST` | string | `0.0.0.0` | API bind host |
| `API_PORT` | int | `8080` | API bind port |

### Running the Service

```bash
# Using the CLI entry point
uv run cricket

# Or directly with uvicorn
uv run uvicorn main:app --host 0.0.0.0 --port 8080

# Development with auto-reload
uv run uvicorn main:app --reload
```

Access the API documentation at:
- **Swagger UI**: http://localhost:8080/docs
- **ReDoc**: http://localhost:8080/redoc
- **OpenAPI JSON**: http://localhost:8080/openapi.json

---

## API Reference

### Endpoints Overview

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check with queue status |
| `GET` | `/status` | Overall runner status |
| `GET` | `/queue` | Current scheduling queue |
| `GET` | `/projects/{project_id}` | Project status and next run |
| `GET` | `/projects/{project_id}/executions` | Execution history for project |
| `POST` | `/projects/refresh` | Refresh projects from database |
| `DELETE` | `/projects/{project_id}/cleanup` | Remove generated project files |
| `GET` | `/executions/{execution_id}` | Specific execution details |
| `GET` | `/stats` | Execution statistics |

### Health Check

```bash
curl http://localhost:8080/health
```

Response:
```json
{
  "status": "healthy",
  "runner_active": true,
  "projects_in_queue": 5,
  "currently_executing": null,
  "last_check": "2026-01-09T10:30:00Z"
}
```

### Get Queue Status

```bash
curl http://localhost:8080/queue
```

Response:
```json
[
  {
    "project_id": "order-validation",
    "project_name": "Order Validation Rules",
    "next_run": "2026-01-09T11:00:00Z",
    "cron_expression": "0 * * * *",
    "timezone": "UTC"
  }
]
```

### Get Execution Statistics

```bash
curl http://localhost:8080/stats
```

Response:
```json
{
  "total": 1250,
  "pending": 2,
  "running": 1,
  "success": 1180,
  "failed": 45,
  "cancelled": 12,
  "timeout": 10,
  "success_rate": 94.40
}
```

---

## Database Schema

Cricket expects the following tables in the PostgreSQL database:

### `projects`

| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR(PK) | Unique project identifier |
| `name` | VARCHAR | Human-readable name |
| `config` | JSONB | Project configuration (data_source, etc.) |
| `is_active` | BOOLEAN | Whether project is active for scheduling |

### `project_schedules`

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL(PK) | Primary key |
| `project_id` | VARCHAR(FK) | Reference to projects.id |
| `cron_expression` | VARCHAR | Cron schedule (e.g., `0 * * * *`) |
| `timezone` | VARCHAR | IANA timezone (e.g., `Europe/Istanbul`) |
| `allow_concurrent` | BOOLEAN | Allow overlapping executions |

### `discrepancy_rules`

| Column | Type | Description |
|--------|------|-------------|
| `rule_id` | VARCHAR(PK) | Unique rule identifier |
| `project_id` | VARCHAR(FK) | Reference to projects.id |
| `definition_id` | INTEGER | Rule definition version |
| `description` | TEXT | Human-readable description |
| `category` | VARCHAR | Attention framework category |
| `severity` | VARCHAR | info/low/medium/high/critical |
| `logic` | TEXT | Natural language logic description |
| `code` | TEXT | Python function code |
| `explanation` | TEXT | Code explanation |
| `parameters` | JSONB | Configurable parameters |
| `dependencies` | JSONB | Required Python packages |

### `project_executions`

| Column | Type | Description |
|--------|------|-------------|
| `id` | SERIAL(PK) | Primary key |
| `project_id` | VARCHAR(FK) | Reference to projects.id |
| `status` | VARCHAR | pending/running/success/failed/cancelled/timeout |
| `scheduled_for` | TIMESTAMP | When execution was scheduled |
| `started_at` | TIMESTAMP | Actual start time |
| `finished_at` | TIMESTAMP | Completion time |
| `exit_code` | INTEGER | Process exit code |
| `error_message` | TEXT | Error details if failed |
| `created_at` | TIMESTAMP | Record creation time |

---

## Dependencies

### Runtime Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `fastapi` | >=0.109.0 | REST API framework |
| `uvicorn` | >=0.27.0 | ASGI server |
| `pydantic` | >=2.12.5 | Data validation |
| `pydantic-settings` | >=2.12.0 | Settings management |
| `psycopg[binary]` | >=3.3.2 | PostgreSQL adapter |
| `croniter` | >=2.0.0 | Cron expression parsing |
| `pytz` | >=2024.1 | Timezone handling |
| `jinja2` | >=3.1.6 | Template engine |
| `polars` | >=1.36.1 | Data processing |
| `httpx` | >=0.28.1 | HTTP client |
| `python-dotenv` | >=1.2.1 | Environment file loading |

### Child Project Dependencies

Generated detector projects automatically include:

| Package | Purpose |
|---------|---------|
| `polars` | DataFrame operations |
| `connectorx` | Fast SQL data loading |
| `httpx` | HTTP requests |
| `pydantic` | Data models |
| `python-dotenv` | Configuration |

Additional dependencies are detected from rule code imports.

---

## Docker Deployment

### Build Image

```bash
docker build -t cricket-runner:latest .
```

### Run Container

```bash
docker run -d \
  --name cricket \
  -p 8080:8080 \
  -e DATABASE_URL=postgresql://user:pass@host:5432/cricket \
  -e ENLIQ_REPORT_URL=https://api.enliq.io/v1/reports \
  cricket-runner:latest
```

### Docker Compose Example

```yaml
version: '3.8'

services:
  cricket:
    build: .
    ports:
      - "8080:8080"
    environment:
      DATABASE_URL: postgresql://postgres:postgres@db:5432/cricket
      ENLIQ_REPORT_URL: https://api.enliq.io/v1/reports
      MAX_QUEUE_SIZE: 10
      EXECUTION_TIMEOUT: 600
    depends_on:
      - db
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  db:
    image: postgres:15
    environment:
      POSTGRES_DB: cricket
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

---

## Development

### Project Structure

```
loodos-arge-normx-cricket/
├── main.py                 # Application entry point
├── config.py               # Settings configuration
├── synthesizer.py          # Code generation engine
├── geppetto/
│   ├── api.py              # FastAPI monitoring endpoints
│   ├── executor.py         # Project execution logic
│   ├── scheduler.py        # Cron scheduling
│   ├── data/
│   │   └── models/
│   │       ├── data_source.py  # Data source configs
│   │       ├── execution.py    # Execution models
│   │       └── rule.py         # Discrepancy rules
│   └── db/
│       └── client.py       # PostgreSQL client
└── templates/
    └── child_app/          # Jinja2 templates for generated projects
        ├── main.py.j2
        ├── config.py.j2
        ├── pyproject.toml.j2
        └── logic/
            ├── detectors.py.j2
            └── processor.py.j2
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=geppetto
```

### Adding New Data Source Types

1. Add a new config model in `geppetto/data/models/data_source.py`
2. Update the union type `DataSourceConfig`
3. Add parsing logic in `executor.py`
4. Update templates in `templates/child_app/`

---

## License

Proprietary - Loodos R&D

---

## Support

For issues and feature requests, contact the Loodos ARGE team.
