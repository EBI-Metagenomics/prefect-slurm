# Tests

This directory contains unit and integration tests for the Prefect Slurm worker.

## Test Types

### Unit Tests (`@pytest.mark.unit`)
- **test_config.py** - Tests for `SlurmWorkerConfiguration` and `SlurmWorkerTemplateVariables`
- **test_worker.py** - Tests for `SlurmWorker` class methods with mocking

### Integration Tests (`@pytest.mark.integration`)  
- **integration/test_slurm_integration.py** - End-to-end tests using real Docker containers

## Prerequisites

- Docker and Docker Compose installed
- `uv` for dependency management
- Port 4200, 6820, 3306, 5433 available on localhost (for integration tests)

## Dependencies

Install all dependencies:

```bash
uv sync --frozen
```

This installs:
- `pytest` - Test framework
- `pytest-asyncio` - Async test support
- `testcontainers` - Docker container management for integration tests
- `httpx` - HTTP client for API tests

## Running Tests

### Unit Tests Only
```bash
uv run pytest -m unit
```

### Integration Tests Only  
```bash
uv run pytest -m integration
```

### All Tests (excluding integration)
```bash
uv run pytest -m "not integration"
```

### All Tests (including integration)
```bash
uv run pytest
```

### Specific Test
```bash
uv run pytest tests/test_worker.py::TestSlurmWorker::test_zombie_detection_edge_cases -v
```

### With Output (see prints)
```bash
uv run pytest -s tests/integration/test_slurm_integration.py::test_slurm_integration_happy_path -v
```

## Test Structure

```
tests/
├── README.md                 # This file
├── conftest.py              # Shared fixtures (mock_file_operations, etc.)
├── test_config.py           # Unit tests for configuration classes
├── test_worker.py           # Unit tests for SlurmWorker class
└── integration/
    ├── conftest.py          # Integration fixtures (docker_compose_setup, tokens)
    ├── test_slurm_integration.py  # Integration tests
    └── utils.py             # Integration test utilities
```

## Unit Tests

### Configuration Tests (`test_config.py`)
- Job specification generation
- Script segment creation (shebang, source files, venv setup)
- Environment variable handling  
- Resource parameter validation
- Template variable processing

### Worker Tests (`test_worker.py`)
- Slurm configuration generation from environment
- Zombie flow detection logic
- JWT token file reading with file locking
- Credentials validation
- Job submission and execution workflow

### Test Fixtures
- **mock_file_operations** - Reusable fixture for mocking async file operations (stat, aiofiles.open)
- **sample_flow_runs** - Mock FlowRun objects for testing
- **sample_slurm_jobs** - Mock Slurm job objects for zombie detection tests

## Integration Tests

Integration tests use real Docker containers to test complete workflows.

### Services Started
- **slurm_db** - MariaDB database for Slurm
- **slurm_node** - Slurm controller/worker/REST API (port 6820)  
- **slurm_submitter** - Prefect worker container
- **prefect_db** - PostgreSQL database for Prefect
- **prefect_server** - Prefect API server (port 4200)

### Test Cases

#### 1. Happy Path (`test_slurm_integration_happy_path`)
Complete end-to-end flow execution:
1. Starts Docker Compose stack with all services
2. Creates work pool matching worker configuration
3. Deploys hello_world.py flow from examples/
4. Submits flow run and waits for completion
5. Validates successful execution

#### 2. Zombie Flow - Killed Job (`test_zombie_flow_killed_slurm_job`)
Tests zombie detection when Slurm job is terminated externally:
1. Starts long_running.py flow (24-hour loop)
2. Waits for flow to be scheduled and get Slurm job ID  
3. Kills Slurm job using `scancel` command
4. Waits for worker to detect zombie and crash flow
5. Validates final state is "Crashed"

#### 3. Zombie Flow - Bad Source File (`test_zombie_flow_bad_source_file`)
Tests failure handling for configuration errors:
1. Creates deployment with non-existent source file
2. Starts flow run
3. Waits for flow to fail during script execution
4. Validates final state is "Crashed"

#### 4. Connectivity Tests
- **test_slurm_api_connectivity** - Verifies Slurm REST API accessibility
- **test_prefect_api_connectivity** - Verifies Prefect server health
- **test_slurm_worker_credentials_validation** - Tests JWT authentication

### Authentication
Tests use **file-based JWT token authentication**:
- Tokens generated dynamically using `scontrol token username=slurm lifespan=infinite`
- Written to temporary files with 600 permissions  
- Worker configured via `PREFECT_SLURM_TOKEN_FILE` environment variable

### Utilities (`integration/utils.py`)
- **create_work_pool()** - Creates Slurm work pools via API
- **wait_for_flow_scheduled()** - Waits for worker to pick up flow
- **wait_for_flow_completed()** - Waits for successful completion
- **wait_for_flow_crashed()** - Waits for flow to crash (zombie detection)
- **kill_slurm_job()** - Kills Slurm jobs for zombie testing
- **get_slurm_job_id()** - Extracts Slurm job ID from flow runs

## Timeouts

- Service startup: 180 seconds (3 minutes)
- Flow execution: 300 seconds (5 minutes)
- Flow scheduling: 60 seconds
- Zombie detection: 120 seconds
- API calls: 10-30 seconds

## Troubleshooting

### Port Conflicts
If ports are in use, tests will fail. Ensure these ports are available:
- 4200 (Prefect server)
- 6820 (Slurm REST API)
- 3306 (MariaDB) 
- 5433 (PostgreSQL)

### Docker Issues
Integration tests require Docker Compose. If you get "Docker command not found":
- Install Docker Desktop or Docker CLI
- Ensure `docker-compose` command is in PATH

### Container Logs
If integration tests fail, check container logs:
```bash
docker-compose -f slurm_environment/docker-compose.yaml logs slurm_node
docker-compose -f slurm_environment/docker-compose.yaml logs prefect_server
docker-compose -f slurm_environment/docker-compose.yaml logs slurm_submitter
```

### Slow Startup
First run may be slow as Docker images are built/pulled. Subsequent runs are faster.

### Token Issues
JWT tokens are generated fresh for each test session. If generation fails:
- Check that Slurm container is healthy
- Verify `scontrol` command works: `docker exec slurm_node scontrol token username=slurm`
- Ensure user 'slurm' exists in container

### Test Isolation
- Integration tests use session-scoped fixtures for performance
- Each test gets fresh work pools and deployments
- Environment variables are properly cleaned up
- Containers are reused across tests in same session