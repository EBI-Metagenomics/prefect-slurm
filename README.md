# Prefect-Slurm

**A Prefect worker for running flows on Slurm HPC clusters**

[![Unit Tests](https://github.com/EBI-Metagenomics/prefect-slurm/actions/workflows/test.yaml/badge.svg)](https://github.com/EBI-Metagenomics/prefect-slurm/actions/workflows/test.yaml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://python.org)
[![Prefect](https://img.shields.io/badge/prefect-3.4.13%2B-blue.svg)](https://prefect.io)

Execute your Prefect flows on high-performance computing clusters using the Slurm workload manager. This worker seamlessly integrates with Slurm's REST API to submit, monitor, and manage flow runs as Slurm jobs.

## Features

✨ **Automatic API Version Detection** - Supports Slurm REST API versions 0.0.40-0.0.42 with automatic detection  
🔒 **Secure Token Management** - JWT-based authentication with file locking and proper permissions  
🔄 **Zombie Job Recovery** - Automatically detects and handles orphaned flow runs after worker restarts  
📊 **Resource Management** - Full Slurm job specification support for CPU, memory, and time limits  
🛠️ **CLI Tools** - Built-in utilities for token management and worker administration  
🧪 **Comprehensive Testing** - Both unit and integration tests

## Quick Start

### Installation

```bash
pip install prefect-slurm
```

### Basic Setup

1. **Create a work pool** using the Slurm worker type:
   ```bash
   prefect work-pool create slurm-pool --type slurm
   ```

2. **Configure authentication** - Set up your Slurm credentials:
   ```bash
   export PREFECT_SLURM_USER_NAME=your_username
   export PREFECT_SLURM_API_URL=http://your-slurm-server:6820
   ```

3. **Set up authentication token**:
   ```bash
   # Generate and store token using built-in CLI
   scontrol token username=$USER lifespan=3600 | prefect-slurm token
   
   # Or set token directly via environment variable
   export PREFECT_SLURM_USER_TOKEN=your_jwt_token
   ```

4. **Start the worker**:
   ```bash
   prefect worker start --pool slurm-pool --type slurm
   ```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PREFECT_SLURM_USER_NAME` | Slurm username | **Required** |
| `PREFECT_SLURM_API_URL` | Slurm REST API URL | **Required** |
| `PREFECT_SLURM_USER_TOKEN` | JWT authentication token | Optional |
| `PREFECT_SLURM_TOKEN_FILE` | Path to token file | `~/.prefect_slurm.jwt` |
| `PREFECT_SLURM_LOCK_TIMEOUT` | File lock timeout (seconds) | `60` |
| `PREFECT_SLURM_ENV_FILE` | Override environment file path | Optional |

### Environment Files

The worker supports loading configuration from environment files using a hierarchical discovery system. Files are loaded in priority order (later files override earlier ones):

1. **System-wide**: `/etc/prefect-slurm/.env`
2. **XDG Config**: `~/.config/prefect-slurm/.env` (or `$XDG_CONFIG_HOME/prefect-slurm/.env`)
3. **User Home**: `~/.prefect_slurm.env`
4. **Current Directory (app-specific)**: `./.prefect_slurm.env`  
5. **Current Directory**: `./.env`
6. **Environment Variable Override**: `$PREFECT_SLURM_ENV_FILE`

**Example environment file** (`.prefect_slurm.env`):
```bash
# Slurm connection settings
PREFECT_SLURM_USER_NAME=your_username
PREFECT_SLURM_API_URL=http://your-slurm-server:6820

# Optional token (alternative to token file)
PREFECT_SLURM_USER_TOKEN=your_jwt_token_here

# Optional custom token file location
PREFECT_SLURM_TOKEN_FILE=~/my_custom_token.jwt

# Optional custom lock timeout
PREFECT_SLURM_LOCK_TIMEOUT=120
```

You can override the automatic discovery by setting `PREFECT_SLURM_ENV_FILE` to point to a specific file:
```bash
export PREFECT_SLURM_ENV_FILE=/path/to/my/custom.env
prefect worker start --pool slurm-pool --type slurm
```

**Note**: CLI commands (`prefect-slurm token`) also support environment files, though only `PREFECT_SLURM_TOKEN_FILE` and `PREFECT_SLURM_LOCK_TIMEOUT` are relevant for CLI operations.

### Work Pool Configuration

Configure your Slurm work pool with job specifications:

```yaml
job_configuration:
  partition: "compute"
  cpu: 4
  memory: 8
  time_limit: 2
  working_dir: "/path/to/working/directory"
  source_files:  # Optional - omit for default Python environment
    - "~/.bashrc"
    - "~/envs/conda/bin/activate"
```

### Environment Setup

The worker supports two environment configuration modes:

**Custom Environment** (when `source_files` are specified):
```yaml
job_configuration:
  source_files:
    - "~/.bashrc"
    - "/opt/conda/bin/activate"
    - "/opt/modules/init.sh"
```
The worker will source these files before executing your flow. Use this for conda environments, module systems, or custom shell configurations.

**Default Python Environment** (when `source_files` is empty or omitted):
```yaml
job_configuration:
  partition: "compute"
  cpu: 4
  memory: 8
```
The worker automatically creates a temporary Python virtual environment with the matching Prefect version installed. The environment is created in `$TMPDIR/.venv_$SLURM_JOB_ID` and cleaned up after job completion.

## CLI Tools

The package includes a command-line utility for token management:

```bash
# Store token from scontrol output at default location
scontrol token username=$USER lifespan=3600 | prefect-slurm token

# Store token to custom location
echo "jwt_token_here" | prefect-slurm token ~/my_token.jwt

# Get help
prefect-slurm token --help
```
The default location for the token is `~/.prefect_slurm.jwt` (can be overridden by setting `PREFECT_SLURM_TOKEN_FILE`) and default permissions are 600 (read/write allowed for user only)

## Running the Examples

You can test the examples in the [examples/](examples/) directory using the local Docker Compose Slurm cluster:

1. **Start the local cluster**:
   ```bash
   cd slurm_environment/
   docker-compose up -d
   ```

2. **Wait for services to be healthy** (check with `docker-compose ps`)

3. **Deploy and run example flows** (from the **prefect_server** container):
   ```bash
   # Enter the Prefect server container
   docker-compose exec prefect_server bash
   
   # Navigate to examples and deploy the hello world example interactively
   cd /opt/data/examples
   prefect deploy
   
   # Run the deployment
   prefect deployment run slurm-hello-world/slurm-hello-world-deployment
   ```

4. **Monitor execution**:
   - Prefect UI: http://localhost:4200
   - Check Slurm jobs (from **slurm_node** container): `docker-compose exec slurm_node squeue`
   - View worker logs: `docker-compose logs slurm_submitter`

The Docker environment provides a complete Slurm cluster with the worker automatically configured and example flows ready to deploy.

## Architecture

The Slurm worker integrates with Prefect's execution model:

1. **Worker Polling** - Continuously polls Prefect API for scheduled flow runs
2. **Job Submission** - Converts flow runs to Slurm job specifications
3. **Execution** - Submits jobs via Slurm REST API with proper resource allocation
4. **Monitoring** - Tracks job status and reports back to Prefect
5. **Cleanup** - Handles zombie jobs and ensures proper flow run state management

```mermaid
graph TB
    A[Prefect Server] -->|polls for flows| B[Slurm Worker]
    B -->|submits jobs| C[Slurm REST API]
    C -->|schedules| D[Slurm Cluster]
    D -->|executes| E[Flow Run]
    E -->|reports status| B
    B -->|updates state| A
```

## Requirements

- **Python**: 3.11+ (< 3.14)
- **Prefect**: 3.4.13+
- **Slurm**: Cluster with REST API enabled (versions 0.0.40-0.0.42 supported)
- **Network**: Access from worker node to both Prefect API and Slurm REST API

## Development

### Running Tests

```bash
# Unit tests only
pytest -m unit

# Integration tests (requires Docker)
pytest -m integration

# CLI tests
pytest -m cli

# All tests
pytest
```

### Test Environment

The project includes Docker-based Slurm cluster for integration testing:

```bash
cd slurm_environment/
docker-compose up -d
```

## Contributing

Contributions are welcome! This project is developed by the [EBI Metagenomics](https://www.ebi.ac.uk/metagenomics/) team.

### Development Workflow

1. Fork the repository
2. Create a feature branch
3. Make your changes with tests
4. Run the full test suite
5. Submit a pull request

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

## Support

- **Issues**: Report bugs and request features via [GitHub Issues](https://github.com/EBI-metagenomics/prefect-slurm/issues)
- **Documentation**: See [tests/README.md](tests/README.md) for detailed testing information