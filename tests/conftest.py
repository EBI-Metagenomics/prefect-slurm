"""
Pytest configuration and shared fixtures for prefect-slurm tests.
"""

from pathlib import Path
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from prefect.client.schemas import FlowRun
from prefect.states import Running

from prefect_slurm.config import SlurmWorkerConfiguration, SlurmWorkerTemplateVariables
from prefect_slurm.worker import SlurmWorker


@pytest.fixture
def sample_flow_run():
    """Create a sample FlowRun for testing."""
    flow_id = uuid4()
    return FlowRun(
        id=flow_id,
        flow_id=flow_id,
        name="test-flow-run",
        state=Running(),
        infrastructure_pid="12345",
    )


@pytest.fixture
def sample_flow_runs():
    """Create multiple sample FlowRuns for testing (no zombies - all PIDs match running jobs)."""
    flows = []
    for i, pid in enumerate(["12345", "67890"]):
        flow_id = uuid4()
        flows.append(
            FlowRun(
                id=flow_id,
                flow_id=flow_id,
                name=f"test-flow-run-{i + 1}",
                state=Running(),
                infrastructure_pid=pid,
            )
        )
    return flows


@pytest.fixture
def sample_slurm_configuration():
    """Create a sample SlurmWorkerConfiguration."""
    return SlurmWorkerConfiguration(
        cpu=2,
        memory=4,
        partition="compute",
        time_limit=1,
        working_dir=Path("/tmp/test"),
        source_files=[Path("/etc/profile")],
    )


@pytest.fixture
def minimal_slurm_configuration():
    """Create a minimal SlurmWorkerConfiguration."""
    return SlurmWorkerConfiguration(
        working_dir=Path("/tmp/test"),
    )


@pytest.fixture
def sample_template_variables():
    """Create sample SlurmWorkerTemplateVariables."""
    return SlurmWorkerTemplateVariables(
        cpu=4,
        memory=8,
        partition="gpu",
        time_limit=2,
        working_dir=Path("/opt/data"),
        source_files=[Path("/home/user/.bashrc")],
    )


@pytest.fixture
def sample_slurm_jobs():
    """Create sample Slurm job states dict for testing (RUNNING jobs only for zombie detection)."""
    return {"12345": "RUNNING", "67890": "RUNNING"}


@pytest.fixture
def sample_all_slurm_jobs():
    """Create sample Slurm job states dict including all states for comprehensive testing."""
    return {"12345": "RUNNING", "67890": "RUNNING", "99999": "COMPLETED"}


@pytest.fixture
def sample_job_spec():
    """Create a sample Slurm job specification."""
    return {
        "job": {
            "script": "#!/bin/bash\necho 'Hello World'",
            "cpus_per_task": 2,
            "memory_per_node": {"set": True, "number": 4096},
            "current_working_directory": "/tmp/test",
            "time_limit": {"set": True, "number": 60},
            "environment": ["PATH=/bin:/usr/bin"],
        }
    }


@pytest.fixture
def sample_env_vars():
    """Create sample environment variables for testing."""
    return {
        "PREFECT_SLURM_API_URL": "http://test-slurm:6820",
        "PREFECT_SLURM_USER_NAME": "testuser",
        "PREFECT_SLURM_USER_TOKEN": "test-token-123",
        "PATH": "/usr/bin:/bin",
    }


@pytest.fixture
def mock_slurm_worker():
    """Create a mock SlurmWorker for testing."""
    worker = Mock(spec=SlurmWorker)
    worker.work_pool = Mock()
    worker.work_pool.name = "test-pool"
    worker._work_queues = {"default"}
    worker.client = AsyncMock()
    worker._logger = Mock()
    return worker


@pytest.fixture(autouse=True)
def setup_test_environment(sample_env_vars, monkeypatch):
    """Set up test environment variables."""
    for key, value in sample_env_vars.items():
        monkeypatch.setenv(key, value)


@pytest.fixture
def mock_slurpy_response():
    """Create a mock response from Slurm API."""
    response = Mock()
    response.job_id = 12345
    response.jobs = []
    return response


@pytest.fixture
def mock_file_operations():
    """Create mock objects for file operations (stat and aiofiles.open).

    Returns a tuple of (mock_stat, mock_file) configured for testing
    async file operations with proper permissions and context managers.
    """

    def _create_mocks(file_content="default_jwt_content", file_permissions=0o100600):
        mock_stat = Mock()
        mock_stat.st_mode = file_permissions

        mock_file = Mock()
        mock_file.fileno.return_value = 123
        mock_file.__aenter__ = AsyncMock(return_value=mock_file)
        mock_file.__aexit__ = AsyncMock(return_value=None)
        mock_file.read = AsyncMock(return_value=file_content)

        return mock_stat, mock_file

    return _create_mocks
