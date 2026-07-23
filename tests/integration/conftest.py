"""
Pytest configuration and fixtures for integration tests.
"""

import os
import tempfile
from typing import AsyncGenerator, Dict, Generator
import shutil

import pytest
from testcontainers.compose import DockerCompose

from .utils import (
    create_token_file,
    extract_jwt_from_scontrol_output,
    get_docker_compose_path,
)

# Use default event loop from pytest-asyncio


@pytest.fixture(scope="session")
def docker_compose_setup() -> Generator[DockerCompose, None, None]:
    """
    Start Docker Compose services for integration testing.

    This fixture starts the entire stack:
    - slurm_db (MariaDB)
    - slurm_node (Slurm controller + worker + REST API)
    - slurm_submitter (Prefect worker)
    - prefect_db (PostgreSQL)
    - prefect_server (Prefect API server)
    """
    compose_path = get_docker_compose_path()

    docker_path = shutil.which("docker")

    if not docker_path:
        raise RuntimeError(
            "Docker command not found - install Docker to run integration tests"
        )

    # Always start fresh containers for integration tests
    print("Starting fresh Docker containers for integration test")
    with DockerCompose(
        context=str(compose_path.parent),
        compose_file_name=compose_path.name,
        pull=False,
        wait=True,
        docker_command_path=docker_path,
    ) as compose:
        print("Waiting for services to start up...")
        yield compose


@pytest.fixture(scope="session")
async def services_ready(docker_compose_setup: DockerCompose) -> Dict[str, str]:
    """
    Wait for all services to be ready and return service URLs.

    :returns: Service URLs for Prefect and Slurm APIs
    :rtype: dict
    """
    prefect_url = "http://localhost:4200/api"
    slurm_url = "http://localhost:6820"

    return {"prefect_url": prefect_url, "slurm_url": slurm_url}


@pytest.fixture(scope="session")
async def slurm_token(docker_compose_setup: DockerCompose) -> AsyncGenerator[str, None]:
    """
    Generate a Slurm JWT token using scontrol and create a token file.

    :returns: Path to the created token file
    :rtype: str
    """
    import subprocess

    # Generate token using docker exec (since get_service API changed)
    result = subprocess.run(
        [
            "docker",
            "exec",
            "-u",
            "slurm",
            "slurm_node",
            "scontrol",
            "token",
            "username=slurm",
            "lifespan=infinite",
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Failed to generate Slurm token: {result.stderr}")

    # Extract JWT from output
    output = result.stdout
    token = extract_jwt_from_scontrol_output(output)

    if not token:
        raise RuntimeError(f"Could not extract JWT from scontrol output: {output}")

    # Create token file with proper permissions
    token_file = create_token_file(token)

    try:
        yield token_file
    finally:
        # Clean up token file
        try:
            os.unlink(token_file)
        except OSError:
            pass


@pytest.fixture(scope="session")
def slurm_env_vars(services_ready: Dict[str, str], slurm_token: str) -> Dict[str, str]:
    """
    Environment variables for Slurm worker configuration.

    :param services_ready: Service URLs from services_ready fixture
    :param slurm_token: Token file path from slurm_token fixture

    :returns: Environment variables for Slurm worker
    :rtype: dict
    """
    return {
        "PREFECT_API_URL": services_ready["prefect_url"],
        "PREFECT_SLURM_API_URL": services_ready["slurm_url"],
        "PREFECT_SLURM_USER_NAME": "slurm",
        "PREFECT_SLURM_TOKEN_FILE": slurm_token,
        "PREFECT_SLURM_LOCK_TIMEOUT": "30",
    }


@pytest.fixture
def temp_dir() -> Generator[str, None, None]:
    """Create a temporary directory for test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir
