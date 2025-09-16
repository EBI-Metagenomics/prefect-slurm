"""
Utility functions for integration tests.
"""

import asyncio
import os
import re
import tempfile
import time
from pathlib import Path
from typing import Dict, Optional

import httpx
from prefect.client.orchestration import PrefectClient
from testcontainers.compose import DockerCompose


async def wait_for_health_check(
    url: str, timeout: int = 120, interval: int = 2
) -> bool:
    """
    Wait for a health check URL to return a successful response.

    :param url: Health check URL to test
    :param timeout: Maximum time to wait in seconds
    :param interval: Time between checks in seconds

    :returns: True if health check succeeded, False if timed out
    :rtype: bool
    """
    start_time = time.time()

    async with httpx.AsyncClient() as client:
        while time.time() - start_time < timeout:
            try:
                response = await client.get(url, timeout=5.0)
                if response.status_code == 200:
                    return True
            except (httpx.RequestError, httpx.TimeoutException):
                pass

            await asyncio.sleep(interval)

    return False


async def wait_for_prefect_api(prefect_url: str, timeout: int = 120) -> bool:
    """
    Wait for Prefect API to be ready.

    :param prefect_url: Base Prefect API URL (e.g., http://localhost:4200/api)
    :param timeout: Maximum time to wait in seconds

    :returns: True if API is ready, False if timed out
    :rtype: bool
    """
    ready_url = f"{prefect_url.rstrip('/')}/ready"

    start_time = time.time()
    async with httpx.AsyncClient() as client:
        while time.time() - start_time < timeout:
            try:
                response = await client.get(ready_url, timeout=5.0)
                if response.status_code == 200:
                    data = response.json()
                    if data.get("message") == "OK":
                        return True
            except (httpx.RequestError, httpx.TimeoutException, ValueError):
                pass

            await asyncio.sleep(2)

    return False


async def wait_for_slurm_api(slurm_url: str, timeout: int = 120) -> bool:
    """
    Wait for Slurm REST API to be ready.

    :param slurm_url: Base Slurm API URL (e.g., http://localhost:6820)
    :param timeout: Maximum time to wait in seconds

    :returns: True if API is ready, False if timed out
    :rtype: bool
    """
    ping_url = f"{slurm_url.rstrip('/')}/slurm/v0.0.42/ping"

    start_time = time.time()
    async with httpx.AsyncClient() as client:
        while time.time() - start_time < timeout:
            try:
                response = await client.get(ping_url, timeout=5.0)
                if response.status_code == 200:
                    return True
            except (httpx.RequestError, httpx.TimeoutException):
                pass

            await asyncio.sleep(2)

    return False


def extract_jwt_from_scontrol_output(output: str) -> Optional[str]:
    """
    Extract JWT token from scontrol token command output.

    :param output: Raw output from scontrol token command

    :returns: JWT token if found, None otherwise
    :rtype: str

    .. example::
        SLURM_JWT=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
    """
    # Look for SLURM_JWT=<token> pattern
    match = re.search(r"SLURM_JWT=([A-Za-z0-9_.-]+)", output)
    if match:
        return match.group(1)

    # Alternative pattern: just the JWT token on a line by itself
    lines = output.strip().split("\n")
    for line in lines:
        line = line.strip()
        if line and "." in line and line.count(".") == 2:
            # Basic JWT format check (3 parts separated by dots)
            parts = line.split(".")
            if all(part for part in parts):  # No empty parts
                return line

    return None


def create_token_file(token: str) -> str:
    """
    Create a temporary token file with proper permissions.

    :param token: JWT token to write

    :returns: Path to the created token file
    :rtype: str
    """
    # Create temporary file
    fd, token_file = tempfile.mkstemp(suffix=".jwt", prefix="slurm_token_")

    try:
        # Write token to file
        os.write(fd, token.encode("utf-8"))
        os.close(fd)

        # Set proper permissions (600 - owner read/write only)
        os.chmod(token_file, 0o600)

        return token_file
    except Exception:
        # Clean up on error
        try:
            os.close(fd)
        except OSError:
            pass
        try:
            os.unlink(token_file)
        except OSError:
            pass
        raise


async def create_work_pool(
    prefect_url: str, pool_name: str = "test-slurm-pool"
) -> Dict:
    """
    Create a Slurm work pool via Prefect API.

    :param prefect_url: Base Prefect API URL
    :param pool_name: Name for the work pool

    :returns: Created work pool data
    :rtype: dict
    """
    async with httpx.AsyncClient() as client:
        # Delete existing work pool if it exists to ensure fresh template
        delete_url = f"{prefect_url.rstrip('/')}/work_pools/{pool_name}"
        delete_response = await client.delete(delete_url, timeout=10.0)
        if delete_response.status_code == 200:
            print(f"Deleted existing work pool '{pool_name}' to create fresh one")

        # Create new work pool with correct template structure
        url = f"{prefect_url.rstrip('/')}/work_pools/"

        # Template based on SlurmWorkerConfiguration and SlurmWorkerTemplateVariables
        payload = {
            "name": pool_name,
            "type": "slurm",
            "description": "Integration test Slurm work pool",
            "is_paused": False,
            "base_job_template": {
                "job_configuration": {
                    "cpu": "{{ cpu }}",
                    "memory": "{{ memory }}",
                    "partition": "{{ partition }}",
                    "script": "{{ script }}",
                    "source_files": "{{ source_files }}",
                    "time_limit": "{{ time_limit }}",
                    "working_dir": "{{ working_dir }}",
                },
                "variables": {
                    "type": "object",
                    "properties": {
                        "cpu": {
                            "type": "integer",
                            "title": "CPU",
                            "description": "CPU count required for the flow",
                            "default": 1,
                        },
                        "memory": {
                            "type": "integer",
                            "title": "Memory",
                            "description": "Memory in GB required for the flow",
                            "default": 4,
                        },
                        "partition": {
                            "type": "string",
                            "title": "Partition",
                            "description": "Slurm partition to use",
                            "default": None,
                        },
                        "script": {
                            "type": "string",
                            "title": "Script",
                            "description": "Script to run in the SLURM job",
                            "default": "",
                        },
                        "source_files": {
                            "type": "array",
                            "title": "Source Files",
                            "description": "List of environment files to source",
                            "default": [],
                            "items": {"type": "string"},
                        },
                        "time_limit": {
                            "type": "integer",
                            "title": "Time Limit",
                            "description": "Max number of wall time in hours for the flow",
                            "default": 1,
                        },
                        "working_dir": {
                            "type": "string",
                            "title": "Working Directory",
                            "description": "Directory where to run the flow from",
                            "default": "/tmp",
                        },
                    },
                    "required": ["working_dir"],
                },
            },
        }

        response = await client.post(url, json=payload, timeout=30.0)
        response.raise_for_status()
        print(f"Created work pool '{pool_name}' with proper Slurm template")
        return response.json()


async def wait_for_flow_state(
    client: PrefectClient,
    flow_run_id: str,
    expected_state: str,
    timeout: int = 60,
    poll_interval: int = 5,
) -> Dict:
    """
    Wait for a flow run to reach a specific state.

    :param client: Prefect client instance
    :param flow_run_id: ID of the flow run to monitor
    :param expected_state: The state to wait for (e.g., "Scheduled", "Completed")
    :param timeout: Maximum time to wait in seconds
    :param poll_interval: Time between status checks in seconds

    :returns: Flow run data when desired state is reached
    :rtype: dict

    :raises TimeoutError: If flow doesn't reach expected state within timeout
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        flow_run = await client.read_flow_run(flow_run_id)
        current_state = flow_run.state.name

        print(f"Flow run {flow_run_id} current state: {current_state}")

        if current_state == expected_state:
            return flow_run

        # Check for terminal failure states
        if current_state in ["Failed", "Crashed", "Cancelled"]:
            raise RuntimeError(
                f"Flow run {flow_run_id} reached terminal state '{current_state}' "
                f"while waiting for '{expected_state}'"
            )

        await asyncio.sleep(poll_interval)

    # Get final state for error message
    flow_run = await client.read_flow_run(flow_run_id)
    current_state = flow_run.state.name
    raise TimeoutError(
        f"Flow run {flow_run_id} did not reach '{expected_state}' within {timeout} seconds. "
        f"Current state: {current_state}"
    )


async def wait_for_flow_scheduled(
    client: PrefectClient, flow_run_id: str, timeout: int = 60
) -> Dict:
    """
    Wait for a flow run to be scheduled (picked up by worker).

    :param client: Prefect client instance
    :param flow_run_id: ID of the flow run to monitor
    :param timeout: Maximum time to wait in seconds

    :returns: Flow run data when scheduled
    :rtype: dict

    :raises TimeoutError: If flow isn't scheduled within timeout
    """
    print(f"Waiting for flow run {flow_run_id} to be scheduled...")
    return await wait_for_flow_state(client, flow_run_id, "Scheduled", timeout)


async def wait_for_flow_completed(
    client: PrefectClient, flow_run_id: str, timeout: int = 300
) -> Dict:
    """
    Wait for a flow run to complete successfully.

    :param client: Prefect client instance
    :param flow_run_id: ID of the flow run to monitor
    :param timeout: Maximum time to wait in seconds

    :returns: Flow run data when completed
    :rtype: dict

    :raises TimeoutError: If flow doesn't complete within timeout
    :raises RuntimeError: If flow fails during execution
    """
    print(f"Waiting for flow run {flow_run_id} to complete...")
    return await wait_for_flow_state(client, flow_run_id, "Completed", timeout)


async def wait_for_flow_crashed(
    client: PrefectClient, flow_run_id: str, timeout: int = 120
) -> Dict:
    """
    Wait for a flow run to crash.

    :param client: Prefect client instance
    :param flow_run_id: ID of the flow run to monitor
    :param timeout: Maximum time to wait in seconds

    :returns: Flow run data when crashed
    :rtype: dict

    :raises TimeoutError: If flow doesn't crash within timeout
    :raises RuntimeError: If flow reaches unexpected terminal state
    """
    print(f"Waiting for flow run {flow_run_id} to crash...")
    return await wait_for_flow_state(client, flow_run_id, "Crashed", timeout)


def kill_slurm_job(compose: DockerCompose, job_id: str) -> bool:
    """
    Kill a Slurm job using scancel command in the slurm_node container.

    :param job_id: Slurm job ID to cancel

    :returns: True if command executed successfully
    :rtype: bool
    """
    try:
        _, stderr, exit_code = compose.exec_in_container(
            command=["scancel", job_id], service_name="slurm_node"
        )

        if exit_code == 0:
            print(f"Successfully killed Slurm job {job_id}")
            return True
        else:
            print(f"Failed to kill job {job_id}: {stderr}")
            return False
    except Exception as e:
        print(f"Error killing job {job_id}: {e}")
        return False


async def get_slurm_job_id(client: PrefectClient, flow_run_id: str) -> Optional[str]:
    """
    Get the Slurm job ID from a flow run's infrastructure_pid.

    :param client: Prefect client instance
    :param flow_run_id: ID of the flow run

    :returns: Slurm job ID if available, None otherwise
    :rtype: str
    """
    flow_run = await client.read_flow_run(flow_run_id)
    return flow_run.infrastructure_pid


async def wait_for_flow_completion(
    prefect_url: str, flow_run_id: str, timeout: int = 300, poll_interval: int = 5
) -> Dict:
    """
    Wait for a flow run to complete.

    :param prefect_url: Base Prefect API URL
    :param flow_run_id: ID of the flow run to monitor
    :param timeout: Maximum time to wait in seconds
    :param poll_interval: Time between status checks in seconds

    :returns: Final flow run data
    :rtype: dict

    :raises TimeoutError: If flow doesn't complete within timeout
    :raises httpx.HTTPStatusError: If API request fails
    """
    url = f"{prefect_url.rstrip('/')}/flow_runs/{flow_run_id}"
    start_time = time.time()

    async with httpx.AsyncClient() as client:
        while time.time() - start_time < timeout:
            response = await client.get(url, timeout=10.0)
            response.raise_for_status()

            flow_run = response.json()
            state_name = flow_run.get("state", {}).get("name", "UNKNOWN")

            if state_name in ["COMPLETED", "FAILED", "CRASHED", "CANCELLED"]:
                return flow_run

            await asyncio.sleep(poll_interval)

    raise TimeoutError(
        f"Flow run {flow_run_id} did not complete within {timeout} seconds"
    )


def get_project_root() -> Path:
    """Get the project root directory."""
    return Path(__file__).parent.parent.parent


def get_docker_compose_path() -> Path:
    """Get path to the Docker Compose file."""
    return get_project_root() / "slurm_environment" / "docker-compose.yaml"
