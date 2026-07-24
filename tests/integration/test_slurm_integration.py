"""
Integration tests for Prefect Slurm worker.

These tests use real Docker containers to test the complete
workflow from flow submission to Slurm job execution.
"""

import asyncio
import os
import time
from typing import Dict

import httpx
import pytest
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.objects import Flow
from testcontainers.compose import DockerCompose

from .utils import (
    create_work_pool,
    wait_for_flow_completed,
    wait_for_flow_scheduled,
    wait_for_flow_crashed,
    kill_slurm_job,
    get_slurm_job_id,
)


def get_slurm_queue_states(compose: DockerCompose) -> Dict[str, str]:
    stdout, _, _ = compose.exec_in_container(
        command=["squeue", "-h", "-o", "%i|%T"],
        service_name="slurm_node",
    )
    return dict(
        line.split("|", maxsplit=1) for line in stdout.splitlines() if "|" in line
    )


def submit_slurm_blocker(compose: DockerCompose, sleep_seconds: int) -> str:
    stdout, _, exit_code = compose.exec_in_container(
        command=[
            "su",
            "-",
            "slurm",
            "-c",
            (f"sbatch --parsable --cpus-per-task=4 --wrap='sleep {sleep_seconds}'"),
        ],
        service_name="slurm_node",
    )
    assert exit_code == 0, f"Failed to submit Slurm blocker job: {stdout}"
    return stdout.strip().split(";")[0]


@pytest.mark.asyncio
@pytest.mark.integration
async def test_slurm_integration_happy_path(
    services_ready: Dict[str, str], slurm_env_vars: Dict[str, str]
):
    """
    Test the complete happy path for Slurm integration.

    This test:
    1. Starts Docker Compose stack with Slurm and Prefect (worker runs in slurm_submitter container)
    2. Creates a work pool matching the worker's expected pool name
    3. Registers and deploys a test flow
    4. Submits a flow run
    5. Waits for the flow to be scheduled (picked up by worker) - 60s timeout
    6. Waits for the flow to complete successfully - 300s timeout
    7. Validates the results (test passes only if flow reaches "Completed" state)
    """
    prefect_url = services_ready["prefect_url"]

    for key, value in slurm_env_vars.items():
        os.environ[key] = value

    try:
        # Use the same pool name as configured in docker-compose.yaml for the worker
        pool_name = "slurm-pool"
        work_pool = await create_work_pool(prefect_url, pool_name)
        print(f"Created work pool: {work_pool['name']}")

        async with PrefectClient(api=prefect_url) as client:
            flow_schema = Flow(name="slurm-hello-world")
            flow_id = await client.create_flow(flow_schema)
            print(f"Registered flow: {flow_id}")

            # Create a deployment
            deployment_id = await client.create_deployment(
                flow_id=flow_id,
                name="integration-test-deployment",
                work_pool_name=pool_name,
                path="/home/slurm/examples",
                entrypoint="hello_world.py:hello_world",
                paused=False,
                job_variables={
                    "working_dir": "/tmp/workdir",
                    "memory": 1,
                    "env": {"PATH": "/bin/:/usr/bin/:/sbin/"},
                },
            )

            print(f"Created deployment: {deployment_id}")

            flow_run = await client.create_flow_run_from_deployment(
                deployment_id=deployment_id
            )
            print(f"Created flow run: {flow_run.id}")

            try:
                # Phase 1: Wait for flow to be scheduled (picked up by worker)
                print("Phase 1: Waiting for flow to be scheduled by worker...")
                await wait_for_flow_scheduled(client, str(flow_run.id), timeout=60)
                print(f"✅ Flow run {flow_run.id} successfully scheduled!")

                # Phase 2: Wait for flow to complete execution
                print("Phase 2: Waiting for flow to complete execution...")
                completed_flow_run = await wait_for_flow_completed(
                    client, str(flow_run.id), timeout=300
                )
                print(f"✅ Flow run {flow_run.id} completed successfully!")

                # Validate final state is "Completed"
                final_state = completed_flow_run.state.name
                assert final_state == "Completed", (
                    f"Expected 'Completed', got '{final_state}'"
                )

                # Print success details
                print(
                    "🎉 Integration test passed! Complete end-to-end flow execution successful."
                )
                print(f"   - Flow run ID: {flow_run.id}")
                print(f"   - Final state: {final_state}")

                if (
                    hasattr(completed_flow_run.state, "message")
                    and completed_flow_run.state.message
                ):
                    print(f"   - State message: {completed_flow_run.state.message}")

            except TimeoutError as e:
                # Get current state for debugging
                current_flow_run = await client.read_flow_run(flow_run.id)
                current_state = current_flow_run.state.name
                print(f"❌ Timeout error: {e}")
                print(f"   - Flow run ID: {flow_run.id}")
                print(f"   - Current state: {current_state}")

                if (
                    hasattr(current_flow_run.state, "message")
                    and current_flow_run.state.message
                ):
                    print(f"   - State message: {current_flow_run.state.message}")

                raise AssertionError(f"Integration test failed due to timeout: {e}")

            except RuntimeError as e:
                # Flow reached terminal failure state
                current_flow_run = await client.read_flow_run(flow_run.id)
                current_state = current_flow_run.state.name
                print(f"❌ Runtime error: {e}")
                print(f"   - Flow run ID: {flow_run.id}")
                print(f"   - Current state: {current_state}")

                if (
                    hasattr(current_flow_run.state, "message")
                    and current_flow_run.state.message
                ):
                    print(f"   - State message: {current_flow_run.state.message}")

                raise AssertionError(
                    f"Integration test failed - flow reached terminal state: {e}"
                )

    finally:
        # Clean up environment variables
        for key in slurm_env_vars.keys():
            os.environ.pop(key, None)
        os.environ.pop("PREFECT_API_URL", None)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_deployment_concurrency_limit_controls_slurm_jobs(
    services_ready: Dict[str, str],
    slurm_env_vars: Dict[str, str],
    docker_compose_setup: DockerCompose,
):
    prefect_url = services_ready["prefect_url"]
    for key, value in slurm_env_vars.items():
        os.environ[key] = value

    flow_runs = []
    submitted_job_ids = set()
    blocker_job_id = None

    try:
        pool_name = "slurm-pool"
        await create_work_pool(prefect_url, pool_name)

        async with PrefectClient(api=prefect_url) as client:
            # Occupy every CPU so deployment jobs remain queued in Slurm.
            blocker_job_id = submit_slurm_blocker(
                docker_compose_setup, sleep_seconds=75
            )
            flow_id = await client.create_flow(Flow(name="slurm-concurrency-test"))
            deployment_id = await client.create_deployment(
                flow_id=flow_id,
                name="concurrency-limit-deployment",
                work_pool_name=pool_name,
                path="/home/slurm/examples",
                entrypoint="concurrency.py:concurrency_test_flow",
                concurrency_limit=2,
                paused=False,
                parameters={"sleep_seconds": 10},
                job_variables={
                    "working_dir": "/tmp/workdir",
                    "memory": 1,
                },
            )
            flow_runs = [
                await client.create_flow_run_from_deployment(
                    deployment_id=deployment_id
                )
                for _ in range(3)
            ]

            admission_deadline = time.monotonic() + 65
            delayed_run_observed = False

            # Observe beyond the 30-second Prefect lease to catch over-submission.
            while time.monotonic() < admission_deadline:
                current_runs = [
                    await client.read_flow_run(flow_run.id) for flow_run in flow_runs
                ]
                submitted = [
                    flow_run
                    for flow_run in current_runs
                    if flow_run.infrastructure_pid is not None
                ]
                submitted_job_ids.update(
                    flow_run.infrastructure_pid for flow_run in submitted
                )
                delayed = [
                    flow_run
                    for flow_run in current_runs
                    if flow_run.infrastructure_pid is None
                    and flow_run.state.name
                    in {
                        "Scheduled",
                        "AwaitingConcurrencySlot",
                    }
                ]
                queue_states = get_slurm_queue_states(docker_compose_setup)
                active_submitted = [
                    flow_run
                    for flow_run in submitted
                    if queue_states.get(flow_run.infrastructure_pid)
                    in {"PENDING", "RUNNING"}
                ]

                assert len(active_submitted) <= 2, (
                    "Deployment concurrency limit was exceeded after Prefect's "
                    "initial lease expired while Slurm jobs remained queued"
                )
                if len(active_submitted) == 2 and len(delayed) == 1:
                    delayed_run_observed = True

                await asyncio.sleep(1)

            assert delayed_run_observed, (
                "Did not observe two queued Slurm jobs with the third flow run "
                "left unsubmitted by the deployment concurrency limit"
            )

            max_active_jobs = 0
            completion_deadline = time.monotonic() + 240

            while time.monotonic() < completion_deadline:
                current_runs = [
                    await client.read_flow_run(flow_run.id) for flow_run in flow_runs
                ]
                submitted_job_ids.update(
                    flow_run.infrastructure_pid
                    for flow_run in current_runs
                    if flow_run.infrastructure_pid is not None
                )

                queue_states = get_slurm_queue_states(docker_compose_setup)
                active_jobs = sum(
                    queue_states.get(job_id) in {"PENDING", "RUNNING"}
                    for job_id in submitted_job_ids
                )
                max_active_jobs = max(max_active_jobs, active_jobs)
                assert active_jobs <= 2

                states = {flow_run.state.name for flow_run in current_runs}
                assert not states.intersection({"Failed", "Crashed", "Cancelled"})
                if states == {"Completed"}:
                    break

                await asyncio.sleep(1)
            else:
                raise TimeoutError("Concurrency-limited flow runs did not all complete")

            assert max_active_jobs == 2
            assert len(submitted_job_ids) == 3
    finally:
        jobs_to_cancel = set(submitted_job_ids)
        if blocker_job_id:
            jobs_to_cancel.add(blocker_job_id)
        if jobs_to_cancel:
            docker_compose_setup.exec_in_container(
                command=["scancel", *jobs_to_cancel],
                service_name="slurm_node",
            )
        for key in slurm_env_vars.keys():
            os.environ.pop(key, None)
        os.environ.pop("PREFECT_API_URL", None)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_slurm_worker_credentials_validation(
    services_ready: Dict[str, str], slurm_env_vars: Dict[str, str]
):
    """
    Test that we can validate Slurm credentials.

    This test verifies:
    1. JWT token file exists and has proper format
    2. We can connect to Slurm API with token
    """
    # Set environment variables
    for key, value in slurm_env_vars.items():
        os.environ[key] = value

    try:
        # Test token reading directly
        token_file = slurm_env_vars["PREFECT_SLURM_TOKEN_FILE"]
        with open(token_file, "r") as f:
            token = f.read().strip()

        # Validate JWT format (3 parts separated by dots)
        assert token.count(".") == 2, "Token should be valid JWT format"
        parts = token.split(".")
        assert all(part for part in parts), "JWT should not have empty parts"

        # Test Slurm API connectivity with token
        slurm_url = services_ready["slurm_url"]
        async with httpx.AsyncClient() as client:
            headers = {"X-SLURM-USER-TOKEN": token}
            response = await client.get(
                f"{slurm_url}/slurm/v0.0.42/ping", headers=headers, timeout=10.0
            )
            # Should get 200 with valid token, or 401/403 if token validation fails
            assert response.status_code in [200, 401, 403], (
                f"Unexpected status: {response.status_code}"
            )

        print("✅ Credentials validation test passed!")

    finally:
        # Clean up environment variables
        for key in slurm_env_vars.keys():
            os.environ.pop(key, None)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_slurm_api_connectivity(services_ready: Dict[str, str]):
    """
    Test direct connectivity to Slurm REST API.

    This test verifies the Slurm REST API is accessible and responds correctly.
    """
    slurm_url = services_ready["slurm_url"]

    async with httpx.AsyncClient() as client:
        # Test ping endpoint - 401 is expected without auth, but means service is running
        ping_response = await client.get(
            f"{slurm_url}/slurm/v0.0.42/ping", timeout=10.0
        )
        assert ping_response.status_code in [200, 401], (
            f"Slurm API should respond (got {ping_response.status_code})"
        )

        if ping_response.status_code == 200:
            ping_data = ping_response.json()
            assert "pings" in ping_data, "Ping response should contain 'pings'"
            print("Slurm API ping succeeded with auth")
        else:
            print("Slurm API responded with 401 (no auth) - service is running")

        print("✅ Slurm API connectivity test passed!")


@pytest.mark.asyncio
@pytest.mark.integration
async def test_prefect_api_connectivity(services_ready: Dict[str, str]):
    """
    Test connectivity to Prefect API.

    This test verifies the Prefect server is accessible and healthy.
    """
    prefect_url = services_ready["prefect_url"]

    async with httpx.AsyncClient() as client:
        # Test ready endpoint
        ready_response = await client.get(f"{prefect_url}/ready", timeout=10.0)
        assert ready_response.status_code == 200, "Prefect API should be ready"

        ready_data = ready_response.json()
        assert ready_data.get("message") == "OK", "Prefect API should return OK"

        # Test basic API functionality - try to get version info
        version_response = await client.get(f"{prefect_url}/version", timeout=10.0)
        assert version_response.status_code == 200, "Should be able to get version info"

        version_text = version_response.text.strip()
        assert version_text, "Version response should not be empty"
        print(f"Prefect version: {version_text}")

        print("✅ Prefect API connectivity test passed!")


@pytest.mark.asyncio
@pytest.mark.integration
async def test_zombie_flow_killed_slurm_job(
    services_ready: Dict[str, str],
    slurm_env_vars: Dict[str, str],
    docker_compose_setup: DockerCompose,
):
    """
    Test zombie flow detection when Slurm job is killed externally.

    This test:
    1. Starts a long-running flow
    2. Waits for it to be scheduled and running
    3. Kills the Slurm job externally using scancel
    4. Waits for worker to detect zombie and mark flow as "Crashed"
    5. Validates final state is "Crashed", not "Completed"
    """
    prefect_url = services_ready["prefect_url"]

    for key, value in slurm_env_vars.items():
        os.environ[key] = value

    try:
        pool_name = "slurm-pool"
        work_pool = await create_work_pool(prefect_url, pool_name)
        print(f"Created work pool: {work_pool['name']}")

        async with PrefectClient(api=prefect_url) as client:
            # Create long-running flow
            flow_schema = Flow(name="long-running-flow")
            flow_id = await client.create_flow(flow_schema)
            print(f"Registered long-running flow: {flow_id}")

            # Create deployment for long-running flow
            deployment_id = await client.create_deployment(
                flow_id=flow_id,
                name="long-running-deployment",
                work_pool_name=pool_name,
                path="/home/slurm/examples",
                entrypoint="long_running.py:long_running_flow",
                paused=False,
                job_variables={
                    "working_dir": "/tmp/workdir",
                    "memory": 1,
                    "time_limit": 2,  # 2 hours should be enough
                },
            )
            print(f"Created deployment: {deployment_id}")

            # Start flow run
            flow_run = await client.create_flow_run_from_deployment(
                deployment_id=deployment_id
            )
            print(f"Created flow run: {flow_run.id}")

            try:
                # Wait for flow to be scheduled
                print("Waiting for long-running flow to be scheduled...")
                await wait_for_flow_scheduled(client, str(flow_run.id), timeout=60)
                print(f"✅ Flow run {flow_run.id} successfully scheduled!")

                # Wait a bit for job to actually start running
                print("Waiting for flow to start running...")
                import asyncio

                await asyncio.sleep(10)

                # Get the Slurm job ID
                job_id = await get_slurm_job_id(client, str(flow_run.id))
                print(f"Flow run has Slurm job ID: {job_id}")

                if job_id:
                    # Kill the Slurm job externally
                    print(f"Killing Slurm job {job_id}...")
                    kill_success = kill_slurm_job(docker_compose_setup, job_id)
                    assert kill_success, f"Failed to kill Slurm job {job_id}"

                    # Wait for worker to detect zombie and crash the flow
                    print("Waiting for worker to detect zombie and crash flow...")
                    crashed_flow_run = await wait_for_flow_crashed(
                        client, str(flow_run.id), timeout=120
                    )
                    print(f"✅ Flow run {flow_run.id} correctly crashed!")

                    # Validate final state is "Crashed"
                    final_state = crashed_flow_run.state.name
                    assert final_state == "Crashed", (
                        f"Expected 'Crashed', got '{final_state}'"
                    )

                    print("🎉 Zombie flow test (killed job) passed!")
                    print(f"   - Flow run ID: {flow_run.id}")
                    print(f"   - Slurm job ID: {job_id}")
                    print(f"   - Final state: {final_state}")
                else:
                    raise AssertionError("Flow run did not get a Slurm job ID")

            except (TimeoutError, RuntimeError) as e:
                print(f"❌ Zombie flow test failed: {e}")
                current_flow_run = await client.read_flow_run(flow_run.id)
                print(f"   - Current state: {current_flow_run.state.name}")
                raise AssertionError(f"Zombie flow test failed: {e}")

    finally:
        # Clean up environment variables
        for key in slurm_env_vars.keys():
            os.environ.pop(key, None)
        os.environ.pop("PREFECT_API_URL", None)


@pytest.mark.asyncio
@pytest.mark.integration
async def test_zombie_flow_bad_source_file(
    services_ready: Dict[str, str], slurm_env_vars: Dict[str, str]
):
    """
    Test zombie flow detection when source file doesn't exist.

    This test:
    1. Creates a deployment with non-existent source file
    2. Starts flow run
    3. Waits for flow to fail during script execution
    4. Validates final state is "Crashed" due to source file error
    """
    prefect_url = services_ready["prefect_url"]

    for key, value in slurm_env_vars.items():
        os.environ[key] = value

    try:
        pool_name = "slurm-pool"
        work_pool = await create_work_pool(prefect_url, pool_name)
        print(f"Created work pool: {work_pool['name']}")

        async with PrefectClient(api=prefect_url) as client:
            # Create flow with bad source file
            flow_schema = Flow(name="slurm-hello-world")
            flow_id = await client.create_flow(flow_schema)
            print(f"Registered flow: {flow_id}")

            # Create deployment with non-existent source file
            deployment_id = await client.create_deployment(
                flow_id=flow_id,
                name="bad-source-deployment",
                work_pool_name=pool_name,
                path="/home/slurm/examples",
                entrypoint="hello_world.py:hello_world",
                paused=False,
                job_variables={
                    "working_dir": "/tmp/workdir",
                    "memory": 1,
                    "source_files": [
                        "/nonexistent/setup.sh"
                    ],  # This will cause failure
                },
            )
            print(f"Created deployment with bad source file: {deployment_id}")

            # Start flow run
            flow_run = await client.create_flow_run_from_deployment(
                deployment_id=deployment_id
            )
            print(f"Created flow run: {flow_run.id}")

            try:
                # Wait for flow to be scheduled
                print("Waiting for flow to be scheduled...")
                await wait_for_flow_scheduled(client, str(flow_run.id), timeout=60)
                print(f"✅ Flow run {flow_run.id} successfully scheduled!")

                # Wait for flow to crash due to source file error
                print("Waiting for flow to crash due to bad source file...")
                crashed_flow_run = await wait_for_flow_crashed(
                    client, str(flow_run.id), timeout=120
                )
                print(f"✅ Flow run {flow_run.id} correctly crashed!")

                # Validate final state is "Crashed"
                final_state = crashed_flow_run.state.name
                assert final_state == "Crashed", (
                    f"Expected 'Crashed', got '{final_state}'"
                )

                print("🎉 Zombie flow test (bad source file) passed!")
                print(f"   - Flow run ID: {flow_run.id}")
                print(f"   - Final state: {final_state}")

            except (TimeoutError, RuntimeError) as e:
                print(f"❌ Bad source file test failed: {e}")
                current_flow_run = await client.read_flow_run(flow_run.id)
                print(f"   - Current state: {current_flow_run.state.name}")
                raise AssertionError(f"Bad source file test failed: {e}")

    finally:
        # Clean up environment variables
        for key in slurm_env_vars.keys():
            os.environ.pop(key, None)
        os.environ.pop("PREFECT_API_URL", None)
