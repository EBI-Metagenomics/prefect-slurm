"""
Unit tests for SlurmWorker class methods.
"""

import importlib
import logging
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import pytest
from prefect.client.schemas import FlowRun, StateType
from prefect.exceptions import InfrastructureError
from prefect.states import Pending, Running
from prefect.workers.base import BaseWorkerResult
from slurpy.v0042.asyncio.rest import ApiException

from prefect_slurm.log_filters import RedactingFilter
from prefect_slurm.worker import (
    DEPLOYMENT_CONCURRENCY_LEASE_DURATION_SECONDS,
    SlurmWorker,
)


def _underlying_logger(logger):
    return logger if isinstance(logger, logging.Logger) else logger.logger


@pytest.mark.unit
class TestSlurmWorker:
    """Test cases for SlurmWorker class methods."""

    @pytest.mark.asyncio
    async def test_filter_runs_by_deployment_concurrency(self):
        worker = SlurmWorker(work_pool_name="test-pool")
        limited_deployment_id = uuid4()
        unlimited_deployment_id = uuid4()

        limited_runs = [
            FlowRun(
                id=uuid4(),
                flow_id=uuid4(),
                deployment_id=limited_deployment_id,
                name=f"limited-{index}",
                state=Pending(),
            )
            for index in range(3)
        ]
        unlimited_run = FlowRun(
            id=uuid4(),
            flow_id=uuid4(),
            deployment_id=unlimited_deployment_id,
            name="unlimited",
            state=Pending(),
        )
        entries = [
            SimpleNamespace(flow_run=flow_run)
            for flow_run in [*limited_runs, unlimited_run]
        ]

        with patch.object(
            worker,
            "_get_deployment_available_slurm_slots",
            AsyncMock(side_effect=[2, None]),
        ) as get_slots:
            filtered = await worker._filter_runs_by_deployment_concurrency(entries)

        assert [entry.flow_run for entry in filtered] == [
            limited_runs[0],
            limited_runs[1],
            unlimited_run,
        ]
        assert get_slots.await_count == 2

    @pytest.mark.asyncio
    async def test_get_deployment_available_slurm_slots(self):
        worker = SlurmWorker(work_pool_name="test-pool")
        deployment_id = uuid4()
        worker._client = AsyncMock()
        worker._client.read_deployment.return_value = SimpleNamespace(
            global_concurrency_limit=SimpleNamespace(
                active=True,
                limit=5,
            )
        )

        with patch.object(
            worker,
            "_count_active_slurm_jobs_for_deployment",
            AsyncMock(return_value=3),
        ):
            available = await worker._get_deployment_available_slurm_slots(
                deployment_id
            )

        assert available == 2

    @pytest.mark.asyncio
    async def test_get_deployment_available_slurm_slots_without_limit(self):
        worker = SlurmWorker(work_pool_name="test-pool")
        worker._client = AsyncMock()
        worker._client.read_deployment.return_value = SimpleNamespace(
            global_concurrency_limit=None
        )

        available = await worker._get_deployment_available_slurm_slots(uuid4())

        assert available is None

    @pytest.mark.asyncio
    async def test_get_deployment_available_slurm_slots_falls_back_on_error(self):
        worker = SlurmWorker(work_pool_name="test-pool")
        worker._client = AsyncMock()
        worker._client.read_deployment.side_effect = RuntimeError("API unavailable")

        available = await worker._get_deployment_available_slurm_slots(uuid4())

        assert available is None

    @pytest.mark.asyncio
    async def test_count_active_slurm_jobs_for_deployment(self):
        worker = SlurmWorker(work_pool_name="test-pool")
        deployment_id = uuid4()
        worker._client = AsyncMock()
        worker._client.read_flow_runs.return_value = [
            FlowRun(
                id=uuid4(),
                flow_id=uuid4(),
                deployment_id=deployment_id,
                name=f"run-{job_id}",
                state=Running(),
                infrastructure_pid=job_id,
            )
            for job_id in ["1", "2", "3", "4"]
        ] + [
            FlowRun(
                id=uuid4(),
                flow_id=uuid4(),
                deployment_id=deployment_id,
                name="submission-in-progress",
                state=Pending(),
                infrastructure_pid=None,
            )
        ]

        with patch.object(
            worker,
            "_get_slurm_job_states",
            AsyncMock(
                return_value={
                    "1": "PENDING",
                    "2": "RUNNING",
                    "3": "COMPLETED",
                    "4": "UNKNOWN",
                }
            ),
        ):
            active_jobs = await worker._count_active_slurm_jobs_for_deployment(
                deployment_id
            )

        assert active_jobs == 4
        flow_run_filter = worker._client.read_flow_runs.call_args.kwargs[
            "flow_run_filter"
        ]
        assert flow_run_filter.deployment_id.any_ == [deployment_id]
        assert flow_run_filter.state.type.any_ == [
            StateType.PENDING,
            StateType.RUNNING,
            StateType.CANCELLING,
        ]

    @pytest.mark.asyncio
    async def test_renew_pending_slurm_concurrency_leases(self):
        worker = SlurmWorker(work_pool_name="test-pool")
        lease_id = uuid4()
        pending_run = FlowRun(
            id=uuid4(),
            flow_id=uuid4(),
            name="queued",
            state=Pending(
                state_details={
                    "deployment_concurrency_lease_id": lease_id,
                }
            ),
            infrastructure_pid="123",
        )
        running_run = FlowRun(
            id=uuid4(),
            flow_id=uuid4(),
            name="already-running",
            state=Running(
                state_details={
                    "deployment_concurrency_lease_id": uuid4(),
                }
            ),
            infrastructure_pid="456",
        )
        worker._client = AsyncMock()

        with (
            patch.object(
                worker,
                "_get_running_or_pending_flow_runs",
                AsyncMock(return_value=[pending_run, running_run]),
            ),
            patch.object(
                worker,
                "_get_slurm_job_states",
                AsyncMock(return_value={"123": "PENDING"}),
            ),
        ):
            await worker._renew_pending_slurm_concurrency_leases()

        worker._client.renew_concurrency_lease.assert_awaited_once_with(
            lease_id=lease_id,
            lease_duration=DEPLOYMENT_CONCURRENCY_LEASE_DURATION_SECONDS,
        )

    @pytest.mark.asyncio
    async def test_renew_pending_slurm_concurrency_leases_ignores_terminal_job(self):
        worker = SlurmWorker(work_pool_name="test-pool")
        pending_run = FlowRun(
            id=uuid4(),
            flow_id=uuid4(),
            name="finished",
            state=Pending(
                state_details={
                    "deployment_concurrency_lease_id": uuid4(),
                }
            ),
            infrastructure_pid="123",
        )
        worker._client = AsyncMock()

        with (
            patch.object(
                worker,
                "_get_running_or_pending_flow_runs",
                AsyncMock(return_value=[pending_run]),
            ),
            patch.object(
                worker,
                "_get_slurm_job_states",
                AsyncMock(return_value={"123": "COMPLETED"}),
            ),
        ):
            await worker._renew_pending_slurm_concurrency_leases()

        worker._client.renew_concurrency_lease.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_get_slurm_configuration_from_env(self, sample_env_vars):
        """Test Slurm configuration generation from environment variables."""
        worker = SlurmWorker(work_pool_name="test-pool")

        config = await worker._get_slurm_configuration()

        assert str(config.host).rstrip("/") == sample_env_vars["PREFECT_SLURM_API_URL"]
        assert config.api_key["user"] == sample_env_vars["PREFECT_SLURM_USER_NAME"]
        assert config.api_key["token"] == sample_env_vars["PREFECT_SLURM_USER_TOKEN"]

    @pytest.mark.asyncio
    async def test_get_slurm_configuration_defaults(self, monkeypatch):
        """Test Slurm configuration with default values and environment token."""
        # Set required fields with default values
        monkeypatch.setenv("PREFECT_SLURM_API_URL", "http://localhost:6820")
        monkeypatch.setenv("PREFECT_SLURM_USER_NAME", "testuser")
        monkeypatch.setenv("PREFECT_SLURM_USER_TOKEN", "env_token")

        worker = SlurmWorker(work_pool_name="test-pool")
        config = await worker._get_slurm_configuration()

        assert (
            str(config.host) == "http://localhost:6820/"
        )  # Default value with trailing slash
        assert config.api_key["user"] == "testuser"
        assert config.api_key["token"] == "env_token"

    def test_filter_zombie_flow_runs_no_zombies(
        self, sample_flow_runs, sample_slurm_jobs
    ):
        """Test zombie detection when all flows have corresponding Slurm jobs."""
        zombies = SlurmWorker._filter_zombie_flow_runs(
            sample_flow_runs, sample_slurm_jobs
        )

        assert len(zombies) == 0

    def test_filter_zombie_flow_runs_with_zombies(
        self, sample_flow_runs, sample_slurm_jobs
    ):
        """Test zombie detection when some flows don't have corresponding Slurm jobs."""
        # Add a flow run without corresponding Slurm job
        zombie_id = uuid4()
        zombie_flow = FlowRun(
            id=zombie_id,
            flow_id=zombie_id,
            name="zombie-flow",
            state=Running(),
            infrastructure_pid="88888",  # This job ID doesn't exist in sample_slurm_jobs
        )
        all_flows = sample_flow_runs + [zombie_flow]

        zombies = SlurmWorker._filter_zombie_flow_runs(all_flows, sample_slurm_jobs)

        assert len(zombies) == 0

    def test_filter_zombie_flow_runs_none_infrastructure_pid(self, sample_slurm_jobs):
        """Test zombie detection with flows that have None infrastructure_pid."""
        flow_id = uuid4()
        flows_with_none_pid = [
            FlowRun(
                id=flow_id,
                flow_id=flow_id,
                name="flow-without-pid",
                state=Pending(),  # PENDING flows with None PID are considered zombies
                infrastructure_pid=None,
            )
        ]

        zombies = SlurmWorker._filter_zombie_flow_runs(
            flows_with_none_pid, sample_slurm_jobs
        )

        assert len(zombies) == 0

    def test_filter_zombie_flow_runs_mixed_scenarios(self, sample_slurm_jobs):
        """Test zombie detection with mixed scenarios."""
        flows = []
        for name, pid, state in [
            ("normal", "12345", Running()),
            ("zombie1", "77777", Running()),
            ("no-pid", None, Pending()),  # PENDING with None PID = zombie
            ("zombie2", "88888", Running()),
        ]:
            flow_id = uuid4()
            flows.append(
                FlowRun(
                    id=flow_id,
                    flow_id=flow_id,
                    name=name,
                    state=state,
                    infrastructure_pid=pid,
                )
            )

        zombies = SlurmWorker._filter_zombie_flow_runs(flows, sample_slurm_jobs)

        assert len(zombies) == 1  # zombie1, no-pid, zombie2
        zombie_pids = {flow.infrastructure_pid for flow in zombies}
        assert zombie_pids == {"77777"}

    def test_retry_variables_default(self):
        """Test default retry env vars"""
        for key in [
            "PREFECT_SLURM_MAX_ATTEMPTS",
            "PREFECT_SLURM_RETRY_MIN_DELAY_SECONDS",
            "PREFECT_SLURM_RETRY_MIN_DELAY_JITTER_SECONDS",
            "PREFECT_SLURM_RETRY_MAX_DELAY_JITTER_SECONDS",
        ]:
            os.environ.pop(key, None)

        import prefect_slurm.worker

        importlib.reload(prefect_slurm.worker)

        assert prefect_slurm.worker.MAX_ATTEMPTS == 3
        assert prefect_slurm.worker.RETRY_MIN_DELAY_SECONDS == 10
        assert prefect_slurm.worker.RETRY_MIN_DELAY_JITTER_SECONDS == 0
        assert prefect_slurm.worker.RETRY_MAX_DELAY_JITTER_SECONDS == 20

    @pytest.mark.parametrize(
        "env_vars,expected",
        [
            (
                {
                    "PREFECT_SLURM_MAX_ATTEMPTS": "5",
                    "PREFECT_SLURM_RETRY_MIN_DELAY_SECONDS": "15",
                    "PREFECT_SLURM_RETRY_MIN_DELAY_JITTER_SECONDS": "5",
                    "PREFECT_SLURM_RETRY_MAX_DELAY_JITTER_SECONDS": "30",
                },
                {"max": 5, "min_delay": 15, "min_jitter": 5, "max_jitter": 30},
            ),
        ],
    )
    def test_retry_variables_custom(self, env_vars, expected, monkeypatch):
        """Test that custom environment variables are read correctly."""
        for key, value in env_vars.items():
            monkeypatch.setenv(key, value)

        import prefect_slurm.worker

        importlib.reload(prefect_slurm.worker)

        assert prefect_slurm.worker.MAX_ATTEMPTS == expected["max"]
        assert prefect_slurm.worker.RETRY_MIN_DELAY_SECONDS == expected["min_delay"]
        assert (
            prefect_slurm.worker.RETRY_MIN_DELAY_JITTER_SECONDS
            == expected["min_jitter"]
        )
        assert (
            prefect_slurm.worker.RETRY_MAX_DELAY_JITTER_SECONDS
            == expected["max_jitter"]
        )

    @pytest.mark.asyncio
    async def test_submit_slurm_job_success(
        self, sample_job_spec, mock_slurpy_response
    ):
        """Test successful Slurm job submission."""

        with patch("slurpy.v0042.asyncio.ApiClient") as mock_api_client:
            mock_client = AsyncMock()
            mock_api = AsyncMock()
            mock_api.post_job_submit.return_value = mock_slurpy_response
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_api_client.return_value = mock_client

            # Mock the API creation
            with patch("slurpy.v0042.asyncio.SlurmApi", return_value=mock_api):
                worker = SlurmWorker(work_pool_name="test-pool")
                result = await worker._submit_slurm_job(sample_job_spec)

            assert result == mock_slurpy_response
            mock_api.post_job_submit.assert_called_once_with(
                job_submit_req=sample_job_spec
            )

    @pytest.mark.asyncio
    async def test_submit_slurm_job_api_exception(self, sample_job_spec):
        """Test Slurm job submission with API exception."""

        with patch("slurpy.v0042.asyncio.ApiClient") as mock_api_client:
            mock_client = AsyncMock()
            mock_api = AsyncMock()
            mock_api.post_job_submit.side_effect = ApiException("API Error")
            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_api_client.return_value = mock_client

            with patch("slurpy.v0042.asyncio.SlurmApi", return_value=mock_api):
                with patch("asyncio.sleep"):
                    worker = SlurmWorker(work_pool_name="test-pool")

                    with pytest.raises(InfrastructureError):
                        await worker._submit_slurm_job(sample_job_spec)

    @pytest.mark.asyncio
    async def test_submit_slurm_job_retry_then_success(
        self, sample_job_spec, mock_slurpy_response
    ):
        """Test job submission retries on ApiException then succeeds."""
        with patch("slurpy.v0042.asyncio.ApiClient") as mock_api_client:
            mock_client = AsyncMock()
            mock_api = AsyncMock()

            # Fail twice, then succeed
            mock_api.post_job_submit.side_effect = [
                ApiException("500: Server Error"),
                ApiException("503: Service Unavailable"),
                mock_slurpy_response,  # Success on 3rd attempt
            ]

            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_api_client.return_value = mock_client

            with patch("slurpy.v0042.asyncio.SlurmApi", return_value=mock_api):
                with patch("asyncio.sleep"):  # Speed up test
                    worker = SlurmWorker(work_pool_name="test-pool")
                    result = await worker._submit_slurm_job(sample_job_spec)

            # Verify success after retries
            assert result == mock_slurpy_response

            # Verify 3 attempts were made
            assert mock_api.post_job_submit.call_count == 3

    @pytest.mark.asyncio
    async def test_submit_slurm_job_retry_exhausted(self, sample_job_spec):
        """Test job submission fails after exhausting all retries."""
        with patch("slurpy.v0042.asyncio.ApiClient") as mock_api_client:
            mock_client = AsyncMock()
            mock_api = AsyncMock()

            # Always fail
            mock_api.post_job_submit.side_effect = ApiException(
                "500: Persistent Server Error"
            )

            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_api_client.return_value = mock_client

            with patch("slurpy.v0042.asyncio.SlurmApi", return_value=mock_api):
                with patch("asyncio.sleep"):  # Speed up test
                    worker = SlurmWorker(work_pool_name="test-pool")

                    # Should raise InfrastructureError after all retries
                    with pytest.raises(InfrastructureError) as exc_info:
                        await worker._submit_slurm_job(sample_job_spec)

            # Verify MAX_ATTEMPTS (3) were made
            import prefect_slurm.worker

            importlib.reload(prefect_slurm.worker)

            assert (
                mock_api.post_job_submit.call_count == prefect_slurm.worker.MAX_ATTEMPTS
            )

            # Verify InfrastructureError wraps the ApiException
            assert isinstance(exc_info.value.args[0], ApiException)

    @pytest.mark.asyncio
    async def test_submit_slurm_job_retry_on_generic_exception(
        self, sample_job_spec, mock_slurpy_response
    ):
        """Test that generic exceptions are also retried."""
        with patch("slurpy.v0042.asyncio.ApiClient") as mock_api_client:
            mock_client = AsyncMock()
            mock_api = AsyncMock()

            # Fail with generic exception, then succeed
            mock_api.post_job_submit.side_effect = [
                ConnectionError("Network error"),
                mock_slurpy_response,
            ]

            mock_client.__aenter__.return_value = mock_client
            mock_client.__aexit__.return_value = None
            mock_api_client.return_value = mock_client

            with patch("slurpy.v0042.asyncio.SlurmApi", return_value=mock_api):
                with patch("asyncio.sleep"):  # Speed up test
                    worker = SlurmWorker(work_pool_name="test-pool")
                    result = await worker._submit_slurm_job(sample_job_spec)

            # Verify success after retry
            assert result == mock_slurpy_response

            # Verify exactly 2 attempts
            assert mock_api.post_job_submit.call_count == 2

    @pytest.mark.asyncio
    async def test_run_method_success(
        self, sample_flow_run, sample_slurm_configuration, mock_slurpy_response
    ):
        """Test successful flow run execution."""
        worker = SlurmWorker(work_pool_name="test-pool")
        task_status = Mock()

        with patch.object(
            worker, "_submit_slurm_job", return_value=mock_slurpy_response
        ) as mock_submit:
            with patch.object(worker, "get_flow_run_logger") as mock_logger:
                mock_logger.return_value = Mock()

                result = await worker.run(
                    flow_run=sample_flow_run,
                    configuration=sample_slurm_configuration,
                    task_status=task_status,
                )

        # Verify job submission was called
        mock_submit.assert_called_once()

        # Verify task status was updated
        task_status.started.assert_called_once_with(mock_slurpy_response.job_id)

        # Verify result
        assert isinstance(result, BaseWorkerResult)
        assert result.status_code == 0
        assert result.identifier == str(mock_slurpy_response.job_id)

    @pytest.mark.asyncio
    async def test_run_method_without_task_status(
        self, sample_flow_run, sample_slurm_configuration, mock_slurpy_response
    ):
        """Test flow run execution without task status."""
        worker = SlurmWorker(work_pool_name="test-pool")

        with patch.object(
            worker, "_submit_slurm_job", return_value=mock_slurpy_response
        ):
            with patch.object(worker, "get_flow_run_logger") as mock_logger:
                mock_logger.return_value = Mock()

                result = await worker.run(
                    flow_run=sample_flow_run,
                    configuration=sample_slurm_configuration,
                    task_status=None,
                )

        # Should not raise an exception
        assert isinstance(result, BaseWorkerResult)

    @pytest.mark.asyncio
    async def test_run_does_not_log_sensitive_environment_values(
        self,
        sample_flow_run,
        sample_slurm_configuration,
        mock_slurpy_response,
        monkeypatch,
        caplog,
    ):
        """Test sensitive environment values are masked in job specification logs
        via the RedactingFilter attached by get_flow_run_logger (not a manual call)."""
        sensitive_value = "dummy-prefect-api-password"
        auth_string = f"prefect-admin:{sensitive_value}"
        sample_slurm_configuration.env = {"PREFECT_API_AUTH_STRING": auth_string}
        monkeypatch.setenv("PREFECT_API_AUTH_STRING", auth_string)
        worker = SlurmWorker(work_pool_name="test-pool")

        caplog.set_level(logging.DEBUG, logger="prefect.flow_runs.worker")

        with patch.object(
            worker, "_submit_slurm_job", return_value=mock_slurpy_response
        ) as mock_submit:
            await worker.run(
                flow_run=sample_flow_run,
                configuration=sample_slurm_configuration,
            )

        submitted_job_spec = mock_submit.call_args.args[0]
        debug_records = [
            record
            for record in caplog.records
            if record.name == "prefect.flow_runs.worker"
            and record.getMessage().startswith("Slurm job specs:")
        ]

        assert len(debug_records) == 1
        logged_job_spec = debug_records[0].getMessage()

        assert "PREFECT_API_AUTH_STRING=********" in logged_job_spec
        assert sensitive_value not in logged_job_spec
        assert sensitive_value in repr(submitted_job_spec)

    def test_worker_logger_has_redacting_filter(self):
        """self._logger should get exactly one RedactingFilter attached."""
        worker = SlurmWorker(work_pool_name="test-pool", name="filter-test-worker")

        logger = _underlying_logger(worker._logger)

        assert sum(isinstance(f, RedactingFilter) for f in logger.filters) == 1

    def test_worker_logger_filter_not_duplicated_across_instances(self):
        """Re-instantiating a worker with the same name reuses a cached logger;
        the filter must not be attached more than once to it."""
        SlurmWorker(work_pool_name="test-pool", name="dedup-worker")
        worker2 = SlurmWorker(work_pool_name="test-pool", name="dedup-worker")

        logger = _underlying_logger(worker2._logger)

        assert sum(isinstance(f, RedactingFilter) for f in logger.filters) == 1

    def test_flow_run_logger_filter_not_duplicated_on_repeated_calls(
        self, sample_flow_run
    ):
        """get_flow_run_logger returns a new adapter each call, but it's backed by
        the same cached child logger; the filter must not accumulate duplicates."""
        worker = SlurmWorker(work_pool_name="test-pool")

        first = worker.get_flow_run_logger(sample_flow_run)
        second = worker.get_flow_run_logger(sample_flow_run)

        assert first.logger is second.logger
        assert sum(isinstance(f, RedactingFilter) for f in second.logger.filters) == 1

    def test_worker_class_attributes(self):
        """Test worker class attributes are set correctly."""
        assert SlurmWorker.type == "slurm"
        assert SlurmWorker.job_configuration is not None
        assert SlurmWorker.job_configuration_variables is not None
        assert SlurmWorker._documentation_url is not None
        assert SlurmWorker._logo_url is not None

    def test_type_consistency_between_fields(self, sample_flow_runs, sample_slurm_jobs):
        """Test type consistency in zombie detection with string/int conversion."""
        # Create flows with string PIDs
        string_pid_flows = []
        for name, pid in [("flow1", "12345"), ("flow2", "54321"), ("flow3", "77777")]:
            flow_id = uuid4()
            string_pid_flows.append(
                FlowRun(
                    id=flow_id,
                    flow_id=flow_id,
                    name=name,
                    state=Running(),
                    infrastructure_pid=pid,
                )
            )

        # Create job states dict (string keys to match infrastructure_pid)
        job_states = {"12345": "RUNNING", "67890": "RUNNING", "77777": "TERMINATED"}

        zombies = SlurmWorker._filter_zombie_flow_runs(string_pid_flows, job_states)

        # flow1 should match (12345), flow2 should be zombie (54321 - not in job_states)
        assert len(zombies) == 1
        assert zombies[0].infrastructure_pid == "77777"

    @pytest.mark.parametrize(
        "infrastructure_pid,state_class,should_be_zombie",
        [
            ("12345", Running, False),  # Exists as RUNNING in sample_slurm_jobs
            ("67890", Running, False),  # Exists as RUNNING in sample_slurm_jobs
            ("99999", Running, False),  # Job doesn't exist in RUNNING jobs
            ("77777", Running, True),  # Job doesn't exist
            (
                None,
                Running,
                False,
            ),  # None PID with RUNNING state = zombie (no matching job)
            (None, "Pending", False),  # None PID with PENDING state = zombie
        ],
    )
    def test_zombie_detection_edge_cases(
        self, infrastructure_pid, state_class, should_be_zombie, sample_slurm_jobs
    ):
        """Test zombie detection with various edge cases."""
        # Handle string state class names for import
        if state_class == "Pending":
            state = Pending()
        else:
            state = state_class()

        flow_id = uuid4()
        flow = FlowRun(
            id=flow_id,
            flow_id=flow_id,
            name="test-flow",
            state=state,
            infrastructure_pid=infrastructure_pid,
        )

        zombies = SlurmWorker._filter_zombie_flow_runs([flow], sample_slurm_jobs)

        if should_be_zombie:
            assert len(zombies) == 1
            assert zombies[0].infrastructure_pid == infrastructure_pid
        else:
            assert len(zombies) == 0

    @pytest.mark.asyncio
    async def test_detect_slurm_api_version_success_v0042(self, monkeypatch):
        """Test successful API version detection for v0.0.42."""
        monkeypatch.setenv("PREFECT_SLURM_USER_NAME", "testuser")
        monkeypatch.setenv("PREFECT_SLURM_API_URL", "http://localhost:6820")
        monkeypatch.setenv("PREFECT_SLURM_USER_TOKEN", "valid.jwt.token")

        worker = SlurmWorker(work_pool_name="test-pool")

        # Mock successful ping for v0042
        with patch("importlib.import_module") as mock_import:
            mock_slurpy_module = Mock()
            mock_rest_module = Mock()
            mock_import.side_effect = lambda name: (
                mock_slurpy_module if "rest" not in name else mock_rest_module
            )

            mock_config = Mock()
            mock_config.api_key = {}
            mock_client = AsyncMock()
            mock_api = AsyncMock()
            mock_api.get_ping = AsyncMock()

            mock_slurpy_module.Configuration = Mock(return_value=mock_config)
            mock_slurpy_module.ApiClient = Mock(return_value=mock_client)
            mock_slurpy_module.SlurmApi = Mock(return_value=mock_api)
            mock_slurpy_module.JobInfo = Mock()
            mock_rest_module.ApiException = Exception

            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)

            version = await worker._detect_slurm_api_version()

            assert version == "v0042"
            assert worker._Configuration == mock_slurpy_module.Configuration
            assert worker._ApiClient == mock_slurpy_module.ApiClient
            assert worker._SlurmApi == mock_slurpy_module.SlurmApi

    @pytest.mark.asyncio
    async def test_detect_slurm_api_version_fallback_to_v0041(self, monkeypatch):
        """Test API version detection falling back to v0041."""
        monkeypatch.setenv("PREFECT_SLURM_USER_NAME", "testuser")
        monkeypatch.setenv("PREFECT_SLURM_API_URL", "http://localhost:6820")
        monkeypatch.setenv("PREFECT_SLURM_USER_TOKEN", "valid.jwt.token")

        worker = SlurmWorker(work_pool_name="test-pool")

        # Mock v0042 failing, v0041 succeeding
        with patch("importlib.import_module") as mock_import:
            call_count = [0]

            def import_side_effect(name):
                call_count[0] += 1
                if "v0042" in name and call_count[0] <= 2:
                    raise ImportError("v0042 not available")

                mock_module = Mock()
                if "rest" in name:
                    mock_module.ApiException = Exception
                else:
                    mock_config = Mock()
                    mock_config.api_key = {}
                    mock_client = AsyncMock()
                    mock_api = AsyncMock()
                    mock_api.get_ping = AsyncMock()

                    mock_module.Configuration = Mock(return_value=mock_config)
                    mock_module.ApiClient = Mock(return_value=mock_client)
                    mock_module.SlurmApi = Mock(return_value=mock_api)
                    mock_module.JobInfo = Mock()

                    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                    mock_client.__aexit__ = AsyncMock(return_value=None)

                return mock_module

            mock_import.side_effect = import_side_effect

            version = await worker._detect_slurm_api_version()

            assert version == "v0041"

    @pytest.mark.asyncio
    async def test_detect_slurm_api_version_fallback_to_v0040(self, monkeypatch):
        """Test API version detection falling back to v0040."""
        monkeypatch.setenv("PREFECT_SLURM_USER_NAME", "testuser")
        monkeypatch.setenv("PREFECT_SLURM_API_URL", "http://localhost:6820")
        monkeypatch.setenv("PREFECT_SLURM_USER_TOKEN", "valid.jwt.token")

        worker = SlurmWorker(work_pool_name="test-pool")

        # Mock v0042 and v0041 failing, v0040 succeeding
        with patch("importlib.import_module") as mock_import:
            call_count = [0]

            def import_side_effect(name):
                call_count[0] += 1
                if ("v0042" in name or "v0041" in name) and call_count[0] <= 4:
                    raise ImportError("version not available")

                mock_module = Mock()
                if "rest" in name:
                    mock_module.ApiException = Exception
                else:
                    mock_config = Mock()
                    mock_config.api_key = {}
                    mock_client = AsyncMock()
                    mock_api = AsyncMock()
                    mock_api.get_ping = AsyncMock()

                    mock_module.Configuration = Mock(return_value=mock_config)
                    mock_module.ApiClient = Mock(return_value=mock_client)
                    mock_module.SlurmApi = Mock(return_value=mock_api)
                    mock_module.JobInfo = Mock()

                    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                    mock_client.__aexit__ = AsyncMock(return_value=None)

                return mock_module

            mock_import.side_effect = import_side_effect

            version = await worker._detect_slurm_api_version()

            assert version == "v0040"

    @pytest.mark.asyncio
    async def test_detect_slurm_api_version_no_compatible_version(self, monkeypatch):
        """Test API version detection when no compatible version is found."""
        monkeypatch.setenv("PREFECT_SLURM_USER_NAME", "testuser")
        monkeypatch.setenv("PREFECT_SLURM_API_URL", "http://localhost:6820")
        monkeypatch.setenv("PREFECT_SLURM_USER_TOKEN", "valid.jwt.token")

        worker = SlurmWorker(work_pool_name="test-pool")

        # Mock all versions failing
        with patch(
            "importlib.import_module", side_effect=ImportError("No versions available")
        ):
            with pytest.raises(
                ValueError, match="No compatible Slurm API version found"
            ):
                await worker._detect_slurm_api_version()

    @pytest.mark.asyncio
    async def test_detect_slurm_api_version_all_ping_fail(self, monkeypatch):
        """Test API version detection when all ping endpoints fail."""
        monkeypatch.setenv("PREFECT_SLURM_USER_NAME", "testuser")
        monkeypatch.setenv("PREFECT_SLURM_API_URL", "http://localhost:6820")
        monkeypatch.setenv("PREFECT_SLURM_USER_TOKEN", "valid.jwt.token")

        worker = SlurmWorker(work_pool_name="test-pool")

        # Mock all versions available but all pings fail
        with patch("importlib.import_module") as mock_import:

            def import_side_effect(name):
                mock_module = Mock()

                if "rest" in name:
                    mock_module.ApiException = Exception
                else:
                    mock_config = Mock()
                    mock_config.api_key = {}
                    mock_client = AsyncMock()
                    mock_api = AsyncMock()
                    mock_api.get_ping = AsyncMock(side_effect=Exception("Ping failed"))

                    mock_module.Configuration = Mock(return_value=mock_config)
                    mock_module.ApiClient = Mock(return_value=mock_client)
                    mock_module.SlurmApi = Mock(return_value=mock_api)
                    mock_module.JobInfo = Mock()

                    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                    mock_client.__aexit__ = AsyncMock(return_value=None)

                return mock_module

            mock_import.side_effect = import_side_effect

            with pytest.raises(
                ValueError, match="No compatible Slurm API version found"
            ):
                await worker._detect_slurm_api_version()

    @pytest.mark.asyncio
    async def test_get_slurm_job_states_success(self, monkeypatch):
        """Test successful retrieval of Slurm job states."""
        monkeypatch.setenv("PREFECT_SLURM_USER_NAME", "testuser")
        monkeypatch.setenv("PREFECT_SLURM_API_URL", "http://localhost:6820")
        monkeypatch.setenv("PREFECT_SLURM_USER_TOKEN", "valid.jwt.token")

        worker = SlurmWorker(work_pool_name="test-pool")
        job_ids = ["12345", "67890", "99999"]

        def create_mock_response(job_id: str):
            mock_response = AsyncMock()

            responses = {
                "12345": {"jobs": [{"job_id": "12345", "state": ["RUNNING"]}]},
                "67890": {"jobs": [{"job_id": "67890", "state": ["COMPLETED"]}]},
                "99999": {"jobs": [{"job_id": "99999", "state": None}]},
            }
            mock_response.json = AsyncMock(
                return_value=responses.get(job_id, {"jobs": []})
            )

            return mock_response

        with patch.object(worker, "_get_slurm_configuration") as mock_get_config:
            mock_config = Mock()
            mock_config.api_key = {}
            mock_get_config.return_value = mock_config

            with patch.object(worker, "_ApiClient") as mock_api_client:
                mock_client = AsyncMock()
                mock_api = AsyncMock()
                mock_api.get_jobs_state_without_preload_content.side_effect = (
                    create_mock_response
                )

                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_api_client.return_value = mock_client

                with patch.object(worker, "_SlurmApi", return_value=mock_api):
                    result = await worker._get_slurm_job_states(job_ids)

                    expected = {
                        "12345": "RUNNING",
                        "67890": "COMPLETED",
                        "99999": "UNKNOWN",
                    }
                    assert result == expected

                    # Verify the job_id query param formatting
                    assert (
                        mock_api.get_jobs_state_without_preload_content.call_count == 3
                    )
                    called_job_ids = [
                        call.args[0]
                        for call in mock_api.get_jobs_state_without_preload_content.call_args_list
                    ]
                    assert set(called_job_ids) == {"12345", "67890", "99999"}

    @pytest.mark.asyncio
    async def test_get_slurm_job_states_empty_list(self, monkeypatch):
        """Test _get_slurm_job_states with empty job ID list returns empty dict."""
        monkeypatch.setenv("PREFECT_SLURM_USER_NAME", "testuser")
        monkeypatch.setenv("PREFECT_SLURM_API_URL", "http://localhost:6820")
        monkeypatch.setenv("PREFECT_SLURM_USER_TOKEN", "valid.jwt.token")

        worker = SlurmWorker(work_pool_name="test-pool")
        job_ids = []

        # Empty list should return empty dict
        result = await worker._get_slurm_job_states(job_ids)
        assert result == {}

    @pytest.mark.asyncio
    async def test_get_slurm_job_states_single_job(self, monkeypatch):
        """Test _get_slurm_job_states with single job ID."""
        monkeypatch.setenv("PREFECT_SLURM_USER_NAME", "testuser")
        monkeypatch.setenv("PREFECT_SLURM_API_URL", "http://localhost:6820")
        monkeypatch.setenv("PREFECT_SLURM_USER_TOKEN", "valid.jwt.token")

        worker = SlurmWorker(work_pool_name="test-pool")
        job_ids = ["12345"]

        # Mock the API response
        mock_response = AsyncMock()
        mock_response.json = AsyncMock(
            return_value={"jobs": [{"job_id": "12345", "state": ["PENDING"]}]}
        )

        with patch.object(worker, "_get_slurm_configuration") as mock_get_config:
            mock_config = Mock()
            mock_config.api_key = {}
            mock_get_config.return_value = mock_config

            with patch.object(worker, "_ApiClient") as mock_api_client:
                mock_client = AsyncMock()
                mock_api = AsyncMock()
                mock_api.get_jobs_state_without_preload_content.return_value = (
                    mock_response
                )

                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=None)
                mock_api_client.return_value = mock_client

                with patch.object(worker, "_SlurmApi", return_value=mock_api):
                    result = await worker._get_slurm_job_states(job_ids)

                    expected = {"12345": "PENDING"}
                    assert result == expected

                    mock_api.get_jobs_state_without_preload_content.assert_called_once_with(
                        "12345"
                    )

    @pytest.mark.asyncio
    async def test_get_running_or_pending_flow_runs_success(self):
        """Test successful retrieval of running/pending flow runs."""
        worker = SlurmWorker(work_pool_name="test-pool")
        worker._work_queues = {"queue1", "queue2"}

        # Mock flow runs
        mock_flow_runs = [Mock(), Mock()]

        # Mock the client and work pool
        mock_client = AsyncMock()
        mock_client.read_flow_runs = AsyncMock(return_value=mock_flow_runs)
        worker._client = mock_client

        mock_work_pool = Mock()
        mock_work_pool.name = "test-pool"
        worker._work_pool = mock_work_pool

        result = await worker._get_running_or_pending_flow_runs()

        assert result == mock_flow_runs

        # Verify the filters used
        call_args = mock_client.read_flow_runs.call_args
        assert call_args is not None

        # Check work pool filter
        work_pool_filter = call_args.kwargs["work_pool_filter"]
        assert work_pool_filter.name.any_ == ["test-pool"]

        # Check work queue filter
        work_queue_filter = call_args.kwargs["work_queue_filter"]
        assert set(work_queue_filter.name.any_) == {"queue1", "queue2"}

        # Check flow run state filter
        flow_run_filter = call_args.kwargs["flow_run_filter"]
        assert flow_run_filter.state.type.any_ == [StateType.RUNNING, StateType.PENDING]

    @pytest.mark.asyncio
    async def test_get_running_or_pending_flow_runs_default_queue(self):
        """Test retrieval when no specific work queues are set."""
        worker = SlurmWorker(work_pool_name="test-pool")
        worker._work_queues = None  # No specific queues

        mock_flow_runs = []

        # Mock the client and work pool
        mock_client = AsyncMock()
        mock_client.read_flow_runs = AsyncMock(return_value=mock_flow_runs)
        worker._client = mock_client

        mock_work_pool = Mock()
        mock_work_pool.name = "test-pool"
        worker._work_pool = mock_work_pool

        result = await worker._get_running_or_pending_flow_runs()

        assert result == mock_flow_runs

        # Verify default queue is used
        call_args = mock_client.read_flow_runs.call_args
        work_queue_filter = call_args.kwargs["work_queue_filter"]
        assert work_queue_filter.name.any_ == ["default"]

    @pytest.mark.asyncio
    async def test_mark_zombie_flow_runs_as_crashed_no_flow_runs(self):
        """Test zombie detection when no flow runs exist."""
        worker = SlurmWorker(work_pool_name="test-pool")

        with patch.object(
            worker, "_get_running_or_pending_flow_runs"
        ) as mock_get_flows:
            mock_get_flows.return_value = []

            # Should complete without errors and log 0 zombies
            await worker._mark_zombie_flow_runs_as_crashed()

            mock_get_flows.assert_called_once()

    @pytest.mark.asyncio
    async def test_mark_zombie_flow_runs_as_crashed_no_zombies(self, sample_flow_runs):
        """Test zombie detection when all flows are healthy."""
        worker = SlurmWorker(work_pool_name="test-pool")

        # Mock that all flows have corresponding running Slurm jobs
        job_states = {"12345": "RUNNING", "67890": "RUNNING"}

        with patch.object(
            worker, "_get_running_or_pending_flow_runs"
        ) as mock_get_flows:
            with patch.object(worker, "_get_slurm_job_states") as mock_get_states:
                with patch.object(worker, "_propose_crashed_state") as mock_crash:
                    mock_get_flows.return_value = sample_flow_runs
                    mock_get_states.return_value = job_states

                    await worker._mark_zombie_flow_runs_as_crashed()

                    mock_get_flows.assert_called_once()
                    mock_get_states.assert_called_once_with(["12345", "67890"])
                    mock_crash.assert_not_called()

    @pytest.mark.asyncio
    async def test_mark_zombie_flow_runs_as_crashed_with_zombies(self):
        """Test zombie detection and crash marking when zombies exist."""
        worker = SlurmWorker(work_pool_name="test-pool")

        # Create flow runs with some zombies
        zombie_id = uuid4()
        healthy_id = uuid4()
        zombie_flow = FlowRun(
            id=zombie_id,
            flow_id=zombie_id,
            name="zombie-flow",
            state=Running(),
            infrastructure_pid="88888",
        )
        healthy_flow = FlowRun(
            id=healthy_id,
            flow_id=healthy_id,
            name="healthy-flow",
            state=Running(),
            infrastructure_pid="12345",
        )
        flow_runs = [zombie_flow, healthy_flow]

        # Mock job states - zombie job doesn't exist, healthy job is running
        job_states = {
            "12345": "RUNNING",
            "88888": "TERMINATED",
        }

        with patch.object(
            worker, "_get_running_or_pending_flow_runs"
        ) as mock_get_flows:
            with patch.object(worker, "_get_slurm_job_states") as mock_get_states:
                with patch.object(worker, "_propose_crashed_state") as mock_crash:
                    mock_get_flows.return_value = flow_runs
                    mock_get_states.return_value = job_states

                    await worker._mark_zombie_flow_runs_as_crashed()

                    mock_get_flows.assert_called_once()
                    mock_get_states.assert_called_once_with(["88888", "12345"])

                    # Only zombie flow should be marked as crashed
                    mock_crash.assert_called_once()
                    crash_call_args = mock_crash.call_args
                    assert crash_call_args.kwargs["flow_run"] == zombie_flow
                    assert "88888" in crash_call_args.kwargs["message"]

    @pytest.mark.asyncio
    async def test_mark_zombie_flow_runs_as_crashed_mixed_states(self):
        """Test zombie detection with mixed flow and job states."""
        worker = SlurmWorker(work_pool_name="test-pool")

        # Create flows with different scenarios
        flow_ids = [uuid4() for _ in range(5)]
        flows = [
            FlowRun(
                id=flow_ids[0],
                flow_id=flow_ids[0],
                name="running-ok",
                state=Running(),
                infrastructure_pid="100",
            ),
            FlowRun(
                id=flow_ids[1],
                flow_id=flow_ids[1],
                name="running-zombie",
                state=Running(),
                infrastructure_pid="200",
            ),
            FlowRun(
                id=flow_ids[2],
                flow_id=flow_ids[2],
                name="pending-ok",
                state=Pending(),
                infrastructure_pid="300",
            ),
            FlowRun(
                id=flow_ids[3],
                flow_id=flow_ids[3],
                name="pending-zombie",
                state=Pending(),
                infrastructure_pid="400",
            ),
            FlowRun(
                id=flow_ids[4],
                flow_id=flow_ids[4],
                name="no-pid",
                state=Pending(),
                infrastructure_pid=None,
            ),
        ]

        # Job states: 100=RUNNING (ok), 200=COMPLETED (zombie), 300=PENDING (ok), 400=missing (zombie)
        job_states = {
            "100": "RUNNING",
            "200": "COMPLETED",
            "300": "PENDING",
            # 400 missing, None not in dict
        }

        with patch.object(
            worker, "_get_running_or_pending_flow_runs"
        ) as mock_get_flows:
            with patch.object(worker, "_get_slurm_job_states") as mock_get_states:
                with patch.object(worker, "_propose_crashed_state") as mock_crash:
                    mock_get_flows.return_value = flows
                    mock_get_states.return_value = job_states

                    await worker._mark_zombie_flow_runs_as_crashed()

                    assert mock_crash.call_count == 1

                    crashed_pids = {
                        call.kwargs["flow_run"].infrastructure_pid
                        for call in mock_crash.call_args_list
                    }
                    assert crashed_pids == {"200"}
