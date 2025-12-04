"""
Unit tests for SlurmWorker class methods.
"""

from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import pytest
from prefect.client.schemas import FlowRun, StateType
from prefect.exceptions import InfrastructureError
from prefect.states import Pending, Running
from prefect.workers.base import BaseWorkerResult
from slurpy.v0042.asyncio.rest import ApiException

from prefect_slurm.worker import SlurmWorker


@pytest.mark.unit
class TestSlurmWorker:
    """Test cases for SlurmWorker class methods."""

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

        assert len(zombies) == 1
        assert zombies[0].infrastructure_pid == "88888"
        assert zombies[0].name == "zombie-flow"

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

        assert len(zombies) == 1  # PENDING flows with None PID are zombies

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

        assert len(zombies) == 3  # zombie1, no-pid, zombie2
        zombie_pids = {flow.infrastructure_pid for flow in zombies}
        assert zombie_pids == {"77777", None, "88888"}

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
                worker = SlurmWorker(work_pool_name="test-pool")

                with pytest.raises(InfrastructureError):
                    await worker._submit_slurm_job(sample_job_spec)

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
        for name, pid in [("flow1", "12345"), ("flow2", "54321")]:
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
        job_states = {"12345": "RUNNING", "67890": "RUNNING"}

        zombies = SlurmWorker._filter_zombie_flow_runs(string_pid_flows, job_states)

        # flow1 should match (12345), flow2 should be zombie (54321 - not in job_states)
        assert len(zombies) == 1
        assert zombies[0].infrastructure_pid == "54321"

    @pytest.mark.parametrize(
        "infrastructure_pid,state_class,should_be_zombie",
        [
            ("12345", Running, False),  # Exists as RUNNING in sample_slurm_jobs
            ("67890", Running, False),  # Exists as RUNNING in sample_slurm_jobs
            ("99999", Running, True),  # Job doesn't exist in RUNNING jobs
            ("88888", Running, True),  # Job doesn't exist
            (
                None,
                Running,
                True,
            ),  # None PID with RUNNING state = zombie (no matching job)
            (None, "Pending", True),  # None PID with PENDING state = zombie
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

                    expected = {"12345": "RUNNING", "67890": "COMPLETED", "99999": None}
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
        job_states = {"12345": "RUNNING"}  # 88888 missing = zombie

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

                    # Should identify 3 zombies: running-zombie, pending-zombie, no-pid
                    assert mock_crash.call_count == 3

                    crashed_pids = {
                        call.kwargs["flow_run"].infrastructure_pid
                        for call in mock_crash.call_args_list
                    }
                    assert crashed_pids == {"200", "400", None}
