# Copyright 2025 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import importlib
import threading
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

import anyio
from anyio.abc import TaskStatus
from prefect.client.orchestration import get_client
from prefect.client.schemas import FlowRun, StateType
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterState,
    FlowRunFilterStateType,
    WorkPoolFilter,
    WorkPoolFilterName,
    WorkQueueFilter,
    WorkQueueFilterName,
)
from prefect.exceptions import (
    InfrastructureError,
)
from prefect.settings import (
    PREFECT_API_URL,
    PREFECT_TEST_MODE,
    PREFECT_WORKER_QUERY_SECONDS,
)
from prefect.utilities.services import critical_service_loop
from prefect.workers.base import BaseWorker, BaseWorkerResult

from prefect_slurm.config import SlurmWorkerConfiguration, SlurmWorkerTemplateVariables
from prefect_slurm.settings import WorkerSettings

if TYPE_CHECKING:
    import slurpy.v0042.asyncio as slurpy
    from prefect.client.schemas import FlowRun


class SlurmWorker(
    BaseWorker[SlurmWorkerConfiguration, SlurmWorkerTemplateVariables, BaseWorkerResult]
):
    """A Prefect worker that submits flow runs as Slurm jobs.

    This worker runs on a Slurm submitter node and submits each flow run as a new
    Slurm job via the Slurm REST API.
    """

    type: str = "slurm"
    job_configuration = SlurmWorkerConfiguration
    job_configuration_variables = SlurmWorkerTemplateVariables
    _documentation_url = "https://www.github.com/EBI-metagenomics/prefect-slurm"
    _logo_url = "https://www.github.com/EBI-metagenomics/prefect-slurm/resources/prefect-slurm-logo.png"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Set default classes as fallback, mainly because of tests

        slurpy_module = importlib.import_module("slurpy.v0042.asyncio")
        rest_module = importlib.import_module("slurpy.v0042.asyncio.rest")

        # Store the classes we need
        self._Configuration = slurpy_module.Configuration
        self._ApiClient = slurpy_module.ApiClient
        self._SlurmApi = slurpy_module.SlurmApi
        self._ApiException = rest_module.ApiException
        self._JobInfo = slurpy_module.JobInfo

        self._settings = WorkerSettings()

    async def start(
        self,
        run_once: bool = False,
        with_healthcheck: bool = False,
        printer: Callable[..., None] = print,
    ) -> None:
        """
        Starts the worker and runs the main worker loops.

        By default, the worker will run loops to poll for scheduled/cancelled flow
        runs and sync with the Prefect API server.

        If `run_once` is set, the worker will only run each loop once and then return.

        If `with_healthcheck` is set, the worker will start a healthcheck server which
        can be used to determine if the worker is still polling for flow runs and restart
        the worker if necessary.

        :param run_once: If set, the worker will only run each loop once then return.
        :param with_healthcheck: If set, the worker will start a healthcheck server.
        :param printer: A `print`-like function where logs will be reported.
        """
        healthcheck_server = None
        healthcheck_thread = None
        try:
            async with self as worker:
                # schedule the scheduled flow run polling loop
                async with anyio.create_task_group() as loops_task_group:
                    loops_task_group.start_soon(
                        partial(
                            critical_service_loop,
                            workload=self.get_and_submit_flow_runs,
                            interval=PREFECT_WORKER_QUERY_SECONDS.value(),
                            run_once=run_once,
                            jitter_range=0.3,
                            backoff=4,  # Up to ~1 minute interval during backoff
                        )
                    )
                    # schedule the sync loop
                    loops_task_group.start_soon(
                        partial(
                            critical_service_loop,
                            workload=self.sync_with_backend,
                            interval=self.heartbeat_interval_seconds,
                            run_once=run_once,
                            jitter_range=0.3,
                            backoff=4,
                        )
                    )

                    self._started_event = await self._emit_worker_started_event()

                    # zombie runs cleanup
                    loops_task_group.start_soon(
                        partial(
                            critical_service_loop,
                            workload=self._mark_zombie_flow_runs_as_crashed,
                            interval=PREFECT_WORKER_QUERY_SECONDS.value(),
                            run_once=run_once,
                            jitter_range=0.3,
                            backoff=4,  # Up to ~1 minute interval during backoff
                        )
                    )

                    if with_healthcheck:
                        from prefect.workers.server import build_healthcheck_server

                        # we'll start the ASGI server in a separate thread so that
                        # uvicorn does not block the main thread
                        healthcheck_server = build_healthcheck_server(
                            worker=worker,
                            query_interval_seconds=PREFECT_WORKER_QUERY_SECONDS.value(),
                        )
                        healthcheck_thread = threading.Thread(
                            name="healthcheck-server-thread",
                            target=healthcheck_server.run,
                            daemon=True,
                        )
                        healthcheck_thread.start()
                    printer(f"Worker {worker.name!r} started!")
        finally:
            if healthcheck_server and healthcheck_thread:
                self._logger.debug("Stopping healthcheck server...")
                healthcheck_server.should_exit = True
                healthcheck_thread.join()
                self._logger.debug("Healthcheck server stopped.")

        printer(f"Worker {worker.name!r} stopped!")

    async def setup(self) -> None:
        """Prepares the worker to run."""
        self._logger.debug("Setting up worker...")
        self._runs_task_group = anyio.create_task_group()
        self._limiter = (
            anyio.CapacityLimiter(self._limit) if self._limit is not None else None
        )

        if not PREFECT_TEST_MODE and not PREFECT_API_URL.value():
            raise ValueError("`PREFECT_API_URL` must be set to start a Worker.")

        await self._settings.validate_credentials()
        await self._detect_slurm_api_version()

        self._client = get_client()

        await self._exit_stack.enter_async_context(self._client)
        await self._exit_stack.enter_async_context(self._runs_task_group)

        await self.sync_with_backend()

        self.is_setup = True

    async def run(
        self,
        flow_run: FlowRun,
        configuration: SlurmWorkerConfiguration,
        task_status: Optional[TaskStatus[int]] = None,
    ) -> BaseWorkerResult:
        logger = self.get_flow_run_logger(flow_run)
        logger.info(f"Submitting flow run {flow_run.id} to Slurm")
        job_spec = configuration.get_slurm_job_spec(flow_run)
        logger.debug(f"Slurm job specs: {job_spec}")

        response = await self._submit_slurm_job(job_spec)

        logger.info(f"Submitted flow run {flow_run.id} to Slurm job {response.job_id}")

        if task_status:
            task_status.started(response.job_id)

        return BaseWorkerResult(
            status_code=0,
            identifier=str(response.job_id),
        )

    async def _get_slurm_configuration(self, configuration_class: Optional[Any] = None):
        """Get Slurm configuration with async token reading.

        Always reads fresh token from file for token rotation scenarios.
        Uses async file operations with locking to detect concurrent writes.

        :returns: Configured Slurm API client
        :rtype: Configuration
        """
        config_class_to_use = configuration_class or self._Configuration

        configuration = config_class_to_use(host=str(self._settings.api_url))

        configuration.api_key["user"] = self._settings.user_name
        configuration.api_key["token"] = (
            await self._settings.get_token()
        ).get_secret_value()

        return configuration

    async def _mark_zombie_flow_runs_as_crashed(self):
        flow_runs = await self._get_running_or_pending_flow_runs()

        if not flow_runs:
            self._logger.debug("Discovered 0 zombie flow runs")

            return

        slurm_job_ids = [
            run.infrastructure_pid
            for run in flow_runs
            if run.infrastructure_pid is not None
        ]

        slurm_job_states = await self._get_slurm_job_states(slurm_job_ids)

        zombie_flow_runs = self._filter_zombie_flow_runs(flow_runs, slurm_job_states)

        self._logger.debug(f"Discovered {len(zombie_flow_runs)} zombie flow runs")

        if zombie_flow_runs:
            for flow_run in zombie_flow_runs:
                await self._propose_crashed_state(
                    flow_run=flow_run,
                    message=f"Slurm job {flow_run.infrastructure_pid} was terminated",
                )

    async def _get_running_or_pending_flow_runs(self):
        flow_runs = await self.client.read_flow_runs(
            work_pool_filter=WorkPoolFilter(
                name=WorkPoolFilterName(any_=[self.work_pool.name])
            ),
            work_queue_filter=WorkQueueFilter(
                name=WorkQueueFilterName(
                    any_=list(self._work_queues if self._work_queues else {"default"})
                )
            ),
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(
                        any_=[StateType.RUNNING, StateType.PENDING]
                    )
                )
            ),
        )

        return flow_runs

    async def _get_slurm_job_states(self, ids: List[str]) -> Dict[str, str | None]:
        if not ids:
            return dict()

        states: Dict[str, str | None] = {}
        lock = anyio.Lock()  # In case we get to see GIL being removed from python and if anyio changes it's concurrency model

        async with anyio.create_task_group() as tg:
            for id in ids:
                tg.start_soon(self.fetch_and_store_job_state, id, states, lock)

        return states

    async def fetch_and_store_job_state(
        self, id: str, states: Dict[str, str | None], lock: anyio.Lock
    ):
        slurm_configuration = await self._get_slurm_configuration()

        async with self._ApiClient(slurm_configuration) as client:
            api: slurpy.SlurmApi = self._SlurmApi(client)

            try:
                response = await api.get_jobs_state_without_preload_content(id)
                result = await response.json()

                if not result.get("jobs", []):
                    job_state = None
                elif not result["jobs"][0].get("state", []):
                    job_state = None
                else:
                    job_state = result["jobs"][0]["state"][0]
            except self._ApiException as e:
                self._logger.warning(f"Failed to fetch state for job {id}: {e}")
                job_state = "UNKNOWN"
            except Exception as e:
                self._logger.error(f"Unexpected error fetching state for job {id}: {e}")
                job_state = "UNKNOWN"

            async with lock:
                states[id] = job_state

    @staticmethod
    def _filter_zombie_flow_runs(
        flow_runs: List[FlowRun], slurm_job_states: Dict[str, str | None]
    ):
        zombie_pairs: List[FlowRun] = []

        for flow_run in flow_runs:
            if flow_run.state is None:
                continue

            matching_slurm_job_state = (
                slurm_job_states[flow_run.infrastructure_pid]
                if flow_run.infrastructure_pid in slurm_job_states
                else None
            )

            if matching_slurm_job_state == "UNKNOWN":
                continue

            if (
                flow_run.state.type == StateType.RUNNING
                and matching_slurm_job_state != "RUNNING"
            ):
                zombie_pairs.append(flow_run)

            if (
                flow_run.state.type == StateType.PENDING
                and matching_slurm_job_state not in ["RUNNING", "PENDING"]
            ):
                zombie_pairs.append(flow_run)

        return zombie_pairs

    async def _detect_slurm_api_version(self) -> str:
        """Detect available Slurm API version by testing /ping endpoint.

        Tests API versions from v0.0.42 down to v0.0.40 and returns the first
        working version. Also stores the required classes for the detected version.

        :returns: Detected API version (e.g., "v0042")
        :rtype: str

        :raises ValueError: If no compatible API version is found
        """
        versions_to_test = [
            ("v0042", "v0.0.42"),
            ("v0041", "v0.0.41"),
            ("v0040", "v0.0.40"),
        ]

        for version, pretty_version in versions_to_test:
            try:
                self._logger.debug(f"Testing Slurm API version {pretty_version}")

                module_name = f"slurpy.{version}.asyncio"
                rest_module_name = f"slurpy.{version}.asyncio.rest"

                slurpy_module = importlib.import_module(module_name)
                rest_module = importlib.import_module(rest_module_name)

                # Store the classes we need
                Configuration = slurpy_module.Configuration
                ApiClient = slurpy_module.ApiClient
                SlurmApi = slurpy_module.SlurmApi
                ApiException = rest_module.ApiException
                JobInfo = slurpy_module.JobInfo

                # Test the API by calling /ping endpoint
                configuration = await self._get_slurm_configuration(Configuration)

                # Test the ping endpoint
                async with ApiClient(configuration) as client:
                    api = SlurmApi(client)
                    try:
                        await api.get_ping()
                        self._logger.info(
                            f"Successfully detected Slurm API version {pretty_version}"
                        )

                        # Store the classes for this version
                        self._Configuration = Configuration
                        self._ApiClient = ApiClient
                        self._SlurmApi = SlurmApi
                        self._ApiException = ApiException
                        self._JobInfo = JobInfo

                        return version

                    except Exception as e:
                        self._logger.debug(
                            f"API version {pretty_version} failed ping test: {e}"
                        )
                        continue

            except ImportError as e:
                self._logger.debug(f"API version {pretty_version} not available: {e}")
                continue
            except Exception as e:
                self._logger.debug(f"Error testing API version {pretty_version}: {e}")
                continue

        raise ValueError(
            f"No compatible Slurm API version found. Tested versions: {', '.join((v for _, v in versions_to_test))}. "
            f"Ensure Slurm REST API is running and accessible at {self._settings.api_url}"
        )

    async def _submit_slurm_job(self, job_spec: Dict[str, Any]):
        slurm_configuration = await self._get_slurm_configuration()

        async with self._ApiClient(slurm_configuration) as client:
            api = self._SlurmApi(client)

            try:
                return await api.post_job_submit(job_submit_req=job_spec)
            except self._ApiException as e:
                raise InfrastructureError(e)
