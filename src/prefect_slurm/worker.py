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

from typing import Any, Dict, List, Optional

import anyio
from prefect.client.schemas import FlowRun
from prefect.workers.base import BaseWorker

from prefect_slurm import SlurmWorkerConfiguration
from prefect_slurm.config import SlurmWorkerTemplateVariables
from prefect_slurm.result import SlurmWorkerResult


class SlurmWorker(BaseWorker):
    """A Prefect worker that submits flow runs as Slurm jobs.
    
    This worker runs on a Slurm submitter node and submits each flow run as a new
    Slurm job via the Slurm REST API.
    """
    
    type: str = "slurm"
    job_configuration = SlurmWorkerConfiguration
    job_configuration_variables = SlurmWorkerTemplateVariables
    _documentation_url = "https://www.github.com/EBI-metagenomics/prefect-slurm"
    _logo_url = "https://www.github.com/EBI-metagenomics/prefect-slurm/resources/prefect-slurm-logo.png"

    async def run(
        self,
        flow_run: FlowRun,
        configuration: SlurmWorkerConfiguration,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> SlurmWorkerResult:
        # Create the execution environment and start execution
        job = await self._create_and_start_job(configuration)

        if task_status:
            # Use a unique ID to mark the run as started. This ID is later used to tear down infrastructure
            # if the flow run is cancelled.
            task_status.started(job.id)

            # Monitor the execution
        job_status = await self._watch_job(job, configuration)

        exit_code = job_status.exit_code if job_status else -1  # Get result of execution for reporting
        return SlurmWorkerResult(
            status_code=exit_code,
            identifier=job.id,
        )