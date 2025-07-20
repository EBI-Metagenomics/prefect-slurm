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

from typing import Optional, Union

from prefect.client.schemas import FlowRun
from prefect.workers.base import BaseJobConfiguration, BaseVariables
from pydantic import Field

from prefect_slurm.slurm_api import SlurmCredentials


class SlurmWorkerConfiguration(BaseJobConfiguration):
    """
    Configuration for a Slurm worker.
    """
    script: Optional[str] = Field(None)
    partition: str = Field(default="compute", description="Slurm partition to use")
    qos: Optional[str] = Field(default=None, description="Slurm QOS to use")
    memory_per_node: Optional[Union[str, int]] = Field(
        default="4G", description="Memory required for the job", json_schema_extra=dict(template="{{ memory_gb }}G")
    )
    time_limit: Optional[str] = Field(
        default="01:00:00", description="Wall time required for the job in HH:MM:SS format", json_schema_extra=dict(template="{{hours}}:00:00")
    )
    cpus_per_task: int = Field(default=1, description="Number of CPUs to allocate per task")
    working_directory: Optional[str] = Field(
        default=None, description="Working directory for the job"
    )
    slurm_credentials: SlurmCredentials = Field(default=None, description="Credentials for the SLURM Rest API")
    # TODO: Defined here so that they can be managed via prefect UI... could also be some env vars exposed to the worker process...
    # consider what is best.


    def prepare_for_flow_run(
        self,
        flow_run: FlowRun,
        deployment: "DeploymentResponse | None" = None,
        flow: "APIFlow | None" = None,
        work_pool: "WorkPool | None" = None,
        worker_name: str | None = None,
    ):
        """
        Prepares the job configuration for a flow run.

        Ensures that the slurm job script is set to the flow run command.

        Args:
            flow_run: The flow run to prepare the job configuration for
            deployment: The deployment associated with the flow run used for
                preparation.
            flow: The flow associated with the flow run used for preparation.
            work_pool: The work pool associated with the flow run used for preparation.
            worker_name: The name of the worker used for preparation.
        """
        super().prepare_for_flow_run(flow_run, deployment, flow, work_pool, worker_name)
        self.script = self._base_flow_run_command()


class SlurmWorkerTemplateVariables(BaseVariables):
    memory_gb: int = Field(
        default=8,
        description="Memory required for the flow"
    )
    hours: int = Field(
        default=1,
        description="Max number of walltime hours for the flow"
    )
