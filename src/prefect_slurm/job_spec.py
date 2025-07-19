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

from typing import Any, Dict, Optional, Union

from pydantic import BaseModel, Field, ConfigDict


class SlurmJobSubmitDescription(BaseModel):
    """
    Pydantic schema for storing a slurm JobSubmitDescription object as json.
    """

    model_config = ConfigDict(extra="allow")

    name: str = Field(..., description="Name of the job")
    script: str = Field(..., description="Content of the job script/command")
    memory_per_node: Optional[Union[str, int]] = Field(
        None, description="Memory required for the job"
    )  # TODO: normalise to a less ambiguous form
    time_limit: Optional[str] = Field(
        None, description="Wall time required for the job in HH:MM:SS format"
    )  # TODO: store as a timedelta-derived type?
    working_directory: Optional[str] = Field(
        None, description="Working directory for the job"
    )


def create_job_specification(
    flow_run_id: str,
    command: str,
    partition: str = "compute",
    qos: Optional[str] = None,
    memory: str = "4G",
    cpus: int = 1,
    time_limit: str = "01:00:00",
    job_name: Optional[str] = None,
    environment: Optional[Dict[str, str]] = None,
    working_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a Slurm job specification for a Prefect flow run.

    Args:
        flow_run_id: ID of the Prefect flow run
        command: Command to execute in the job
        partition: Slurm partition to use
        qos: Slurm QOS to use
        memory: Memory allocation for the job
        cpus: Number of CPUs to allocate
        time_limit: Time limit for the job
        job_name: Name for the Slurm job
        environment: Environment variables to set for the job
        working_dir: Working directory for the job

    Returns:
        Slurm job specification as a dictionary
    """
    if job_name is None:
        job_name = f"prefect-{flow_run_id}"

    # Create a SlurmJobSubmitDescription instance
    job_description = SlurmJobSubmitDescription(
        name=job_name,
        script=command,
        memory_per_node=memory,
        time_limit=time_limit,
        working_directory=working_dir,
    )

    # Create the full job specification with additional parameters
    job_spec = {
        "job": {
            "name": job_description.name,
            "partition": partition,
            "cpus_per_task": cpus,
            "time_limit": job_description.time_limit,
            "environment": {
                "PREFECT_FLOW_RUN_ID": flow_run_id,
            },
            "current_working_directory": job_description.working_directory,
            "standard_output": f"prefect-{flow_run_id}.out",
            "standard_error": f"prefect-{flow_run_id}.err",
            "command": job_description.script,
        }
    }

    # Add optional parameters
    if qos:
        job_spec["job"]["qos"] = qos

    if job_description.memory_per_node:
        job_spec["job"]["memory_per_node"] = job_description.memory_per_node

    if environment:
        job_spec["job"]["environment"].update(environment)

    return job_spec


def create_flow_run_command(
    flow_run_id: str,
    api_url: Optional[str] = None,
    api_key: Optional[str] = None,
) -> str:
    """Create the command to execute a Prefect flow run.

    Args:
        flow_run_id: ID of the flow run to execute
        api_url: URL of the Prefect API
        api_key: API key for the Prefect API

    Returns:
        Command to execute the flow run
    """
    command = f"prefect flow-run execute {flow_run_id}"

    if api_url:
        command += f" --api-url {api_url}"

    # Note: In a real implementation, we would need to handle the API key securely
    # This is just a stub implementation

    return command
