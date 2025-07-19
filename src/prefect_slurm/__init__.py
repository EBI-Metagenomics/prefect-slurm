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

__version__ = "0.1.0"

from prefect_slurm.worker import SlurmWorker
from prefect_slurm.slurm_api import SlurmApiClient
from prefect_slurm.job_spec import create_job_specification, create_flow_run_command
from prefect_slurm.config import SlurmWorkerConfiguration, SlurmWorkerTemplateVariables, SlurmCredentials
from prefect_slurm.monitoring import JobMonitor
from prefect_slurm.lifecycle import recover_running_flow_runs, cleanup_worker_resources

__all__ = [
    "SlurmWorker",
    "SlurmApiClient",
    "SlurmCredentials",
    "SlurmWorkerConfiguration",
    "SlurmWorkerTemplateVariables",
    "create_job_specification",
    "create_flow_run_command",
    "JobMonitor",
    "recover_running_flow_runs",
    "cleanup_worker_resources",
]
