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

import logging
from typing import List

from prefect.client import get_client

from prefect_slurm.monitoring import JobMonitor
from prefect_slurm.slurm_api import SlurmApiClient

logger = logging.getLogger(__name__)


async def recover_running_flow_runs(
    slurm_client: SlurmApiClient,
    job_monitor: JobMonitor,
    work_queue_name: str,
) -> List[str]:
    """Recover monitoring for flow runs that should be running.
    
    This function is called when the worker starts to recover monitoring
    for flow runs that were submitted before the worker went down.
    
    Args:
        slurm_client: Client for interacting with the Slurm REST API
        job_monitor: Monitor for Slurm jobs
        work_queue_name: Name of the work queue to recover flow runs from
        
    Returns:
        List of flow run IDs that were recovered
    """
    recovered_flow_runs = []
    
    try:
        # In a real implementation, this would:
        # 1. Query the Prefect API for flow runs in the RUNNING state in the work queue
        # 2. For each flow run, check if there's a corresponding Slurm job
        # 3. If there is, start monitoring it
        # 4. If there isn't, mark the flow run as failed
        
        # This is just a stub implementation
        async with get_client() as client:
            # Get running flow runs from the work queue
            # This is a placeholder - in a real implementation, we would use the Prefect API
            running_flow_runs = []
            
            for flow_run in running_flow_runs:
                flow_run_id = flow_run["id"]
                
                # Check if there's a corresponding Slurm job
                # In a real implementation, we would store the job ID in the flow run metadata
                # or use a naming convention to find it
                job_id = None
                
                if job_id:
                    # Start monitoring the job
                    await job_monitor.start_monitoring(job_id, flow_run_id)
                    recovered_flow_runs.append(flow_run_id)
                    logger.info(f"Recovered monitoring for flow run {flow_run_id} (job {job_id})")
                else:
                    # Mark the flow run as failed
                    logger.warning(f"Could not find Slurm job for flow run {flow_run_id}")
    
    except Exception as e:
        logger.error(f"Error recovering running flow runs: {e}")
    
    return recovered_flow_runs


async def cleanup_worker_resources(
    slurm_client: SlurmApiClient,
    job_monitor: JobMonitor,
) -> None:
    """Clean up resources when the worker is shutting down.
    
    Args:
        slurm_client: Client for interacting with the Slurm REST API
        job_monitor: Monitor for Slurm jobs
    """
    try:
        # Stop all job monitors
        await job_monitor.stop_all()
        
        # Close the Slurm API client
        await slurm_client.close()
    
    except Exception as e:
        logger.error(f"Error cleaning up worker resources: {e}")