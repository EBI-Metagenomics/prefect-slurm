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

import asyncio
import logging
from typing import Any, Dict

from prefect.client import get_client
from prefect.exceptions import PrefectHTTPStatusError

from prefect_slurm.slurm_api import SlurmApiClient

logger = logging.getLogger(__name__)


class JobMonitor:
    """Monitor for Slurm jobs.
    
    This class handles monitoring Slurm jobs and reporting their status
    back to the Prefect API.
    """
    
    def __init__(
        self,
        slurm_client: SlurmApiClient,
        poll_interval: int = 30,
    ):
        """Initialize the job monitor.
        
        Args:
            slurm_client: Client for interacting with the Slurm REST API
            poll_interval: Interval in seconds between status checks
        """
        self.slurm_client = slurm_client
        self.poll_interval = poll_interval
        self._monitors = {}
    
    async def start_monitoring(self, job_id: str, flow_run_id: str) -> None:
        """Start monitoring a job.
        
        Args:
            job_id: ID of the Slurm job to monitor
            flow_run_id: ID of the Prefect flow run associated with the job
        """
        if job_id in self._monitors:
            logger.warning(f"Already monitoring job {job_id}")
            return
        
        self._monitors[job_id] = asyncio.create_task(
            self._monitor_job(job_id, flow_run_id)
        )
    
    async def stop_monitoring(self, job_id: str) -> None:
        """Stop monitoring a job.
        
        Args:
            job_id: ID of the job to stop monitoring
        """
        if job_id in self._monitors:
            self._monitors[job_id].cancel()
            try:
                await self._monitors[job_id]
            except asyncio.CancelledError:
                pass
            del self._monitors[job_id]
    
    async def stop_all(self) -> None:
        """Stop monitoring all jobs."""
        for job_id in list(self._monitors.keys()):
            await self.stop_monitoring(job_id)
    
    async def _monitor_job(self, job_id: str, flow_run_id: str) -> None:
        """Monitor a job and report its status to Prefect.
        
        Args:
            job_id: ID of the Slurm job to monitor
            flow_run_id: ID of the Prefect flow run associated with the job
        """
        try:
            while True:
                try:
                    status = await self.slurm_client.get_job_status(job_id)
                    await self._update_flow_run_status(flow_run_id, status)
                    
                    # Check if the job is complete
                    job_state = status.get("job_state", "")
                    if job_state in ["COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"]:
                        logger.info(f"Job {job_id} completed with state {job_state}")
                        break
                    
                except Exception as e:
                    logger.error(f"Error monitoring job {job_id}: {e}")
                
                await asyncio.sleep(self.poll_interval)
        finally:
            if job_id in self._monitors:
                del self._monitors[job_id]
    
    async def _update_flow_run_status(self, flow_run_id: str, job_status: Dict[str, Any]) -> None:
        """Update the status of a flow run based on the job status.
        
        Args:
            flow_run_id: ID of the flow run to update
            job_status: Status of the Slurm job
        """
        # In a real implementation, this would map Slurm job states to Prefect flow run states
        # and update the flow run status accordingly
        # This is just a stub implementation
        job_state = job_status.get("job_state", "")
        
        # Example mapping of Slurm states to Prefect states
        # PENDING -> PENDING
        # RUNNING -> RUNNING
        # COMPLETED -> COMPLETED
        # FAILED -> FAILED
        # CANCELLED -> CANCELLED
        # TIMEOUT -> FAILED
        
        try:
            async with get_client() as client:
                # This is a stub - in a real implementation, we would update the flow run status
                # based on the job status
                pass
        except PrefectHTTPStatusError as e:
            logger.error(f"Error updating flow run {flow_run_id}: {e}")