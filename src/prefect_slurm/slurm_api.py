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

from typing import Any, Dict, Union

import httpx
from prefect.blocks.abstract import CredentialsBlock
from pydantic import BaseModel, Field, ConfigDict, SecretStr

from prefect_slurm.job_spec import SlurmJobSubmitDescription


class SlurmJobSubmitRequest(BaseModel):
    """
    Pydantic schema for a Slurm job submission request.
    """

    job: SlurmJobSubmitDescription

    model_config = ConfigDict(extra="allow")


class SlurmJobResponse(BaseModel):
    """
    Pydantic schema for a Slurm job response.
    """

    job_id: str = Field(..., description="ID of the submitted job")

    model_config = ConfigDict(extra="allow")


class SlurmJobStatusResponse(BaseModel):
    """
    Pydantic schema for a Slurm job status response.
    """

    job_id: str = Field(..., description="ID of the job")
    job_state: str = Field(..., description="State of the job")

    model_config = ConfigDict(extra="allow")


class SlurmRestAuth(httpx.Auth):
    def __init__(self, user, token):
        self.token = token
        self.user = user

    def auth_flow(self, request):
        request.headers['X-SLURM-USER-TOKEN'] = self.token
        request.headers['X-SLURM-USER-NAME'] = self.user
        yield request


class SlurmApiClient:
    """Client for interacting with the Slurm REST API.

    This client handles authentication, job submission, and status checking
    for Slurm jobs via the REST API.
    """

    def __init__(
        self,
        api_url: str,
        api_base_path: str,
        api_user: str,
        api_token: str = None,
        timeout: int = 30,
    ):
        """
        Args:
            api_url: URL of the Slurm REST API server
            api_base_path: Base path for the Slurm REST API, e.g. the version number of the API
            api_user: Slurm user for the Slurm REST API
            api_token: Authentication token (JWT) for the Slurm REST API
            timeout: Timeout for API requests in seconds
        """
        self.api_url = api_url.rstrip("/") + "/" + api_base_path.strip("/")
        self.api_user = api_user
        self.api_token = api_token
        self.timeout = timeout
        self._client = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create an HTTP client for API requests."""
        if self._client is None:
            self._client = httpx.AsyncClient(
                auth=SlurmRestAuth(self.api_user, self.api_token),
                base_url=self.api_url,
                timeout=self.timeout,
            )
        return self._client

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def submit_job(self, job_spec: Union[Dict[str, Any], SlurmJobSubmitRequest]) -> str:
        """Submit a job to the Slurm cluster.

        Args:
            job_spec: Slurm job specification as a dictionary or SlurmJobSubmitRequest

        Returns:
            Job ID of the submitted job
        """
        client = await self._get_client()

        # Convert to dict if it's a Pydantic model
        if isinstance(job_spec, SlurmJobSubmitRequest):
            job_spec_dict = job_spec.model_dump(exclude_none=True)
        else:
            job_spec_dict = job_spec

        response = await client.post("/jobs", json=job_spec_dict)
        response.raise_for_status()

        # Parse the response using the Pydantic model
        data = response.json()
        job_response = SlurmJobResponse(**data)

        return job_response.job_id

    async def get_job_status(self, job_id: str) -> SlurmJobStatusResponse:
        """Get the status of a job.

        Args:
            job_id: ID of the job to check

        Returns:
            Job status information as a SlurmJobStatusResponse
        """
        client = await self._get_client()
        response = await client.get(f"/job/{job_id}")
        response.raise_for_status()

        # Parse the response using the Pydantic model
        data = response.json()
        return SlurmJobStatusResponse(**data)

    async def cancel_job(self, job_id: str) -> None:
        """Cancel a job.

        Args:
            job_id: ID of the job to cancel
        """
        client = await self._get_client()
        response = await client.delete(f"/job/{job_id}")
        response.raise_for_status()


class SlurmCredentials(CredentialsBlock):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    _block_type_name = "Slurm REST API Credentials"
    _documentation_url = "https://www.github.com/EBI-metagenomics/prefect-slurm"
    _logo_url = "https://www.github.com/EBI-metagenomics/prefect-slurm/resources/prefect-slurm-logo.png"
    slurm_server: str = Field(..., description="URL of the Slurm REST API", title="Slurm Server")
    slurm_base_path: str = Field("/slurm/v0.0.40/", description="Base path for the Slurm REST API", title="Slurm Base Path")
    slurm_user: str = Field(..., description="Slurm user for the Slurm REST API", title="Slurm User")
    slurm_token: SecretStr = Field(..., description="Slurm JWT token for the Slurm REST API", title="Slurm Token")

    def get_client(self, *args: Any, **kwargs: Any) -> SlurmApiClient:
        return SlurmApiClient(
            api_url=self.slurm_server,
            api_base_path=self.slurm_base_path,
            api_user=self.slurm_user,
            api_token=self.slurm_token.get_secret_value(),
            *args,
            **kwargs,
        )
