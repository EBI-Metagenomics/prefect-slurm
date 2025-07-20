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

from typing import Any, Optional

from pydantic import BaseModel, Field, ConfigDict, field_serializer


class SlurmJobSubmitDescription(BaseModel):
    """
    Pydantic schema for storing a slurm JobSubmitDescription object as json.
    """

    model_config = ConfigDict(extra="allow")

    name: str = Field(..., description="Name of the job")
    script: str = Field(..., description="Content of the job script/command")
    memory_per_node: int = Field(
        None, description="Memory required for the job in GB"
    )
    time_limit: int = Field(
        None, description="Wall time required for the job in minutes"
    )  # TODO: store as a timedelta-derived type?
    working_directory: Optional[str] = Field(
        None, description="Working directory for the job"
    )
    # TODO environment

    @field_serializer("memory_per_node", return_type=dict)
    def serialize_memory_per_node(self, v: int, info: Any) -> dict:
        return { "set": True, "number": v }