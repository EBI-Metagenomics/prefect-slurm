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

import shlex
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Literal, Optional

from prefect import __version__ as prefect_version
from prefect.client.schemas import FlowRun
from prefect.workers.base import BaseJobConfiguration, BaseVariables
from pydantic import Field

if TYPE_CHECKING:
    from prefect.client.schemas.objects import FlowRun, WorkPool
    from prefect.client.schemas.responses import (
        DeploymentResponse,
    )
    from prefect.flows import Flow as APIFlow


LOG_MASK_PATTERNS = (
    "PREFECT_API_AUTH_STRING",
    "PREFECT_API_KEY",
)


class SlurmWorkerConfiguration(BaseJobConfiguration):
    """
    Configuration for a Slurm worker.
    """

    cpu: int = Field(default=1, description="CPU count required for the flow")
    memory: int = Field(default=4, description="Memory in GB required for the flow")
    partition: Optional[str] = Field(default=None, description="Slurm partition to use")
    shebang: str = Field(
        default="#!/bin/bash",
        pattern=r"^#!/.+$",
        description="Indicates which shell to use when running the slurm job",
    )
    script: str = Field(
        default="",
        description="Script to run in the SLURM job",
        json_schema_extra=dict(template="{{command}}"),
    )
    source_files: list[Path] = Field(
        default_factory=list,
        title="Source Files",
        description="List of environment files to source",
    )
    time_limit: int = Field(
        default=1,
        title="Time Limit",
        description="Max number of wall time in hours for the flow",
    )
    working_dir: Path = Field(
        title="Working Directory",
        description="Directory where to to run the flow from",
    )
    image: Optional[str] = Field(
        default=None,
        title="Container Image",
        description=(
            "Container image to run the flow inside via Singularity/Apptainer "
            "(e.g. 'docker://org/image:tag', 'library://...', or a path to a "
            "local .sif file). When set, the flow run command executes inside "
            "this container instead of a bootstrapped Python environment."
        ),
    )
    container_runtime: Literal["auto", "singularity", "apptainer"] = Field(
        default="auto",
        title="Container Runtime",
        description=(
            "Which container runtime binary to invoke. 'auto' prefers apptainer, "
            "falling back to singularity, resolved at job-run time on the "
            "compute node."
        ),
    )
    bind_paths: list[str] = Field(
        default_factory=list,
        title="Bind Paths",
        description=(
            "Additional host paths to bind-mount into the container (--bind). "
            "The job's working directory is always bound automatically."
        ),
    )
    container_options: list[str] = Field(
        default_factory=list,
        title="Container Options",
        description=(
            "Extra raw arguments appended to 'singularity/apptainer exec', "
            "e.g. ['--containall']."
        ),
    )

    def prepare_for_flow_run(
        self,
        flow_run: FlowRun,
        deployment: Optional["DeploymentResponse"] = None,
        flow: Optional["APIFlow"] = None,
        work_pool: Optional["WorkPool"] = None,
        worker_name: Optional[str] = None,
    ):
        """
        Prepares the job configuration for a flow run.

        Ensures that the slurm job script is set to the flow run command.

        :param flow_run: The flow run to prepare the job configuration for
        :param deployment: The deployment associated with the flow run used for
            preparation.
        :param flow: The flow associated with the flow run used for preparation.
        :param work_pool: The work pool associated with the flow run used for preparation.
        :param worker_name: The name of the worker used for preparation.
        """
        super().prepare_for_flow_run(flow_run, deployment, flow, work_pool, worker_name)

        script_segments = [
            self._script_shebang_segment(),
            self._script_setup_segment(),
            self._script_command_segment(),
            self._script_teardown_segment(),
        ]

        self.script = "\n".join(
            (segment for segment in script_segments if segment is not None)
        )

    def get_slurm_job_spec(self, flow_run: FlowRun) -> Dict[str, Any]:
        return dict(
            job=dict(
                name=flow_run.name,
                script=self.script,
                cpus_per_task=self.cpu,
                memory_per_node={"set": True, "number": self.memory * 1024},
                current_working_directory=str(self.working_dir),
                time_limit={"set": True, "number": self.time_limit * 60},
                partition=self.partition,
                environment=self._env_to_list(),
            )
        )

    def _script_shebang_segment(self):
        return self.shebang.strip()

    def _script_setup_segment(self):
        if self.image:
            return self._script_source_segment()

        if self.source_files:
            return self._script_source_segment()

        return self._script_python_venv_segment()

    def _script_teardown_segment(self):
        if self.image or self.source_files:
            return None

        return 'deactivate\nrm -rf "$VENV_DIR"'

    def _script_command_segment(self) -> str:
        if self.image:
            return self._script_container_command_segment()

        return self.command

    def _script_container_command_segment(self) -> str:
        bind_paths = list(dict.fromkeys([str(self.working_dir), *self.bind_paths]))
        exec_parts = [
            "exec",
            "--bind",
            shlex.quote(",".join(bind_paths)),
            "--pwd",
            shlex.quote(str(self.working_dir)),
            *self.container_options,
            shlex.quote(self.image),
            self.command,
        ]
        exec_line = " ".join(exec_parts)

        if self.container_runtime == "auto":
            runtime_resolution = (
                'CONTAINER_RUNTIME="$(command -v apptainer || command -v singularity)"\n'
                'if [ -z "$CONTAINER_RUNTIME" ]; then\n'
                '  echo "Error: neither apptainer nor singularity found on PATH" >&2\n'
                "  exit 1\n"
                "fi"
            )
        else:
            runtime_resolution = f'CONTAINER_RUNTIME="{self.container_runtime}"'

        return f'{runtime_resolution}\n"$CONTAINER_RUNTIME" {exec_line}'

    def _script_source_segment(self):
        if not self.source_files:
            return None

        return "\n".join((f"source {file}" for file in self.source_files))

    def _script_python_venv_segment(self):
        return (
            f'VENV_DIR="$TMPDIR/.venv_$SLURM_JOB_ID"\n'
            f'python -m venv "$VENV_DIR"\n'
            f'source "$VENV_DIR/bin/activate"\n'
            f'pip install "prefect=={prefect_version}" importlib-metadata'
        )

    def _env_to_list(self):
        return [f"{key}={value}" for key, value in self.env.items()]


class SlurmWorkerTemplateVariables(BaseVariables):
    cpu: int = Field(default=1, description="CPU count required for the flow")
    memory: int = Field(default=4, description="Memory in GB required for the flow")
    partition: Optional[str] = Field(default=None, description="Slurm partition to use")
    shebang: str = Field(
        default="#!/bin/bash",
        pattern=r"^#!/.+$",
        description="Indicates which shell to use when running the slurm job",
    )
    source_files: list[Path] = Field(
        default_factory=list,
        title="Source Files",
        description=(
            "List of environment files to source\n"
            "If none are provided a new python environment will be created and sourced in the working directory"
        ),
        examples=[["~/.bashrc", "~/envs/conda/bin/activate"]],
    )
    time_limit: int = Field(
        default=1,
        title="Time Limit",
        description="Max number of wall time in hours for the flow",
    )
    working_dir: Path = Field(
        title="Working Directory",
        description="Directory where to to run the flow from",
    )
    image: Optional[str] = Field(
        default=None,
        title="Container Image",
        description=(
            "Container image to run the flow inside via Singularity/Apptainer "
            "(e.g. 'docker://org/image:tag', 'library://...', or a path to a "
            "local .sif file). When set, the flow run command executes inside "
            "this container instead of a bootstrapped Python environment."
        ),
    )
    container_runtime: Literal["auto", "singularity", "apptainer"] = Field(
        default="auto",
        title="Container Runtime",
        description=(
            "Which container runtime binary to invoke. 'auto' prefers apptainer, "
            "falling back to singularity, resolved at job-run time on the "
            "compute node."
        ),
    )
    bind_paths: list[str] = Field(
        default_factory=list,
        title="Bind Paths",
        description=(
            "Additional host paths to bind-mount into the container (--bind). "
            "The job's working directory is always bound automatically."
        ),
    )
    container_options: list[str] = Field(
        default_factory=list,
        title="Container Options",
        description=(
            "Extra raw arguments appended to 'singularity/apptainer exec', "
            "e.g. ['--containall']."
        ),
    )
