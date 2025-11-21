"""
Unit tests for SlurmWorkerConfiguration and SlurmWorkerTemplateVariables.
"""

from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import pytest
from prefect import __version__ as prefect_version
from prefect.client.schemas import FlowRun

from prefect_slurm.config import SlurmWorkerConfiguration, SlurmWorkerTemplateVariables


@pytest.mark.unit
class TestSlurmWorkerConfiguration:
    """Test cases for SlurmWorkerConfiguration class."""

    def test_get_slurm_job_spec_basic(self, sample_slurm_configuration):
        """Test basic job spec generation."""
        sample_slurm_configuration.script = "#!/bin/bash\necho 'test'"

        job_spec = sample_slurm_configuration.get_slurm_job_spec()

        expected = {
            "job": {
                "script": "#!/bin/bash\necho 'test'",
                "cpus_per_task": 2,
                "memory_per_node": {"set": True, "number": 4096},  # 4GB * 1024
                "current_working_directory": "/tmp/test",
                "time_limit": {"set": True, "number": 60},  # 1 hour * 60
                "partition": "compute",
                "environment": [],
            }
        }

        assert job_spec == expected

    def test_get_slurm_job_spec_minimal(self, minimal_slurm_configuration):
        """Test job spec generation with minimal configuration."""
        minimal_slurm_configuration.script = "echo 'minimal'"

        job_spec = minimal_slurm_configuration.get_slurm_job_spec()

        assert job_spec["job"]["script"] == "echo 'minimal'"
        assert job_spec["job"]["cpus_per_task"] == 1  # default
        assert job_spec["job"]["memory_per_node"] == {
            "set": True,
            "number": 4096,
        }  # default 4GB
        assert job_spec["job"]["time_limit"] == {
            "set": True,
            "number": 60,
        }  # default 1 hour

    def test_script_shebang_segment_default(self, sample_slurm_configuration):
        """Test shebang generation with default value."""
        shebang = sample_slurm_configuration._script_shebang_segment()
        assert shebang == "#!/bin/bash"

    def test_script_shebang_segment_custom(self, sample_slurm_configuration):
        """Test shebang generation with custom values."""
        test_cases = [
            "#!/usr/bin/python3",
            "#!/bin/zsh",
            "#!/usr/bin/env python",
            "#!/bin/sh",
        ]

        for custom_shebang in test_cases:
            sample_slurm_configuration.shebang = custom_shebang
            result = sample_slurm_configuration._script_shebang_segment()
            assert result == custom_shebang

    def test_script_shebang_segment_with_whitespace(self, sample_slurm_configuration):
        """Test shebang generation strips whitespace."""
        sample_slurm_configuration.shebang = "  #!/bin/bash  \n"
        shebang = sample_slurm_configuration._script_shebang_segment()
        assert shebang == "#!/bin/bash"

    def test_script_source_segment_with_files(self, sample_slurm_configuration):
        """Test source segment generation with source files."""
        sample_slurm_configuration.source_files = [
            Path("/etc/profile"),
            Path("/home/user/.bashrc"),
        ]

        source_segment = sample_slurm_configuration._script_source_segment()

        expected = "source /etc/profile\nsource /home/user/.bashrc"
        assert source_segment == expected

    def test_script_source_segment_no_files(self, sample_slurm_configuration):
        """Test source segment generation with no source files."""
        sample_slurm_configuration.source_files = []

        source_segment = sample_slurm_configuration._script_source_segment()

        assert source_segment is None

    def test_script_python_venv_segment(self, sample_slurm_configuration):
        """Test Python venv segment generation."""
        venv_segment = sample_slurm_configuration._script_python_venv_segment()

        expected = (
            'VENV_DIR="$TMPDIR/.venv_$SLURM_JOB_ID"\n'
            'python -m venv "$VENV_DIR"\n'
            'source "$VENV_DIR/bin/activate"\n'
            f'pip install "prefect=={prefect_version}"'
        )

        assert expected == venv_segment

    def test_script_setup_segment_no_files(self, sample_slurm_configuration):
        """Test source segment generation with no source files"""
        sample_slurm_configuration.source_files = []

        with (
            patch.object(
                sample_slurm_configuration, "_script_source_segment"
            ) as mock_source_segment,
            patch.object(
                sample_slurm_configuration, "_script_python_venv_segment"
            ) as mock_python_venv_segment,
        ):
            sample_slurm_configuration._script_setup_segment()

        mock_source_segment.assert_not_called()
        mock_python_venv_segment.assert_called()

    def test_script_setup_segment_with_files(self, sample_slurm_configuration):
        """Test source segment generation with source files"""
        sample_slurm_configuration.source_files = [
            Path("/etc/profile"),
            Path("/home/user/.bashrc"),
        ]

        with (
            patch.object(
                sample_slurm_configuration, "_script_source_segment"
            ) as mock_source_segment,
            patch.object(
                sample_slurm_configuration, "_script_python_venv_segment"
            ) as mock_python_venv_segment,
        ):
            sample_slurm_configuration._script_setup_segment()

        mock_python_venv_segment.assert_not_called()
        mock_source_segment.assert_called()

    def test_env_to_list_conversion(self, sample_slurm_configuration):
        """Test environment dictionary to list conversion."""
        sample_slurm_configuration.env = {
            "PATH": "/usr/bin:/bin",
            "HOME": "/home/user",
            "USER": "testuser",
        }

        env_list = sample_slurm_configuration._env_to_list()

        expected = ["PATH=/usr/bin:/bin", "HOME=/home/user", "USER=testuser"]
        # Sort both lists since dict order might vary
        assert sorted(env_list) == sorted(expected)

    def test_prepare_for_flow_run_script_generation(self, sample_slurm_configuration):
        """Test script generation during flow run preparation."""
        flow_run = FlowRun(id=uuid4(), name="test-flow-run", flow_id=uuid4())
        sample_slurm_configuration.command = "python -m prefect.engine"

        sample_slurm_configuration.prepare_for_flow_run(flow_run)

        # Check that script was generated
        assert sample_slurm_configuration.script is not None
        assert "#!/bin/bash" in sample_slurm_configuration.script
        assert "python -m prefect.engine" in sample_slurm_configuration.script

    def test_prepare_for_flow_run_with_custom_shebang(self, sample_slurm_configuration):
        """Test script generation with custom shebang."""
        flow_run = FlowRun(id=uuid4(), name="test-flow-run", flow_id=uuid4())
        sample_slurm_configuration.command = "python -m prefect.engine"
        sample_slurm_configuration.shebang = "#!/usr/bin/python3"

        sample_slurm_configuration.prepare_for_flow_run(flow_run)

        # Check that custom shebang appears in script
        script = sample_slurm_configuration.script
        assert script is not None
        assert script.startswith("#!/usr/bin/python3")
        assert "#!/bin/bash" not in script  # Default should not appear
        assert "python -m prefect.engine" in script

    def test_prepare_for_flow_run_with_source_files(self, sample_slurm_configuration):
        """Test script generation with source files."""
        flow_run = FlowRun(
            id=uuid4(), name="test-flow-run-with-source", flow_id=uuid4()
        )
        sample_slurm_configuration.command = "python test.py"
        sample_slurm_configuration.source_files = [Path("/etc/profile")]

        sample_slurm_configuration.prepare_for_flow_run(flow_run)

        script = sample_slurm_configuration.script
        assert "#!/bin/bash" in script
        assert "source /etc/profile" in script
        assert "python test.py" in script

    def test_prepare_for_flow_run_with_source_files_and_custom_shebang(
        self, sample_slurm_configuration
    ):
        """Test script generation with source files and custom shebang."""
        flow_run = FlowRun(
            id=uuid4(), name="test-flow-run-custom-shebang", flow_id=uuid4()
        )
        sample_slurm_configuration.command = "python test.py"
        sample_slurm_configuration.source_files = [Path("/etc/profile")]
        sample_slurm_configuration.shebang = "#!/bin/zsh"

        sample_slurm_configuration.prepare_for_flow_run(flow_run)

        script = sample_slurm_configuration.script
        assert script.startswith("#!/bin/zsh")
        assert "#!/bin/bash" not in script  # Default should not appear
        assert "source /etc/profile" in script
        assert "python test.py" in script

    @pytest.mark.parametrize(
        "cpu,memory,time_limit",
        [
            (1, 2, 1),
            (4, 16, 24),
            (8, 32, 48),
        ],
    )
    def test_job_spec_with_different_resources(self, cpu, memory, time_limit):
        """Test job spec generation with different resource configurations."""
        config = SlurmWorkerConfiguration(
            cpu=cpu,
            memory=memory,
            time_limit=time_limit,
            working_dir=Path("/tmp"),
        )
        config.script = "echo 'test'"

        job_spec = config.get_slurm_job_spec()

        assert job_spec["job"]["cpus_per_task"] == cpu
        assert job_spec["job"]["memory_per_node"]["number"] == memory * 1024
        assert job_spec["job"]["time_limit"]["number"] == int(time_limit * 60)

    def test_env_variables_in_prepare_for_flow_run(self, sample_slurm_configuration):
        """Test that user environment variables are preserved during flow run preparation."""
        flow_run = FlowRun(id=uuid4(), name="test-env-preservation", flow_id=uuid4())
        sample_slurm_configuration.command = "echo 'test'"
        sample_slurm_configuration.env = {
            "CUSTOM_VAR": "custom_value",
            "USER": "testuser",
        }

        sample_slurm_configuration.prepare_for_flow_run(flow_run)

        # Check that custom environment variables are preserved
        assert "CUSTOM_VAR" in sample_slurm_configuration.env
        assert "USER" in sample_slurm_configuration.env
        assert sample_slurm_configuration.env["CUSTOM_VAR"] == "custom_value"
        assert sample_slurm_configuration.env["USER"] == "testuser"

    @pytest.mark.parametrize(
        "valid_shebang",
        [
            "#!/bin/bash",
            "#!/usr/bin/python3",
            "#!/bin/zsh",
            "#!/usr/bin/env python",
            "#!/bin/sh",
            "#!/usr/bin/perl",
            "#!/usr/local/bin/fish",
        ],
    )
    def test_shebang_validation_valid_patterns(self, valid_shebang):
        """Test that valid shebang patterns are accepted."""
        config = SlurmWorkerConfiguration(
            shebang=valid_shebang, working_dir=Path("/tmp/test")
        )
        assert config.shebang == valid_shebang

    @pytest.mark.parametrize(
        "invalid_shebang",
        [
            "#!",  # Empty interpreter
            "!/bin/bash",  # Missing #
            "#!/",  # Missing interpreter
            "# !/bin/bash",  # Space in shebang
            "bin/bash",  # No shebang at all
            "",  # Empty string
        ],
    )
    def test_shebang_validation_invalid_patterns(self, invalid_shebang):
        """Test that invalid shebang patterns are rejected."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            SlurmWorkerConfiguration(
                shebang=invalid_shebang, working_dir=Path("/tmp/test")
            )


@pytest.mark.unit
class TestSlurmWorkerTemplateVariables:
    """Test cases for SlurmWorkerTemplateVariables class."""

    def test_default_values(self):
        """Test default values for template variables."""
        variables = SlurmWorkerTemplateVariables(working_dir=Path("/tmp/test"))

        assert variables.cpu == 1
        assert variables.memory == 4
        assert variables.partition is None
        assert variables.shebang == "#!/bin/bash"
        assert variables.time_limit == 1
        assert variables.source_files == []

    def test_custom_values(self, sample_template_variables):
        """Test custom values for template variables."""
        assert sample_template_variables.cpu == 4
        assert sample_template_variables.memory == 8
        assert sample_template_variables.partition == "gpu"
        assert sample_template_variables.shebang == "#!/bin/bash"  # Default value
        assert sample_template_variables.time_limit == 2
        assert sample_template_variables.working_dir == Path("/opt/data")

    @pytest.mark.parametrize(
        "valid_shebang",
        [
            "#!/bin/bash",
            "#!/usr/bin/python3",
            "#!/bin/zsh",
            "#!/usr/bin/env python",
            "#!/bin/sh",
        ],
    )
    def test_template_variables_shebang_validation_valid(self, valid_shebang):
        """Test that valid shebang patterns are accepted in template variables."""
        variables = SlurmWorkerTemplateVariables(
            shebang=valid_shebang, working_dir=Path("/tmp/test")
        )
        assert variables.shebang == valid_shebang

    @pytest.mark.parametrize(
        "invalid_shebang",
        [
            "#!",  # Empty interpreter
            "!/bin/bash",  # Missing #
            "#!/",  # Missing interpreter
            "",  # Empty string
        ],
    )
    def test_template_variables_shebang_validation_invalid(self, invalid_shebang):
        """Test that invalid shebang patterns are rejected in template variables."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            SlurmWorkerTemplateVariables(
                shebang=invalid_shebang, working_dir=Path("/tmp/test")
            )

    @pytest.mark.parametrize("cpu", [1, 2, 4, 8, 16])
    def test_cpu_validation(self, cpu):
        """Test CPU parameter validation."""
        variables = SlurmWorkerTemplateVariables(cpu=cpu, working_dir=Path("/tmp/test"))
        assert variables.cpu == cpu

    @pytest.mark.parametrize("memory", [1, 2, 4, 8, 16, 32, 64])
    def test_memory_validation(self, memory):
        """Test memory parameter validation."""
        variables = SlurmWorkerTemplateVariables(
            memory=memory, working_dir=Path("/tmp/test")
        )
        assert variables.memory == memory

    def test_source_files_list(self):
        """Test source files as list of paths."""
        files = [Path("/etc/profile"), Path("/home/user/.bashrc")]
        variables = SlurmWorkerTemplateVariables(
            source_files=files, working_dir=Path("/tmp/test")
        )

        assert variables.source_files == files
        assert all(isinstance(f, Path) for f in variables.source_files)

    def test_script_teardown_segment_with_source_files(self):
        """Test teardown segment returns None when source files exist."""
        config = SlurmWorkerConfiguration(
            working_dir=Path("/tmp/test"), source_files=[Path("/etc/profile")]
        )

        teardown = config._script_teardown_segment()
        assert teardown is None

    def test_script_teardown_segment_without_source_files(self):
        """Test teardown segment returns cleanup commands when no source files."""
        config = SlurmWorkerConfiguration(working_dir=Path("/tmp/test"))

        teardown = config._script_teardown_segment()
        expected = 'deactivate\nrm -rf "$VENV_DIR"'
        assert teardown == expected

    def test_working_directory_path(self):
        """Test working directory as Path object."""
        working_dir = Path("/opt/my_project")
        variables = SlurmWorkerTemplateVariables(working_dir=working_dir)

        assert variables.working_dir == working_dir
        assert isinstance(variables.working_dir, Path)
