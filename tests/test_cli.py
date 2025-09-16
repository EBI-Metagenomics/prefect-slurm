"""
Unit tests for CLI utilities.
"""

import os
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from prefect_slurm.cli import cli, extract_jwt_token
from prefect_slurm.settings import CLISettings


@pytest.mark.cli
@pytest.mark.unit
class TestExtractJwtToken:
    """Test cases for extract_jwt_token function."""

    def test_extract_jwt_token_slurm_jwt_format(self):
        """Test extraction from SLURM_JWT=token format."""
        text = "SLURM_JWT=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"

        result = extract_jwt_token(text)

        assert (
            result
            == "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"
        )

    def test_extract_jwt_token_plain_format(self):
        """Test extraction of plain JWT token."""
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"

        result = extract_jwt_token(token)

        assert result == token

    def test_extract_jwt_token_with_whitespace(self):
        """Test extraction with surrounding whitespace."""
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"
        text = f"\n\t  {token}  \n\t"

        result = extract_jwt_token(text)

        assert result == token

    def test_extract_jwt_token_multiline_with_extra_text(self):
        """Test extraction from multiline text with extra content."""
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"
        text = f"""
        Some output from scontrol
        Token created successfully
        {token}
        Additional info
        """

        result = extract_jwt_token(text)

        assert result == token

    def test_extract_jwt_token_no_token_found(self):
        """Test when no valid JWT token is found."""
        text = "No token here, just some random text"

        result = extract_jwt_token(text)

        assert result is None

    def test_extract_jwt_token_empty_input(self):
        """Test with empty input."""
        result = extract_jwt_token("")
        assert result is None

        result = extract_jwt_token("   \n\t  ")
        assert result is None

    def test_extract_jwt_token_invalid_jwt_parts(self):
        """Test with tokens that have wrong number of parts."""
        # Too few parts
        result = extract_jwt_token("only-one-part")
        assert result is None

        # Only one dot
        result = extract_jwt_token("only.one-part")
        assert result is None

        # Too many parts (4 parts)
        result = extract_jwt_token("one.two.three.four")
        assert result is None

    def test_extract_jwt_token_empty_jwt_parts(self):
        """Test with tokens that have empty parts."""
        result = extract_jwt_token("part1..part3")
        assert result is None

        result = extract_jwt_token(".part2.part3")
        assert result is None


@pytest.mark.cli
@pytest.mark.unit
class TestTokenCommand:
    """Test cases for the token CLI command."""

    def test_token_command_with_positional_argument(self):
        """Test token command with positional file argument."""
        runner = CliRunner()
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"

        with patch.object(CLISettings, "write_token_file") as mock_write:
            result = runner.invoke(cli, ["token", "/custom/path.jwt"], input=token)

        assert result.exit_code == 0
        assert "✓ Token successfully written" in result.output
        assert "✓ File permissions set to 600" in result.output
        mock_write.assert_called_once_with(token)

    def test_token_command_with_env_var(self):
        """Test token command using PREFECT_SLURM_TOKEN_FILE environment variable."""
        runner = CliRunner()
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"

        with patch.object(CLISettings, "write_token_file") as mock_write:
            result = runner.invoke(
                cli,
                ["token"],
                input=token,
                env={"PREFECT_SLURM_TOKEN_FILE": "/env/path.jwt"},
            )

        assert result.exit_code == 0
        mock_write.assert_called_once_with(token)

    def test_token_command_default_path(self):
        """Test token command with default path."""
        runner = CliRunner()
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"

        with patch.object(CLISettings, "write_token_file") as mock_write:
            with patch.dict(os.environ, {}, clear=True):  # Clear env vars
                result = runner.invoke(cli, ["token"], input=token)

        assert result.exit_code == 0
        mock_write.assert_called_once_with(token)

    def test_token_command_no_stdin_input(self):
        """Test token command fails when no stdin input provided."""
        runner = CliRunner()

        result = runner.invoke(cli, ["token"], input="")

        assert result.exit_code == 1
        assert "Error: No input provided via stdin" in result.output
        assert "Hint: Try piping token input" in result.output

    def test_token_command_invalid_jwt_in_input(self):
        """Test token command fails when no valid JWT found in input."""
        runner = CliRunner()

        result = runner.invoke(cli, ["token"], input="invalid token format")

        assert result.exit_code == 1
        assert "Error: No valid JWT token found in input" in result.output
        assert "Expected: JWT format with 3 parts" in result.output

    def test_token_command_slurm_jwt_format_input(self):
        """Test token command with SLURM_JWT= format input."""
        runner = CliRunner()
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"
        input_text = f"SLURM_JWT={token}"

        with patch.object(CLISettings, "write_token_file") as mock_write:
            result = runner.invoke(cli, ["token"], input=input_text)

        assert result.exit_code == 0
        mock_write.assert_called_once_with(token)

    def test_token_command_multiline_scontrol_output(self):
        """Test token command with realistic scontrol output."""
        runner = CliRunner()
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"
        scontrol_output = f"""
        scontrol: Requesting new token for user
        Token request successful
        Token: {token}
        Token will expire in 100 seconds
        """

        with patch.object(CLISettings, "write_token_file") as mock_write:
            result = runner.invoke(cli, ["token"], input=scontrol_output)

        assert result.exit_code == 0
        mock_write.assert_called_once_with(token)

    def test_token_command_write_error(self):
        """Test token command handles write errors gracefully."""
        runner = CliRunner()
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"

        with patch.object(
            CLISettings, "write_token_file", side_effect=OSError("Permission denied")
        ):
            result = runner.invoke(cli, ["token"], input=token)

        assert result.exit_code == 1
        assert "Error: Permission denied" in result.output

    def test_token_command_value_error(self):
        """Test token command handles validation errors gracefully."""
        runner = CliRunner()
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"

        with patch.object(
            CLISettings, "write_token_file", side_effect=ValueError("Invalid token")
        ):
            result = runner.invoke(cli, ["token"], input=token)

        assert result.exit_code == 1
        assert "Error: Invalid token" in result.output

    def test_token_command_keyboard_interrupt(self):
        """Test token command handles keyboard interrupt gracefully."""
        runner = CliRunner()
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"

        with patch.object(
            CLISettings, "write_token_file", side_effect=KeyboardInterrupt()
        ):
            result = runner.invoke(cli, ["token"], input=token)

        assert result.exit_code == 1
        assert "Operation cancelled" in result.output

    def test_token_command_help(self):
        """Test token command help output."""
        runner = CliRunner()

        result = runner.invoke(cli, ["token", "--help"])

        assert result.exit_code == 0
        assert "Read JWT token from stdin" in result.output
        assert "scontrol token username=user" in result.output

    def test_token_command_path_expansion(self):
        """Test that ~ is properly expanded in file paths."""
        runner = CliRunner()
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"

        with patch.object(CLISettings, "write_token_file") as mock_write:
            result = runner.invoke(cli, ["token", "~/test_token.jwt"], input=token)

        assert result.exit_code == 0
        mock_write.assert_called_once_with(token)
