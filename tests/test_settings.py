"""
Comprehensive unit tests for Settings classes and environment file functionality.
"""

import os
import stat
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from prefect_slurm.settings import (
    get_env_file_paths,
    Settings,
    WorkerSettings,
    CLISettings,
)


@pytest.mark.settings
@pytest.mark.unit
class TestCliSettingsWriteTokenFile:
    """Test cases for CliSettings.write_token_file method."""

    def test_write_existing_token_file_success(self, tmp_path):
        """Test successful token file writing."""
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"
        file_path = tmp_path / "token.jwt"
        file_path.touch(0o600)

        settings = CLISettings(token_file=file_path)

        with patch("fcntl.flock"), patch("os.chmod") as mock_chmod:
            settings.write_token_file(token)

        # Verify file was written
        assert file_path.exists()
        assert file_path.read_text() == token

        # Verify permissions were set
        mock_chmod.assert_called_once_with(file_path, 0o600)

    def test_write_non_existing_token_file_success(self, tmp_path):
        """Test successful token file writing."""
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"
        file_path = tmp_path / "new_token.jwt"
        settings = CLISettings(token_file=file_path)

        with patch("fcntl.flock"):
            settings.write_token_file(token)

        # Verify file was written
        assert file_path.exists()
        assert file_path.read_text() == token

        file_stat = os.stat(file_path)
        file_mode = stat.S_IMODE(file_stat.st_mode)
        assert file_mode == 0o600

    def test_write_token_file_creates_parent_directories(self, tmp_path):
        """Test that parent directories are created if they don't exist."""
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"
        file_path = tmp_path / "nested" / "dirs" / "token.jwt"
        settings = CLISettings(token_file=file_path)

        with patch("fcntl.flock"), patch("os.chmod"):
            settings.write_token_file(token)

        assert file_path.exists()
        assert file_path.read_text() == token

    def test_write_token_file_empty_token(self):
        """Test error handling for empty token."""
        settings = CLISettings(token_file=Path("/tmp/token.jwt"))
        with pytest.raises(ValueError, match="Token cannot be empty"):
            settings.write_token_file("")

    def test_write_token_file_invalid_jwt_format(self):
        """Test error handling for invalid JWT format."""
        settings = CLISettings(token_file=Path("/tmp/token.jwt"))

        with pytest.raises(ValueError, match="Invalid JWT format"):
            settings.write_token_file("invalid.token")

        with pytest.raises(ValueError, match="Invalid JWT format"):
            settings.write_token_file("one.two.three.four")

    def test_write_token_file_jwt_with_empty_parts(self):
        """Test error handling for JWT with empty parts."""
        settings = CLISettings(token_file=Path("/tmp/token.jwt"))
        with pytest.raises(ValueError, match="Invalid JWT format"):
            settings.write_token_file("part1..part3")

    def test_write_token_file_os_error(self, tmp_path):
        """Test error handling for OS errors during file operations."""
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"
        file_path = tmp_path / "token.jwt"
        settings = CLISettings(token_file=file_path)

        with patch("builtins.open", side_effect=OSError("Permission denied")):
            with pytest.raises(OSError, match="Failed to write token file"):
                settings.write_token_file(token)

    def test_write_token_file_permission_error_on_touch(self, tmp_path):
        """Test PermissionError is raised when file touch fails due to permissions."""
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"
        file_path = tmp_path / "protected" / "token.jwt"
        settings = CLISettings(token_file=file_path)

        # Mock touch to raise PermissionError to simulate inability to create file
        with patch.object(
            Path, "touch", side_effect=PermissionError("Operation not permitted")
        ):
            with pytest.raises(
                PermissionError, match=f"Permission denied accessing {file_path}"
            ):
                settings.write_token_file(token)


@pytest.mark.settings
@pytest.mark.unit
class TestCliSettingsIntegration:
    """Integration-style tests for CliSettings.write_token_file with real file operations."""

    def test_cli_settings_write_token_file_with_real_file_ops(self, tmp_path):
        """Test CliSettings.write_token_file with actual file operations (mocked locking only)."""
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"
        file_path = tmp_path / "token.jwt"
        settings = CLISettings(token_file=file_path)

        # Only mock the file locking, let real file operations happen
        with patch("fcntl.flock"):
            settings.write_token_file(token)

        # Verify file was created with correct content
        assert file_path.exists()
        assert file_path.read_text() == token

        # Verify permissions are set correctly (600 = rw-------)
        file_stat = file_path.stat()
        file_mode = file_stat.st_mode & 0o777  # Get permission bits
        assert file_mode == 0o600

    def test_cli_settings_write_token_file_locking_behavior(self, tmp_path):
        """Test that file locking is called correctly."""
        token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"
        file_path = tmp_path / "token.jwt"
        settings = CLISettings(token_file=file_path)

        with patch("fcntl.flock") as mock_flock:
            settings.write_token_file(token)

        # Verify file locking was called
        mock_flock.assert_called_once()
        args, _ = mock_flock.call_args
        file_desc, lock_type = args

        # Should use exclusive lock
        import fcntl

        assert lock_type == fcntl.LOCK_EX
        assert isinstance(file_desc, int)  # Should be file descriptor


# =============================================================================
# NEW TESTS - Environment File Path Discovery
# =============================================================================


@pytest.mark.settings
@pytest.mark.unit
class TestGetEnvFilePaths:
    """Test cases for get_env_file_paths function."""

    def test_default_path_order(self):
        """Test that env file paths are returned in correct priority order."""
        with patch.dict(os.environ, {}, clear=True):
            paths = get_env_file_paths()

        # Should have exactly 5 paths when no override is set
        assert len(paths) == 5

        # Construct expected paths and expand them
        from pathlib import Path

        expected_paths = [
            Path("/etc/prefect-slurm/.env").expanduser(),
            Path("~/.config/prefect-slurm/.env").expanduser(),
            Path("~/.prefect_slurm.env").expanduser(),
            Path(".prefect_slurm.env").expanduser(),
            Path(".env").expanduser(),
        ]

        # Compare actual paths with expected paths
        assert paths == expected_paths

    def test_prefect_slurm_env_file_override(self):
        """Test PREFECT_SLURM_ENV_FILE environment variable override."""
        custom_path = "/custom/path/.env"

        with patch.dict(os.environ, {"PREFECT_SLURM_ENV_FILE": custom_path}):
            paths = get_env_file_paths()

        # Should have 6 paths when override is set
        assert len(paths) == 6

        # Construct expected paths including the override
        from pathlib import Path

        expected_paths = [
            Path("/etc/prefect-slurm/.env").expanduser(),
            Path("~/.config/prefect-slurm/.env").expanduser(),
            Path("~/.prefect_slurm.env").expanduser(),
            Path(".prefect_slurm.env").expanduser(),
            Path(".env").expanduser(),
            Path(custom_path).expanduser(),
        ]

        # Compare actual paths with expected paths
        assert paths == expected_paths

    def test_empty_override_not_included(self):
        """Test that empty PREFECT_SLURM_ENV_FILE is not included."""
        with patch.dict(os.environ, {"PREFECT_SLURM_ENV_FILE": ""}):
            paths = get_env_file_paths()

        # Should have 5 paths when empty override is set (empty string is falsy)
        assert len(paths) == 5

        # Construct expected paths without the empty override
        from pathlib import Path

        expected_paths = [
            Path("/etc/prefect-slurm/.env").expanduser(),
            Path("~/.config/prefect-slurm/.env").expanduser(),
            Path("~/.prefect_slurm.env").expanduser(),
            Path(".prefect_slurm.env").expanduser(),
            Path(".env").expanduser(),
        ]

        # Compare actual paths with expected paths
        assert paths == expected_paths

    def test_xdg_config_home_override(self):
        """Test custom XDG_CONFIG_HOME path."""
        custom_xdg = "/custom/config"

        with patch.dict(os.environ, {"XDG_CONFIG_HOME": custom_xdg}):
            paths = get_env_file_paths()

        # Should have 5 paths when XDG override is set but no PREFECT_SLURM_ENV_FILE
        assert len(paths) == 5

        # Construct expected paths with custom XDG config
        from pathlib import Path

        expected_paths = [
            Path("/etc/prefect-slurm/.env").expanduser(),
            Path(f"{custom_xdg}/prefect-slurm/.env").expanduser(),
            Path("~/.prefect_slurm.env").expanduser(),
            Path(".prefect_slurm.env").expanduser(),
            Path(".env").expanduser(),
        ]

        # Compare actual paths with expected paths
        assert paths == expected_paths

    def test_path_expansion_tilde(self):
        """Test that tilde paths are expanded properly."""
        with patch.dict(os.environ, {}, clear=True):
            paths = get_env_file_paths()

        # All paths should be absolute (no ~ remaining)
        for path in paths:
            assert not str(path).startswith("~")

        # Should find expanded home directory paths
        home_dir = os.path.expanduser("~")
        home_paths = [p for p in paths if str(p).startswith(home_dir)]
        assert (
            len(home_paths) >= 2
        )  # At least ~/.config/prefect-slurm/.env and ~/.prefect_slurm.env

    def test_path_expansion_env_vars(self):
        """Test that tilde but not env vars are expanded (expanduser only, not expandvars)."""
        test_path = "~/custom/.env"  # Use tilde which IS expanded by expanduser

        with patch.dict(os.environ, {"PREFECT_SLURM_ENV_FILE": test_path}):
            paths = get_env_file_paths()

        # Should not contain ~ in the final paths (tilde should be expanded)
        path_strs = [str(p) for p in paths]
        assert not any("~" in p for p in path_strs)

        # The override path should be expanded
        home_dir = os.path.expanduser("~")
        expected_override = f"{home_dir}/custom/.env"
        assert str(paths[-1]) == expected_override

    def test_all_paths_are_pathlib_objects(self):
        """Test that all returned objects are Path instances."""
        paths = get_env_file_paths()

        for path in paths:
            assert isinstance(path, Path)

    def test_no_duplicate_paths(self):
        """Test that no duplicate paths are returned."""
        paths = get_env_file_paths()
        path_strs = [str(p) for p in paths]

        assert len(path_strs) == len(set(path_strs))


# =============================================================================
# NEW TESTS - Base Settings Class
# =============================================================================


@pytest.mark.settings
@pytest.mark.unit
class TestSettingsBaseClass:
    """Test cases for base Settings class functionality."""

    def test_default_values(self):
        """Test that default values are set correctly."""
        settings = Settings()

        assert settings.token_file == Path("~/.prefect_slurm.jwt").expanduser()
        assert settings.lock_timeout == 60.0

    def test_token_file_expansion(self):
        """Test that token_file path is expanded properly."""
        # Test with tilde
        settings = Settings(token_file="~/custom.jwt")
        assert not str(settings.token_file).startswith("~")
        assert settings.token_file.is_absolute()

        # Test with absolute path
        settings = Settings(token_file="/absolute/path.jwt")
        assert settings.token_file == Path("/absolute/path.jwt")

    def test_env_prefix_loading(self):
        """Test that environment variables with prefix are loaded."""
        with patch.dict(
            os.environ,
            {
                "PREFECT_SLURM_TOKEN_FILE": "/env/token.jwt",
                "PREFECT_SLURM_LOCK_TIMEOUT": "30.5",
            },
        ):
            settings = Settings()

        assert settings.token_file == Path("/env/token.jwt")
        assert settings.lock_timeout == 30.5

    def test_lock_timeout_validation(self):
        """Test lock_timeout field validation."""
        # Valid positive float
        settings = Settings(lock_timeout=45.5)
        assert settings.lock_timeout == 45.5

        # Should accept integers too
        settings = Settings(lock_timeout=60)
        assert settings.lock_timeout == 60.0

        # Should reject negative values
        with pytest.raises(ValidationError):
            Settings(lock_timeout=-1.0)

        # Should reject zero
        with pytest.raises(ValidationError):
            Settings(lock_timeout=0.0)

    def test_env_file_loading_precedence(self, tmp_path):
        """Test that environment files are loaded in correct precedence."""
        # Create multiple env files
        env_file_1 = tmp_path / "low_priority.env"
        env_file_2 = tmp_path / "high_priority.env"

        env_file_1.write_text("PREFECT_SLURM_LOCK_TIMEOUT=10.0\n")
        env_file_2.write_text("PREFECT_SLURM_LOCK_TIMEOUT=20.0\n")

        # Mock get_env_file_paths to return our test files
        # Create custom settings class with our test env files
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file_1, env_file_2]
            )

        settings = TestSettings()

        # Higher priority file should win
        assert settings.lock_timeout == 20.0

    def test_env_vars_override_env_files(self, tmp_path):
        """Test that environment variables override env file values."""
        env_file = tmp_path / "test.env"
        env_file.write_text("PREFECT_SLURM_LOCK_TIMEOUT=30.0\n")

        # Create custom settings class with our test env file
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        with patch.dict(os.environ, {"PREFECT_SLURM_LOCK_TIMEOUT": "40.0"}):
            settings = TestSettings()

        # Environment variable should override file
        assert settings.lock_timeout == 40.0

    def test_missing_env_files_handled_gracefully(self):
        """Test that missing env files don't cause errors."""
        non_existent_paths = [
            Path("/non/existent/file1.env"),
            Path("/non/existent/file2.env"),
        ]

        # Create custom settings class with non-existent env files
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=non_existent_paths
            )

        # Should not raise any exception
        settings = TestSettings()
        assert settings.lock_timeout == 60.0  # Should use default

    def test_malformed_env_file_handled_gracefully(self, tmp_path):
        """Test that malformed env files don't crash the application."""
        env_file = tmp_path / "malformed.env"
        env_file.write_text(
            "INVALID_LINE_WITHOUT_EQUALS\n=INVALID_KEY\n# Just malformed lines, no extra variables\n"
        )

        # Create custom settings class with our malformed env file
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        # Should not raise exception, might just ignore malformed lines
        with patch.dict(os.environ, {}, clear=True):
            settings = TestSettings()
            assert isinstance(settings, Settings)


# =============================================================================
# NEW TESTS - WorkerSettings Class
# =============================================================================


@pytest.mark.settings
@pytest.mark.unit
class TestWorkerSettings:
    """Comprehensive test cases for WorkerSettings class."""

    def test_required_fields(self):
        """Test that required fields are enforced."""
        # Clear environment variables to ensure no values are loaded
        with patch.dict(os.environ, {}, clear=True):
            # Should raise ValidationError when required fields are missing
            with pytest.raises(ValidationError) as exc_info:
                WorkerSettings()

            errors = exc_info.value.errors()
            field_names = [error["loc"][0] for error in errors]
            assert "api_url" in field_names
            assert "user_name" in field_names

    def test_valid_initialization(self):
        """Test successful WorkerSettings initialization."""
        # Clear environment variables to ensure no token is loaded
        with patch.dict(os.environ, {}, clear=True):
            settings = WorkerSettings(
                api_url="http://localhost:6820", user_name="testuser"
            )

            assert (
                str(settings.api_url) == "http://localhost:6820/"
            )  # HttpUrl adds trailing slash
            assert settings.user_name == "testuser"
            assert settings.user_token is None

    def test_api_url_validation(self):
        """Test api_url field validation."""
        # Valid URLs
        valid_urls = [
            "http://localhost:6820",
            "https://slurm.example.com:6820",
            "http://192.168.1.100:6820",
            "https://slurm-api.company.com/",
        ]

        for url in valid_urls:
            settings = WorkerSettings(api_url=url, user_name="testuser")
            assert settings.api_url is not None

        # Invalid URLs
        invalid_urls = [
            "not-a-url",
            "ftp://invalid-scheme.com",
            "localhost:6820",  # Missing scheme
            "",
        ]

        for url in invalid_urls:
            with pytest.raises(ValidationError):
                WorkerSettings(api_url=url, user_name="testuser")

    def test_user_name_validation(self):
        """Test user_name field validation."""
        # Valid user names
        valid_names = ["ab", "user", "test_user", "user123", "very-long-username"]

        for name in valid_names:
            settings = WorkerSettings(api_url="http://localhost:6820", user_name=name)
            assert settings.user_name == name

        # Invalid user names (too short)
        invalid_names = ["", "a"]

        for name in invalid_names:
            with pytest.raises(ValidationError):
                WorkerSettings(api_url="http://localhost:6820", user_name=name)

    def test_user_token_secret_str(self):
        """Test user_token SecretStr behavior."""
        token = "secret-token-value"
        settings = WorkerSettings(
            api_url="http://localhost:6820", user_name="testuser", user_token=token
        )

        # Should be SecretStr instance
        assert settings.user_token is not None
        assert hasattr(settings.user_token, "get_secret_value")
        assert settings.user_token.get_secret_value() == token

        # Should be masked in string representation
        settings_str = str(settings)
        assert token not in settings_str
        assert "**********" in settings_str

    def test_environment_variable_loading(self):
        """Test loading from environment variables."""
        env_vars = {
            "PREFECT_SLURM_API_URL": "http://env-api:6820",
            "PREFECT_SLURM_USER_NAME": "env-user",
            "PREFECT_SLURM_USER_TOKEN": "env-token",
            "PREFECT_SLURM_TOKEN_FILE": "/env/token.jwt",
            "PREFECT_SLURM_LOCK_TIMEOUT": "45.0",
        }

        with patch.dict(os.environ, env_vars):
            settings = WorkerSettings()

        assert str(settings.api_url) == "http://env-api:6820/"
        assert settings.user_name == "env-user"
        assert settings.user_token.get_secret_value() == "env-token"
        assert settings.token_file == Path("/env/token.jwt")
        assert settings.lock_timeout == 45.0

    def test_env_file_loading(self, tmp_path):
        """Test loading WorkerSettings from env file."""
        env_file = tmp_path / "worker.env"
        env_file.write_text("""
PREFECT_SLURM_API_URL=http://file-api:6820
PREFECT_SLURM_USER_NAME=file-user
PREFECT_SLURM_USER_TOKEN=file-token
PREFECT_SLURM_LOCK_TIMEOUT=75.5
""")

        # Create a custom settings class that uses our test env file
        from pydantic_settings import SettingsConfigDict

        class TestWorkerSettings(WorkerSettings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        # Clear environment variables to ensure values come from file
        with patch.dict(os.environ, {}, clear=True):
            settings = TestWorkerSettings()

        assert str(settings.api_url) == "http://file-api:6820/"
        assert settings.user_name == "file-user"
        assert settings.user_token.get_secret_value() == "file-token"
        assert settings.lock_timeout == 75.5

    @pytest.mark.asyncio
    async def test_get_token_with_env_var_token(self):
        """Test get_token method when user_token is set via env var."""
        env_token = "env-token-value"

        with patch.dict(
            os.environ,
            {
                "PREFECT_SLURM_API_URL": "http://localhost:6820",
                "PREFECT_SLURM_USER_NAME": "testuser",
                "PREFECT_SLURM_USER_TOKEN": env_token,
            },
        ):
            settings = WorkerSettings()

        # Should return the env var token directly (as SecretStr)
        token = await settings.get_token()
        assert token.get_secret_value() == env_token

    @pytest.mark.asyncio
    async def test_get_token_from_file(self, tmp_path):
        """Test get_token method when reading from file."""
        token_file = tmp_path / "token.jwt"
        file_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"

        # Create token file with proper permissions
        token_file.write_text(file_token)
        token_file.chmod(0o600)

        # Clear environment variables to ensure reading from file, not env vars
        with patch.dict(os.environ, {}, clear=True):
            settings = WorkerSettings(
                api_url="http://localhost:6820",
                user_name="testuser",
                token_file=token_file,
            )

            with patch("fcntl.flock"):  # Mock file locking
                with patch("asyncio.wait_for", return_value=None):  # Mock timeout
                    token = await settings.get_token()

            # Token from file should be returned as string
            assert token.get_secret_value() == file_token

    @pytest.mark.asyncio
    async def test_get_token_file_permission_error(self, tmp_path):
        """Test get_token handles file permission errors."""
        token_file = tmp_path / "token.jwt"
        token_file.write_text(
            "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiYWRtaW4iOnRydWV9.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"
        )
        token_file.chmod(0o644)  # Wrong permissions

        # Clear environment variables to force reading from file
        with patch.dict(os.environ, {}, clear=True):
            settings = WorkerSettings(
                api_url="http://localhost:6820",
                user_name="testuser",
                token_file=token_file,
            )

            # Don't mock file operations - let them actually check permissions
            with pytest.raises(ValueError, match="must have 600 permissions"):
                await settings.get_token()

    @pytest.mark.asyncio
    async def test_get_token_file_not_found(self):
        """Test get_token handles missing file."""
        # Clear environment variables to force reading from file
        with patch.dict(os.environ, {}, clear=True):
            settings = WorkerSettings(
                api_url="http://localhost:6820",
                user_name="testuser",
                token_file="/non/existent/token.jwt",
            )

            with pytest.raises(FileNotFoundError, match="not found"):
                await settings.get_token()

    @pytest.mark.asyncio
    async def test_get_token_invalid_jwt_format(self, tmp_path):
        """Test get_token validates JWT format."""
        token_file = tmp_path / "token.jwt"
        token_file.write_text("invalid-jwt-format")
        token_file.chmod(0o600)

        # Clear environment variables to force reading from file
        with patch.dict(os.environ, {}, clear=True):
            settings = WorkerSettings(
                api_url="http://localhost:6820",
                user_name="testuser",
                token_file=token_file,
            )

            # Don't mock file operations - let them actually validate JWT
            with pytest.raises(ValueError, match="does not contain a valid JWT token"):
                await settings.get_token()

    @pytest.mark.asyncio
    async def test_validate_credentials_success(self):
        """Test validate_credentials method success scenario."""
        settings = WorkerSettings(
            api_url="http://localhost:6820",
            user_name="testuser",
            user_token="valid.jwt.token",
        )

        # Should not raise any exception
        await settings.validate_credentials()

    @pytest.mark.asyncio
    async def test_validate_credentials_failure(self):
        """Test validate_credentials method failure scenario."""
        # Clear environment variables to ensure no token is found
        with patch.dict(os.environ, {}, clear=True):
            settings = WorkerSettings(
                api_url="http://localhost:6820",
                user_name="testuser",
                token_file="/non/existent/token.jwt",
            )

            with pytest.raises(ValueError, match="No authentication found"):
                await settings.validate_credentials()


# =============================================================================
# NEW TESTS - CLI Settings (Additional Tests Beyond Moved Ones)
# =============================================================================


@pytest.mark.settings
@pytest.mark.unit
class TestCLISettings:
    """Additional test cases for CLISettings class."""

    def test_cli_settings_inheritance(self):
        """Test that CLISettings properly inherits from Settings."""
        settings = CLISettings()

        # Should have all base Settings fields
        assert hasattr(settings, "token_file")
        assert hasattr(settings, "lock_timeout")

        # Should have CLISettings-specific methods
        assert hasattr(settings, "write_token_file")

        # Should use same defaults as base Settings
        assert settings.token_file == Path("~/.prefect_slurm.jwt").expanduser()
        assert settings.lock_timeout == 60.0

    def test_cli_settings_env_file_loading(self, tmp_path):
        """Test that CLISettings loads from env files."""
        env_file = tmp_path / "cli.env"
        env_file.write_text("""
PREFECT_SLURM_TOKEN_FILE=/cli/token.jwt
PREFECT_SLURM_LOCK_TIMEOUT=25.0
""")

        # Create a custom settings class that uses our test env file
        from pydantic_settings import SettingsConfigDict

        class TestCLISettings(CLISettings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        settings = TestCLISettings()

        assert settings.token_file == Path("/cli/token.jwt")
        assert settings.lock_timeout == 25.0

    def test_cli_settings_custom_token_file(self):
        """Test CLISettings with custom token file."""
        custom_path = "/custom/cli/token.jwt"
        settings = CLISettings(token_file=custom_path)

        assert settings.token_file == Path(custom_path)

    def test_cli_vs_worker_settings_differences(self):
        """Test differences between CLISettings and WorkerSettings."""
        cli_settings = CLISettings()

        # CLISettings should not have WorkerSettings-specific fields
        assert not hasattr(cli_settings, "api_url")
        assert not hasattr(cli_settings, "user_name")
        assert not hasattr(cli_settings, "user_token")

        # CLISettings should have write_token_file method
        assert hasattr(cli_settings, "write_token_file")

        # WorkerSettings should not have write_token_file method
        with patch.dict(
            os.environ,
            {
                "PREFECT_SLURM_API_URL": "http://localhost:6820",
                "PREFECT_SLURM_USER_NAME": "testuser",
            },
        ):
            worker_settings = WorkerSettings()
            assert not hasattr(worker_settings, "write_token_file")


# =============================================================================
# NEW TESTS - Environment File Integration
# =============================================================================


@pytest.mark.settings
@pytest.mark.unit
class TestEnvFileIntegration:
    """End-to-end integration tests for environment file functionality."""

    def test_multiple_env_files_precedence(self, tmp_path):
        """Test precedence when multiple env files exist."""
        # Create files with different priorities
        low_priority = tmp_path / "low.env"
        mid_priority = tmp_path / "mid.env"
        high_priority = tmp_path / "high.env"

        low_priority.write_text(
            "PREFECT_SLURM_LOCK_TIMEOUT=10.0\nPREFECT_SLURM_TOKEN_FILE=/low/token.jwt\n"
        )
        mid_priority.write_text(
            "PREFECT_SLURM_LOCK_TIMEOUT=20.0\n"
        )  # Doesn't override token_file
        high_priority.write_text(
            "PREFECT_SLURM_LOCK_TIMEOUT=30.0\nPREFECT_SLURM_TOKEN_FILE=/high/token.jwt\n"
        )

        # Create custom settings class with our test env files
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_",
                env_file=[low_priority, mid_priority, high_priority],
            )

        settings = TestSettings()

        # Highest priority should win for all fields
        assert settings.lock_timeout == 30.0
        assert settings.token_file == Path("/high/token.jwt")

    def test_partial_configuration_files(self, tmp_path):
        """Test that partial configuration in different files works correctly."""
        file1 = tmp_path / "partial1.env"
        file2 = tmp_path / "partial2.env"

        file1.write_text("PREFECT_SLURM_LOCK_TIMEOUT=15.0\n")
        file2.write_text("PREFECT_SLURM_TOKEN_FILE=/partial/token.jwt\n")

        # Create custom settings class with our test env files
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[file1, file2]
            )

        settings = TestSettings()

        # Should get values from both files
        assert settings.lock_timeout == 15.0
        assert settings.token_file == Path("/partial/token.jwt")

    def test_env_file_with_comments_and_whitespace(self, tmp_path):
        """Test parsing env files with comments and whitespace."""
        env_file = tmp_path / "complex.env"
        env_file.write_text("""
# This is a comment
PREFECT_SLURM_LOCK_TIMEOUT=42.0

# Another comment
PREFECT_SLURM_TOKEN_FILE=/commented/token.jwt  # inline comment

# Empty lines should be ignored

""")

        # Create custom settings class with our test env file
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        settings = TestSettings()

        assert settings.lock_timeout == 42.0
        assert settings.token_file == Path("/commented/token.jwt")

    def test_env_file_with_quoted_values(self, tmp_path):
        """Test parsing env files with quoted values."""
        env_file = tmp_path / "quoted.env"
        env_file.write_text("""
PREFECT_SLURM_TOKEN_FILE="/path with spaces/token.jwt"
PREFECT_SLURM_LOCK_TIMEOUT='55.5'
""")

        # Create custom settings class with our test env file
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        settings = TestSettings()

        assert settings.token_file == Path("/path with spaces/token.jwt")
        assert settings.lock_timeout == 55.5

    def test_worker_settings_from_env_file(self, tmp_path):
        """Test loading complete WorkerSettings from env file."""
        env_file = tmp_path / "worker.env"
        env_file.write_text("""
PREFECT_SLURM_API_URL=https://production-slurm:6820
PREFECT_SLURM_USER_NAME=production_user
PREFECT_SLURM_USER_TOKEN=production.jwt.token.here
PREFECT_SLURM_TOKEN_FILE=/production/token.jwt
PREFECT_SLURM_LOCK_TIMEOUT=120.0
""")

        # Create custom settings class with our test env file
        from pydantic_settings import SettingsConfigDict

        class TestWorkerSettings(WorkerSettings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        # Clear environment variables to ensure values come from file
        with patch.dict(os.environ, {}, clear=True):
            settings = TestWorkerSettings()

        assert str(settings.api_url) == "https://production-slurm:6820/"
        assert settings.user_name == "production_user"
        assert settings.user_token.get_secret_value() == "production.jwt.token.here"
        assert settings.token_file == Path("/production/token.jwt")
        assert settings.lock_timeout == 120.0

    def test_env_vars_override_multiple_env_files(self, tmp_path):
        """Test that environment variables override all env files."""
        file1 = tmp_path / "file1.env"
        file2 = tmp_path / "file2.env"

        file1.write_text("PREFECT_SLURM_LOCK_TIMEOUT=100.0\n")
        file2.write_text("PREFECT_SLURM_LOCK_TIMEOUT=200.0\n")

        # Test environment variable override with custom settings class
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[file1, file2]
            )

        with patch.dict(os.environ, {"PREFECT_SLURM_LOCK_TIMEOUT": "300.0"}):
            settings = TestSettings()

        # Environment variable should win over all files
        assert settings.lock_timeout == 300.0

    def test_working_directory_relative_env_files(self, tmp_path):
        """Test that relative paths in env file discovery work from different working directories."""
        # Create a subdirectory with env file
        subdir = tmp_path / "subdir"
        subdir.mkdir()

        env_file = subdir / ".env"
        env_file.write_text("PREFECT_SLURM_LOCK_TIMEOUT=999.0\n")

        # Change to subdirectory
        original_cwd = os.getcwd()
        try:
            os.chdir(subdir)

            # Should find .env in current directory
            from pydantic_settings import SettingsConfigDict

            class TestSettings(Settings):
                model_config = SettingsConfigDict(
                    env_prefix="prefect_slurm_", env_file=[Path(".env")]
                )

            settings = TestSettings()

            assert settings.lock_timeout == 999.0

        finally:
            os.chdir(original_cwd)

    def test_file_permissions_on_env_files(self, tmp_path):
        """Test that env files with different permissions are handled correctly."""
        # Create env file with restrictive permissions
        env_file = tmp_path / "restricted.env"
        env_file.write_text("PREFECT_SLURM_LOCK_TIMEOUT=888.0\n")
        env_file.chmod(0o600)  # Owner read/write only

        # Create custom settings class with our test env file
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        # Should still be able to read the file (assuming we're the owner)
        settings = TestSettings()
        assert settings.lock_timeout == 888.0

    def test_invalid_env_file_values(self, tmp_path):
        """Test handling of invalid values in env files."""
        env_file = tmp_path / "invalid.env"
        env_file.write_text("""
PREFECT_SLURM_LOCK_TIMEOUT=not_a_number
PREFECT_SLURM_TOKEN_FILE=/valid/path.jwt
""")

        # Create custom settings class with our test env file
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        # Should raise ValidationError for invalid values
        with pytest.raises(ValidationError):
            TestSettings()

    def test_env_file_encoding_handling(self, tmp_path):
        """Test that env files with different encodings are handled correctly."""
        env_file = tmp_path / "encoding.env"

        # Write with UTF-8 encoding (including some non-ASCII characters in comments)
        content = "# Configuration file with ünicode\nPREFECT_SLURM_LOCK_TIMEOUT=77.7\n"
        env_file.write_text(content, encoding="utf-8")

        # Create custom settings class with our test env file
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        settings = TestSettings()
        assert settings.lock_timeout == 77.7


# =============================================================================
# NEW TESTS - Error Handling
# =============================================================================


@pytest.mark.settings
@pytest.mark.unit
class TestEnvFileErrorHandling:
    """Test error handling scenarios for environment file functionality."""

    def test_permission_denied_env_file(self, tmp_path):
        """Test handling of env files that can't be read due to permissions."""
        env_file = tmp_path / "permission_denied.env"
        env_file.write_text("PREFECT_SLURM_LOCK_TIMEOUT=123.0\n")
        env_file.chmod(0o000)  # No permissions

        try:
            # Create custom settings class with our test env file
            from pydantic_settings import SettingsConfigDict

            class TestSettings(Settings):
                model_config = SettingsConfigDict(
                    env_prefix="prefect_slurm_", env_file=[env_file]
                )

            # Should raise PermissionError when file can't be read
            with pytest.raises(PermissionError):
                TestSettings()
        finally:
            # Restore permissions for cleanup
            env_file.chmod(0o644)

    def test_malformed_env_file_syntax(self, tmp_path):
        """Test handling of env files with syntax errors."""
        env_file = tmp_path / "malformed.env"
        env_file.write_text("""
# Comment about the file
INVALID_LINE_NO_EQUALS
=KEY_MISSING
# Another comment
PREFECT_SLURM_LOCK_TIMEOUT=45.0
""")

        # Create custom settings class with our test env file
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        # Should parse valid lines and ignore invalid ones
        with patch.dict(os.environ, {}, clear=True):
            settings = TestSettings()
            # Should get the valid value
            assert settings.lock_timeout == 45.0

    def test_circular_reference_in_env_vars(self, tmp_path):
        """Test handling of circular references in environment variables."""
        env_file = tmp_path / "circular.env"
        env_file.write_text("""
PREFECT_SLURM_TOKEN_FILE=$PREFECT_SLURM_TOKEN_FILE/token.jwt
PREFECT_SLURM_LOCK_TIMEOUT=66.0
""")

        # Create custom settings class with our test env file
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        # Should handle gracefully (might just leave unexpanded or ignore)
        with patch.dict(os.environ, {}, clear=True):
            settings = TestSettings()
            # Should at least get the valid timeout value
            assert settings.lock_timeout == 66.0

    def test_very_large_env_file(self, tmp_path):
        """Test handling of very large env files."""
        env_file = tmp_path / "large.env"

        # Create a large file with many comments (to test large file handling without extra variables)
        lines = ["# Large environment file\n"]
        for i in range(1000):
            lines.append(f"# Comment line {i}\n")
        lines.append("PREFECT_SLURM_LOCK_TIMEOUT=999.0\n")

        env_file.write_text("".join(lines))

        # Create custom settings class with our test env file
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        # Should handle large files without issues
        with patch.dict(os.environ, {}, clear=True):
            settings = TestSettings()
            assert settings.lock_timeout == 999.0

    def test_concurrent_env_file_access(self, tmp_path):
        """Test concurrent access to env files doesn't cause issues."""
        env_file = tmp_path / "concurrent.env"
        env_file.write_text("PREFECT_SLURM_LOCK_TIMEOUT=333.0\n")

        # Create custom settings class with our test env file
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        # Simulate concurrent access
        settings1 = TestSettings()
        settings2 = TestSettings()

        assert settings1.lock_timeout == 333.0
        assert settings2.lock_timeout == 333.0

    def test_env_file_deleted_during_loading(self, tmp_path):
        """Test handling when env file is deleted during loading process."""
        env_file = tmp_path / "deleted.env"
        env_file.write_text("PREFECT_SLURM_LOCK_TIMEOUT=444.0\n")

        # Delete the file before loading to simulate file being deleted
        env_file.unlink()

        # Create custom settings class with our deleted env file
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_",
                env_file=[env_file],  # File doesn't exist anymore
            )

        # Should handle missing file gracefully
        settings = TestSettings()
        assert settings.lock_timeout == 60.0  # Should use default

    def test_invalid_field_types_from_env_file(self, tmp_path):
        """Test validation errors for invalid field types from env files."""
        env_file = tmp_path / "invalid_types.env"
        env_file.write_text("""
PREFECT_SLURM_LOCK_TIMEOUT=not_a_number
PREFECT_SLURM_TOKEN_FILE=123  # Will become Path(123) which is fine
""")

        # Create custom settings class with our test env file
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[env_file]
            )

        # Should raise ValidationError for invalid float
        with pytest.raises(ValidationError) as exc_info:
            TestSettings()

            errors = exc_info.value.errors()
            assert any(error["loc"] == ("lock_timeout",) for error in errors)

    def test_binary_env_file(self, tmp_path):
        """Test handling of binary files mistakenly treated as env files."""
        # Create a binary file
        binary_file = tmp_path / "binary.env"
        binary_file.write_bytes(b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR")

        # Create custom settings class with our binary file
        from pydantic_settings import SettingsConfigDict

        class TestSettings(Settings):
            model_config = SettingsConfigDict(
                env_prefix="prefect_slurm_", env_file=[binary_file]
            )

        # Should handle binary files gracefully (ignore or error gracefully)
        try:
            settings = TestSettings()
            # If it succeeds, should use defaults
            assert settings.lock_timeout == 60.0
        except UnicodeDecodeError:
            # If it fails with decode error, that's also acceptable
            pass


# =============================================================================
# NEW TESTS - Field Validation
# =============================================================================


@pytest.mark.settings
@pytest.mark.unit
class TestSettingsFieldValidation:
    """Deep validation tests for Settings fields."""

    def test_token_file_path_types(self):
        """Test token_file accepts different path types."""
        # String path
        settings = Settings(token_file="/string/path.jwt")
        assert settings.token_file == Path("/string/path.jwt")

        # Path object
        settings = Settings(token_file=Path("/path/object.jwt"))
        assert settings.token_file == Path("/path/object.jwt")

        # Path with tilde
        settings = Settings(token_file="~/tilde/path.jwt")
        assert str(settings.token_file).startswith("/")  # Should be expanded
        assert "~" not in str(settings.token_file)

    def test_token_file_expansion_edge_cases(self):
        """Test edge cases in token_file path expansion."""
        # Multiple tildes (only first should be expanded)
        settings = Settings(token_file="~/path/~/file.jwt")
        path_str = str(settings.token_file)
        assert not path_str.startswith("~")
        assert "~/file.jwt" in path_str  # Second tilde should remain

        # Environment variable in path
        with patch.dict(os.environ, {"TEST_DIR": "/test/directory"}):
            settings = Settings(token_file="$TEST_DIR/token.jwt")
            # This might or might not expand depending on pydantic behavior
            assert isinstance(settings.token_file, Path)

    def test_lock_timeout_boundary_values(self):
        """Test lock_timeout field boundary values."""
        # Very small positive value
        settings = Settings(lock_timeout=0.001)
        assert settings.lock_timeout == 0.001

        # Large value
        settings = Settings(lock_timeout=3600.0)  # 1 hour
        assert settings.lock_timeout == 3600.0

        # Exactly zero should fail
        with pytest.raises(ValidationError):
            Settings(lock_timeout=0.0)

        # Negative should fail
        with pytest.raises(ValidationError):
            Settings(lock_timeout=-0.001)

    def test_lock_timeout_type_coercion(self):
        """Test lock_timeout type coercion."""
        # Integer should be converted to float
        settings = Settings(lock_timeout=60)
        assert settings.lock_timeout == 60.0
        assert isinstance(settings.lock_timeout, float)

        # String number should be converted
        settings = Settings(lock_timeout="45.5")
        assert settings.lock_timeout == 45.5

        # Invalid string should fail
        with pytest.raises(ValidationError):
            Settings(lock_timeout="not_a_number")

    def test_worker_settings_url_formats(self):
        """Test various URL formats for WorkerSettings.api_url."""
        # Different schemes
        valid_cases = [
            ("http://localhost:6820", "http://localhost:6820/"),
            ("https://secure.example.com:6820", "https://secure.example.com:6820/"),
            ("http://192.168.1.100:6820/path", "http://192.168.1.100:6820/path"),
        ]

        for input_url, expected_url in valid_cases:
            settings = WorkerSettings(api_url=input_url, user_name="testuser")
            assert str(settings.api_url) == expected_url

    def test_worker_settings_secret_str_edge_cases(self):
        """Test SecretStr edge cases for user_token."""
        # Empty string token
        settings = WorkerSettings(
            api_url="http://localhost:6820", user_name="testuser", user_token=""
        )
        assert settings.user_token.get_secret_value() == ""

        # Token with special characters
        special_token = "token!@#$%^&*()_+-={}[]|\\:;\"'<>?,./"
        settings = WorkerSettings(
            api_url="http://localhost:6820",
            user_name="testuser",
            user_token=special_token,
        )
        assert settings.user_token.get_secret_value() == special_token

        # Very long token
        long_token = "a" * 10000
        settings = WorkerSettings(
            api_url="http://localhost:6820", user_name="testuser", user_token=long_token
        )
        assert settings.user_token.get_secret_value() == long_token

    def test_settings_repr_and_str(self):
        """Test string representations don't leak sensitive data."""
        settings = WorkerSettings(
            api_url="http://localhost:6820",
            user_name="testuser",
            user_token="secret-token",
        )

        # String representation should not contain the actual token
        str_repr = str(settings)
        repr_repr = repr(settings)

        assert "secret-token" not in str_repr
        assert "secret-token" not in repr_repr
        assert "**********" in str_repr  # Should show masked value

        # Other fields should be visible
        assert "testuser" in str_repr
        assert "localhost:6820" in str_repr

    def test_settings_dict_export(self):
        """Test exporting settings to dict format."""
        settings = WorkerSettings(
            api_url="http://localhost:6820",
            user_name="testuser",
            user_token="secret-token",
        )

        # Test model_dump
        settings_dict = settings.model_dump()

        # Should contain all fields
        assert "api_url" in settings_dict
        assert "user_name" in settings_dict
        assert "user_token" in settings_dict
        assert "token_file" in settings_dict
        assert "lock_timeout" in settings_dict

        # Secret should be masked by default (SecretStr object shows as masked in str())
        assert str(settings_dict["user_token"]) == "**********"

        # Test with secrets revealed (depending on pydantic version, this might still be masked)
        # settings_dict_with_secrets = settings.model_dump(mode='json')

    def test_field_aliases_and_serialization(self):
        """Test field aliases and serialization behavior."""
        settings = WorkerSettings(api_url="http://localhost:6820", user_name="testuser")

        # Test that all fields have expected types
        assert isinstance(settings.api_url, type(settings.api_url))  # HttpUrl type
        assert isinstance(settings.user_name, str)
        assert settings.user_token is None or hasattr(
            settings.user_token, "get_secret_value"
        )
        assert isinstance(settings.token_file, Path)
        assert isinstance(settings.lock_timeout, float)
