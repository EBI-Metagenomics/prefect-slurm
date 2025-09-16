import asyncio
import fcntl
import os
import re
import stat
from pathlib import Path
from typing import Optional

import aiofiles
from pydantic import Field, HttpUrl, PositiveFloat, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

JWT_PATTERN = re.compile(r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$")


def get_env_file_paths():
    # From low priority to high priority
    # 1. System-wide (optional): /etc/prefect-slurm/.env
    # 2. XDG Config (Linux/Unix): ~/.config/prefect-slurm/.env
    # 3. User Home: ~/.prefect_slurm.env
    # 4. Current Directory (app-specific): ./.prefect_slurm.env
    # 5. Current Directory: ./.env
    # 6. Environment Variable Override: PREFECT_SLURM_ENV_FILE

    env_path_override = os.environ.get("PREFECT_SLURM_ENV_FILE", None)
    xdg_config_path = os.environ.get("XDG_CONFIG_HOME", "~/.config")

    paths = [
        Path("/etc/prefect-slurm/.env"),
        Path(xdg_config_path) / "prefect-slurm/.env",
        Path("~/.prefect_slurm.env"),
        Path(".prefect_slurm.env"),
        Path(".env"),
    ]

    if env_path_override:
        paths.append(Path(env_path_override))

    return [path.expanduser() for path in paths]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="prefect_slurm_", env_file=get_env_file_paths()
    )

    token_file: Path = Field("~/.prefect_slurm.jwt")
    lock_timeout: PositiveFloat = Field(60.0)

    @field_validator("token_file", mode="after")
    def expand_user(cls, value: Path):
        return value.expanduser()


class WorkerSettings(Settings):
    api_url: HttpUrl
    user_name: str = Field(min_length=2)
    user_token: Optional[SecretStr] = Field(None)

    async def get_token(self):
        """Read token from file with async locking and permission validation.

        Always validates file permissions (600) and reads fresh content.
        Uses async file operations with file locking, waiting for any concurrent writes to complete.
        Includes configurable timeout to prevent indefinite waiting.


        :returns: Token content (stripped)
        :rtype: str

        :raises ValueError: If file permissions are incorrect, file is empty, or access fails
        :raises FileNotFoundError: If token file doesn't exist
        :raises PermissionError: If no permission to read file
        :raises OSError: If lock timeout occurs or other OS errors during file operations
        """
        if self.user_token is not None:
            return self.user_token

        try:
            file_stat = os.stat(self.token_file)
            file_mode = stat.S_IMODE(file_stat.st_mode)

            if file_mode != 0o600:
                raise ValueError(
                    f"Token file {self.token_file} must have 600 permissions "
                    f"(owner read/write only). Current permissions: {oct(file_mode)}"
                )
        except FileNotFoundError:
            raise FileNotFoundError(f"Token file {self.token_file} not found")

        try:
            async with aiofiles.open(self.token_file, "r", encoding="utf-8") as f:
                fd = f.fileno()

                await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None, fcntl.flock, fd, fcntl.LOCK_SH
                    ),
                    timeout=self.lock_timeout,
                )

                content = await f.read()

            content = content.strip()

            if not content:
                raise ValueError(f"Token file {self.token_file} is empty")

            if not bool(JWT_PATTERN.match(content)):
                raise ValueError(
                    f"Token file {self.token_file} does not contain a valid JWT token"
                )

            return SecretStr(content)

        except asyncio.TimeoutError:
            raise OSError(
                f"Timeout after {self.lock_timeout}s waiting for file lock on {self.token_file}. "
                f"Another process may be writing to the file. "
                f"Adjust PREFECT_SLURM_LOCK_TIMEOUT environment variable if needed."
            )
        except (OSError, IOError) as e:
            raise OSError(f"Error reading token file {self.token_file}: {e}") from e

    async def validate_credentials(self):
        try:
            await self.get_token()
        except Exception as e:
            raise ValueError(
                f"No authentication found. Either set PREFECT_SLURM_USER_TOKEN "
                f"environment variable or create token file: {self.token_file}"
            ) from e


class CLISettings(Settings):
    def write_token_file(self, token: str) -> None:
        """
        Write JWT token to file with exclusive lock and proper permissions.

        :param token: JWT token to write

        :raises OSError: If file operations fail
        :raises ValueError: If token is invalid
        """
        if not token:
            raise ValueError("Token cannot be empty")

        # Validate JWT format
        parts = token.split(".")
        if len(parts) != 3 or not all(part for part in parts):
            raise ValueError(f"Invalid JWT format: {token}")

        try:
            if not self.token_file.exists():
                # Ensure parent directory exists
                self.token_file.parent.mkdir(parents=True, exist_ok=True)
                self.token_file.touch(0o600)
            else:
                # Set 600 permissions (owner read/write only)
                os.chmod(self.token_file, 0o600)
        except PermissionError as e:
            raise PermissionError(
                f"Permission denied accessing {self.token_file}: {e}"
            ) from e
        except FileNotFoundError as e:
            raise FileNotFoundError(f"File not found: {self.token_file}") from e
        except OSError as e:
            raise OSError(f"File system error with {self.token_file}: {e}") from e

        # Write token with exclusive lock
        try:
            with open(self.token_file, "w") as f:
                # Acquire exclusive lock
                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
                f.write(token)
                f.flush()
                os.fsync(f.fileno())
        except OSError as e:
            raise OSError(f"Failed to write token file {self.token_file}: {e}") from e
