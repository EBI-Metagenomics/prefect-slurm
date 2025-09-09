#!/usr/bin/env python3
"""
CLI utilities for Prefect Slurm worker.
"""

import fcntl
import os
import re
import sys
from pathlib import Path
from typing import Optional

import click

jwt_pattern = re.compile(
    r"^(.+[=\s])?([A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)$"
)


def extract_jwt_token(text: str) -> Optional[str]:
    """
    Extract a JWT token from text input.

    Looks for JWT pattern in various formats:
    - SLURM_JWT=<token>
    - Just the JWT token on its own line
    - JWT token anywhere in the text

    Args:
        text: Input text that may contain a JWT token

    Returns:
        str: JWT token if found, None otherwise
    """
    for line in text.splitlines():
        match = jwt_pattern.match(line.strip())

        if match:
            return match.group(2)

    return None


def write_token_file(token: str, file_path: Path) -> None:
    """
    Write JWT token to file with exclusive lock and proper permissions.

    Args:
        token: JWT token to write
        file_path: Path to token file

    Raises:
        OSError: If file operations fail
        ValueError: If token is invalid
    """
    if not token:
        raise ValueError("Token cannot be empty")

    # Validate JWT format
    parts = token.split(".")
    if len(parts) != 3 or not all(part for part in parts):
        raise ValueError(f"Invalid JWT format: {token}")

    try:
        if not file_path.exists():
            # Ensure parent directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.touch(0o600)
        else:
            # Set 600 permissions (owner read/write only)
            os.chmod(file_path, 0o600)
    except PermissionError as e:
        raise PermissionError(f"Permission denied accessing {file_path}: {e}") from e
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {file_path}") from e
    except OSError as e:
        raise OSError(f"File system error with {file_path}: {e}") from e

    # Write token with exclusive lock
    try:
        with open(file_path, "w") as f:
            # Acquire exclusive lock
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            f.write(token)
            f.flush()
            os.fsync(f.fileno())
    except OSError as e:
        raise OSError(f"Failed to write token file {file_path}: {e}") from e


@click.group()
def cli():
    """Prefect Slurm worker utilities."""
    pass


@cli.command()
@click.argument("token_file", required=False)
@click.help_option("-h", "--help")
def token(token_file):
    """
    Read JWT token from stdin and write to secure token file.

    TOKEN_FILE: Optional path to token file. Uses PREFECT_SLURM_TOKEN_FILE env var or ~/.prefect_slurm.jwt if not provided.

    Examples:

      scontrol token username=user lifespan=100 | prefect-slurm token

      echo 'jwt_token' | prefect-slurm token ~/custom_path.jwt

      prefect-slurm token < token_file.txt
    """
    try:
        # Read from stdin
        stdin_input = sys.stdin.read()
        if not stdin_input.strip():
            click.echo("Error: No input provided via stdin", err=True)
            click.echo("Hint: Try piping token input or use --help for usage", err=True)
            sys.exit(1)

        # Extract JWT token from input
        jwt = extract_jwt_token(stdin_input)
        if not jwt:
            click.echo("Error: No valid JWT token found in input", err=True)
            click.echo(f"Input received: {stdin_input[:100]}...", err=True)
            click.echo("Expected: JWT format with 3 parts separated by dots", err=True)
            sys.exit(1)

        # Determine output file path
        if token_file:
            file_path = Path(token_file).expanduser()
        else:
            env_path = os.getenv("PREFECT_SLURM_TOKEN_FILE")
            if env_path:
                file_path = Path(env_path).expanduser()
            else:
                file_path = Path("~/.prefect_slurm.jwt").expanduser()

        # Write token to file
        write_token_file(jwt, file_path)

        # Success output
        click.echo(f"✓ Token successfully written to {file_path}")
        click.echo("✓ File permissions set to 600")

    except ValueError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except OSError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
    except KeyboardInterrupt:
        click.echo("\nOperation cancelled", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()
