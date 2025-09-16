#!/usr/bin/env python3
"""
CLI utilities for Prefect Slurm worker.
"""

import re
import sys
from typing import Optional

import click
from prefect_slurm.settings import CLISettings

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

    :param text: Input text that may contain a JWT token

    :returns: JWT token if found, None otherwise
    :rtype: str
    """
    for line in text.splitlines():
        match = jwt_pattern.match(line.strip())

        if match:
            return match.group(2)

    return None


@click.group()
def cli():
    """Prefect Slurm worker utilities."""
    pass


@cli.command()
@click.argument("token_file", required=False)
@click.help_option("-h", "--help")
def token(token_file: Optional[str]):
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

        if token_file:
            settings = CLISettings(token_file=token_file)
        else:
            settings = CLISettings()

        settings.write_token_file(jwt)

        # Success output
        click.echo(f"✓ Token successfully written to {settings.token_file}")
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
