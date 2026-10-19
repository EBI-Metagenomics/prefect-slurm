#!/usr/bin/env python3
"""
Example flow to validate that container bind-mounted paths are visible
inside a Singularity/Apptainer container execution.
"""

from pathlib import Path

from prefect import flow


@flow(name="slurm-container-bind-check", log_prints=True)
def container_bind_check(marker_path: str):
    """Read a marker file to prove the path hosting it was bind-mounted."""
    content = Path(marker_path).read_text().strip()
    print(f"Read marker file {marker_path!r}: {content!r}")


if __name__ == "__main__":
    # For local testing
    container_bind_check("/tmp/marker.txt")
