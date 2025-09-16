#!/usr/bin/env python3
"""
Simple example flow to test Slurm worker implementation
"""

from prefect import flow


@flow(name="slurm-hello-world", log_prints=True)
def hello_world():
    """Test flow to validate Slurm worker execution"""

    print("Hello form a prefect flow!")


if __name__ == "__main__":
    # For local testing
    hello_world()
