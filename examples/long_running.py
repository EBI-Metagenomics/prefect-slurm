#!/usr/bin/env python3
"""
Simple example flow to test Slurm worker implementation
"""

import time
from datetime import datetime
from prefect import flow


@flow(name="long-running-flow", log_prints=True)
def long_running_flow():
    """Test flow to validate Slurm worker execution"""

    print("Starting Slurm Very Long Flow")

    for _ in range(int(86400 / 5)):
        print(datetime.now())
        time.sleep(5)

    print("Finished Very Long Flow")


if __name__ == "__main__":
    # For local testing
    result = long_running_flow()
