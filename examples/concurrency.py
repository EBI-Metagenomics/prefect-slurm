#!/usr/bin/env python3

import time

from prefect import flow


@flow(name="slurm-concurrency-test")
def concurrency_test_flow(sleep_seconds: int = 30):
    time.sleep(sleep_seconds)


if __name__ == "__main__":
    concurrency_test_flow()
