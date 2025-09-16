#!/usr/bin/env python3
"""
Simple example flow to test Slurm worker implementation
"""

from prefect import flow, task


@task
def compute_fibonacci(n: int = 10):
    """Compute fibonacci sequence up to n"""
    if n <= 0:
        return []
    elif n == 1:
        return [0]
    elif n == 2:
        return [0, 1]

    fib = [0, 1]
    for i in range(2, n):
        fib.append(fib[i - 1] + fib[i - 2])
    return fib


@flow(name="slurm-test-flow", log_prints=True)
def fibonacci_flow():
    """Test flow to validate Slurm worker execution"""

    fib_result = compute_fibonacci(15)
    print(f"Fibonacci sequence: {fib_result}")

    return fib_result


if __name__ == "__main__":
    # For local testing
    result = fibonacci_flow()
    print(f"Flow result: {result}")
