# Prefect-Slurm

A Prefect worker designed to execute Prefect flows on a Slurm HPC cluster.

## Overview

This repository contains a Prefect worker implementation that runs on a Slurm submitter node and submits each flow run as a new Slurm job via the Slurm REST API.

### How it works

1. The Prefect worker runs on a Slurm submitter node
2. The worker polls a prefect server for compatible flow runs in its work queue
3. The worker creates a new Slurm job specification for the flow run
4. The job is submitted to the Slurm cluster via the Slurm REST API
5. The Slurm scheduler allocates resources and executes the flow
6. The worker polls the Slurm scheduler for progress and pushes status and logs back to the Prefect API
7. The worker has lifecycle methods to try and pick up flowruns that _should be currently running_, according to the prefect server, when the worker starts. These lifecycle methods are crucial to resumability after downtime, considering that the type of flowruns executed on a Slurm cluster are likely to be long-running HPC jobs like data pipelines.

This architecture allows for isolation of each flow run in its own Slurm job.

## Development Setup

This project uses Poetry for dependency management. Follow these steps to set up your development environment:

### Prerequisites

- Python 3.11 or newer (but less than 3.14)
- Poetry installed on your system ([Installation instructions](https://python-poetry.org/docs/#installation))
- Access to a Slurm cluster with REST API enabled (for actually running)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/EBI-metagenomics/prefect-slurm.git
   cd prefect-slurm
   ```

2. Install dependencies using Poetry:
   ```bash
   poetry install
   ```

3. Activate the virtual environment:
   ```bash
   poetry shell
   ```

### Configuration

To configure the worker for your Slurm environment, you'll need to:

1. Set up authentication for the Slurm REST API
2. Configure Prefect to use this worker
3. Set appropriate environment variables for your Slurm cluster

Detailed configuration instructions will be provided as the project develops.

## Usage

TODO

## License

Apache 2.0

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.