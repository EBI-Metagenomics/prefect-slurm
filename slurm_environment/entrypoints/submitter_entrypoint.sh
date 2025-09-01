#!/bin/bash
set -e

# Create initial token file
scontrol token username=slurm lifespan=4000 | prefect-slurm token

# Start token update cronjob
echo "0 * * * * /bin/bash -c 'source /home/slurm/app/.venv/bin/activate && scontrol token username=slurm lifespan=4000 | prefect-slurm token' >> /var/log/slurm/token_refresh.log 2>&1" | crontab -u slurm -
sudo service cron start

# Start prefect-slurm worker in dev mode
watchmedo auto-restart \
    --directory=/home/slurm/app/prefect_slurm \
    --pattern=*.py \
    --recursive \
    -- \
    prefect worker start --pool slurm-pool --type slurm --name donco-slurm-worker
