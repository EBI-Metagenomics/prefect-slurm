#!/bin/bash
set -e

echo "ℹ️ Start dbus"
service dbus start

echo "ℹ️ Ensure ownership of munge key"
chown -R munge:munge /etc/munge

echo "ℹ️ Start munged for auth"
gosu munge /usr/sbin/munged

echo "ℹ️ Start slurm dbd for job persistence"
slurmdbd -vvv
echo "slurm dbd started"

while ! nc -z localhost 6819; do
    echo "ℹ️ Waiting for slurm dbd to be ready..."
    sleep 2
done

echo "ℹ️ Start slurm controller"
slurmctld -vvv

while ! nc -z localhost 6817; do
    echo "ℹ️ Waiting for slurm ctl to be ready..."
    sleep 2
done

echo "ℹ️ Start slurm worker daemon"
slurmd -vvv

export SLURM_JWT=daemon
# Disabling security checks for slurmrestd since we're running it locally (otherwise a separate user would be needed)
export SLURMRESTD_SECURITY=disable_unshare_sysv,disable_unshare_files,disable_user_check

echo "ℹ️ Start slurm rest api daemon"
slurmrestd 0.0.0.0:6820 > /var/log/slurm/slurmrestd.log 2>&1 &


tail -f /var/log/slurm/slurmdbd.log /var/log/slurm/slurmd.log /var/log/slurm/slurmctld.log /var/log/slurm/slurmrestd.log

exec "$@"
