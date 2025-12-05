FROM ubuntu:noble AS slurm-base
LABEL authors="mgnify team"

RUN groupadd -r slurm
RUN useradd -rm -d /home/slurm -s /bin/bash -g slurm -G sudo slurm
RUN mkdir -p /home/slurm && chown -R slurm:slurm /home/slurm
RUN useradd -rms /bin/bash munge

RUN apt -y update && apt -y upgrade && apt -y install curl nano wget zip bzip2 rsync build-essential

RUN apt -y install python3-pip python-is-python3

RUN apt -y install sudo gosu netcat-traditional

RUN apt-get install -y \
    libmunge-dev \
    munge \
    libcurl4-openssl-dev \
    libhttp-parser-dev \
    libjson-c-dev \
    libyaml-dev \
    libjwt-dev \
    libmariadb-dev \
    libmariadb-dev-compat \
    lua5.4 \
    dbus \
    python3.12 \
    python3.12-venv \
    python3-pip \
    && ln -sf /usr/bin/python3.12 /usr/bin/python3 \
    && ln -sf /usr/bin/python3.12 /usr/bin/python

RUN curl -sL https://download.schedmd.com/slurm/slurm-24.11.6.tar.bz2 | tar -xj
RUN cd slurm-24.11.6 && \
    ./configure --sysconfdir=/etc/slurm && \
    make install -j16

COPY slurm_environment/configs/slurm.conf /etc/slurm/slurm.conf
COPY slurm_environment/configs/slurmdbd.conf /etc/slurm/slurmdbd.conf
COPY slurm_environment/configs/cgroup.conf /etc/slurm/cgroup.conf
RUN chmod 600 /etc/slurm/slurmdbd.conf
RUN chown -R slurm:slurm /etc/slurm/

RUN mkdir -p /run/munge && chown -R munge /run/munge
RUN mkdir -p /var/run/slurm && chown -R slurm:slurm /var/run/slurm
RUN mkdir -p /var/log/slurm && chown -R slurm:slurm /var/log/slurm
RUN mkdir -p /var/spool/slurmctld && chown -R slurm:slurm /var/spool/slurmctld
RUN mkdir -p /var/spool/slurmd && chown -R slurm:slurm /var/spool/slurmd

# Setup jwt key - generate random 256-bit key
RUN dd if=/dev/random of=/var/spool/slurmctld/jwt_hs256.key bs=32 count=1
RUN chown slurm:slurm /var/spool/slurmctld/jwt_hs256.key
RUN chmod 0600 /var/spool/slurmctld/jwt_hs256.key


# DBD + Ctl + Worker + REST in a single container
COPY slurm_environment/entrypoints/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Stage 2: Submitter node with Python development environment
FROM slurm-base AS submitter

RUN apt-get install -y cron

COPY slurm_environment/entrypoints/submitter_entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
RUN echo "slurm ALL=(ALL) NOPASSWD: /usr/sbin/service" >> /etc/sudoers

# Set up working directory and switch to slurm user early
RUN mkdir -p /home/slurm/app && chown -R slurm:slurm /home/slurm/app
USER slurm
WORKDIR /home/slurm/app

# Install uv as slurm user
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/home/slurm/.local/bin:$PATH"

# Copy dependency files first
COPY --chown=slurm:slurm pyproject.toml pyproject.toml 
COPY --chown=slurm:slurm uv.lock uv.lock
COPY --chown=slurm:slurm README.md ./
COPY --chown=slurm:slurm prefect_slurm/ prefect_slurm/

# Install the project in editable mode with dev dependencies
RUN uv sync --frozen

# Set PATH environment variable to include uv's virtual environment
ENV PATH="/home/slurm/app/.venv/bin:$PATH"

# Environment variables for Prefect
ENV PREFECT_API_URL=http://prefect_server:4200/api
ENV PYTHONPATH=/home/slurm/app/prefect_slurm

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
