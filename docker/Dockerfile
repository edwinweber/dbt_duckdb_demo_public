FROM python:3.13

ARG DOCKER_COMPOSE_VERSION=v2.39.4
ARG DUCKDB_VERSION=1.5.6
ARG DBT_OSS_VERSION=2.0.5
ARG DBT_OSS_SHA256=da45bb51a200617b6390ae6804158b1de4a64070d1bd8caeb4f71a282bea899a

LABEL maintainer="Edwin" \
      version="1.0.0" \
      description="Danish Democracy Data — dbt/DuckDB/Dagster pipeline"

# System dependencies for DuckDB, Azure SDK, and dbt
RUN apt-get update && \
    apt-get install -y --no-install-recommends git curl unzip ca-certificates docker-cli rsync openssh-client && \
    mkdir -p /usr/local/lib/docker/cli-plugins && \
    curl -fsSL "https://github.com/docker/compose/releases/download/${DOCKER_COMPOSE_VERSION}/docker-compose-linux-x86_64" \
        -o /usr/local/lib/docker/cli-plugins/docker-compose && \
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose && \
    rm -rf /var/lib/apt/lists/*


# Install DuckDB CLI (SQLite extension is included in the main distribution)
RUN curl -fsSL https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip && \
    unzip /tmp/duckdb.zip -d /usr/local/bin && \
    chmod +x /usr/local/bin/duckdb && \
    rm /tmp/duckdb.zip

# Install dbt-oss standalone binary (replaces dbt-core Python package).
# The binary is a self-contained executable — no Python dbt package is installed.
# dagster-dbt uses CLI mode (DbtCliResource) automatically when no dbt Python
# package is importable; see scripts/install_dbt_oss.sh for the install logic.
RUN TARBALL="dbt-core-${DBT_OSS_VERSION}-x86_64-unknown-linux-gnu.tar.gz" && \
    curl -fsSL "https://github.com/dbt-labs/dbt-core/releases/download/v${DBT_OSS_VERSION}/${TARBALL}" \
        -o /tmp/dbt.tar.gz && \
    echo "${DBT_OSS_SHA256}  /tmp/dbt.tar.gz" | sha256sum --check --quiet && \
    tar xzf /tmp/dbt.tar.gz -C /tmp && \
    install -m 0755 "$(find /tmp -maxdepth 2 -name dbt -type f | head -1)" /usr/local/bin/dbt && \
    rm -rf /tmp/dbt.tar.gz /tmp/dbt-core-*

# Create the non-root user.  UID/GID 1000 matches the host-side ownership
# documented in the one-time host setup commands in docker-compose.yml.
RUN groupadd --gid 1000 app && \
    useradd --uid 1000 --gid 1000 --create-home --home-dir /home/app --shell /bin/bash app

WORKDIR /app

# Install dependencies first (layer cached until pyproject.toml changes)
COPY pyproject.toml ./
RUN mkdir -p ddd_python && touch ddd_python/__init__.py && \
    pip install --no-cache-dir ".[dagster]"

# Copy project source and re-register the package (fast — deps already cached)
COPY . .
RUN pip install --no-cache-dir --no-deps ".[dagster]"

# Generate dbt manifest.json (required by dagster-dbt at import time)
# Use build arg for temp duckdb path so it doesn't leak into final ENV
ARG DBT_PARSE_DB=/tmp/dbt_parse.duckdb
RUN DUCKDB_DATABASE_LOCATION=${DBT_PARSE_DB} \
    STORAGE_TARGET=local \
    DANISH_DEMOCRACY_DATA_SOURCE=/data/local/Files/Bronze/DDD \
    RFAM_DATA_SOURCE=/data/local/Files/Bronze/RFAM \
    sh -c 'cd dbt && dbt deps && dbt parse' && \
    rm -f ${DBT_PARSE_DB}

# Make the entrypoint executable before dropping privileges.
RUN chmod +x docker/docker-entrypoint.sh

# Create volume-mount directories, write the DuckDB CLI config for the app
# user, then hand ownership of everything to app in one layer.
# The /data subtree is created here so Docker does not auto-create the dirs as
# root when the bind mounts are attached — keeping host-side ownership intact.
RUN mkdir -p /data/dlt_pipelines /data/duckdb /data/dbt_logs /data/dagster /data/local && \
    printf '%s\n' \
        "SET ca_cert_file='/etc/ssl/certs/ca-certificates.crt';" \
        "SET azure_transport_option_type='curl';" \
        > /home/app/.duckdbrc && \
    chown -R app:app /app /home/app /data

# Default environment variables pointing to persistent volume paths
ENV STORAGE_TARGET=local \
    LOCAL_STORAGE_PATH=/data/local \
    DANISH_DEMOCRACY_DATA_SOURCE=/data/local/Files/Bronze/DDD \
    RFAM_DATA_SOURCE=/data/local/Files/Bronze/RFAM \
    DLT_PIPELINES_DIR=/data/dlt_pipelines \
    DUCKDB_DATABASE_LOCATION=/data/duckdb/danish_democracy_data.duckdb \
    DUCKDB_DATABASE=danish_democracy_data \
    DBT_PROJECT_DIRECTORY=/app/dbt \
    DBT_MODELS_DIRECTORY=/app/dbt/models \
    DBT_LOGS_DIRECTORY=/data/dbt_logs \
    DAGSTER_HOME=/data/dagster \
    DANISH_DEMOCRACY_BASE_URL=https://oda.ft.dk/api \
    HOME=/home/app

# Drop to non-root for all runtime commands.
USER app

# Entrypoint wrapper: ensures DuckDB Azure secret exists on every container start
ENTRYPOINT ["docker/docker-entrypoint.sh"]
CMD ["python", "-m"]
