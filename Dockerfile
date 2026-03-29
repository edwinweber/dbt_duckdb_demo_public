FROM python:3.12

LABEL maintainer="Edwin" \
      version="1.0.0" \
      description="Danish Democracy Data — dbt/DuckDB/Dagster pipeline"

# System dependencies for DuckDB, Azure SDK, and dbt
RUN apt-get update && \
    apt-get install -y --no-install-recommends git curl unzip ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Install DuckDB CLI v1.5.1
RUN curl -fsSL https://github.com/duckdb/duckdb/releases/download/v1.5.1/duckdb_cli-linux-amd64.zip -o /tmp/duckdb.zip && \
    unzip /tmp/duckdb.zip -d /usr/local/bin && \
    chmod +x /usr/local/bin/duckdb && \
    rm /tmp/duckdb.zip

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

# Create volume mount directories
RUN mkdir -p /data/dlt_pipelines /data/duckdb /data/dbt_logs /data/dagster /data/local

# Create non-root user for runtime security
RUN groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid appuser --create-home appuser && \
    chown -R appuser:appuser /data /app

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
    DANISH_DEMOCRACY_BASE_URL=https://oda.ft.dk/api

# Auto-configure every DuckDB CLI session with CA certs and Azure transport
RUN printf '%s\n' \
    "SET ca_cert_file='/etc/ssl/certs/ca-certificates.crt';" \
    "SET azure_transport_option_type='curl';" \
    > /home/appuser/.duckdbrc && \
    chown appuser:appuser /home/appuser/.duckdbrc

# Entrypoint wrapper: ensures DuckDB Azure secret exists on every container start
RUN chmod +x docker-entrypoint.sh

USER appuser

ENTRYPOINT ["./docker-entrypoint.sh"]
CMD ["python", "-m"]
