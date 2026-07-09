import os

from dotenv import load_dotenv

# Read secrets and variables from .env file
load_dotenv()


def _require(name: str) -> str:
    """Return the value of environment variable *name*, or raise if missing."""
    value = os.getenv(name)
    if not value:
        raise OSError(f"Required environment variable {name!r} is not set")
    return value


def _int_env(name: str, default: int) -> int:
    """Return the integer value of environment variable *name*.

    Falls back to *default* when the variable is absent.  Raises
    ``EnvironmentError`` (rather than a bare ``ValueError``) with a clear
    message when the variable is set but cannot be parsed as an integer, so
    misconfiguration is surfaced at import time with a useful diagnostic.
    """
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        raise OSError(f"Environment variable {name!r} must be an integer; got {raw!r}") from None


# Map of attribute name → env var name for required variables that are
# resolved lazily (on first access) rather than at import time.
_LAZY_REQUIRED: dict[str, str] = {
    # Fabric / OneLake
    "FABRIC_ONELAKE_STORAGE_ACCOUNT": "FABRIC_ONELAKE_STORAGE_ACCOUNT",
    "FABRIC_WORKSPACE": "FABRIC_WORKSPACE",
    "FABRIC_ONELAKE_FOLDER_BRONZE": "FABRIC_ONELAKE_FOLDER_BRONZE",
    "FABRIC_ONELAKE_FOLDER_SILVER": "FABRIC_ONELAKE_FOLDER_SILVER",
    "FABRIC_ONELAKE_FOLDER_GOLD": "FABRIC_ONELAKE_FOLDER_GOLD",
    # Azure AD service principal
    "AZURE_TENANT_ID": "AZURE_TENANT_ID",
    "AZURE_CLIENT_ID": "AZURE_CLIENT_ID",
    "AZURE_CLIENT_SECRET": "AZURE_CLIENT_SECRET",
}


def __getattr__(name: str) -> str:
    """PEP 562 module-level __getattr__: resolve required env vars on first access.

    Called by Python only when *name* is not found in the module's __dict__,
    so optional variables that are set as plain module globals at import time
    are returned immediately without going through this function.
    """
    env_var = _LAZY_REQUIRED.get(name)
    if env_var is not None:
        return _require(env_var)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


# ── Fabric / OneLake (optional — eager) ──────────────────────────────────
FABRIC_CAPACITY_NAME = os.getenv("FABRIC_CAPACITY_NAME")
FABRIC_ONELAKE_MOUNT = os.getenv("FABRIC_ONELAKE_MOUNT")

# ── DuckDB / dbt (optional — eager) ─────────────────────────────────────
DUCKDB_DATABASE_LOCATION = os.getenv(
    "DUCKDB_DATABASE_LOCATION", "duckdb/danish_democracy_data.duckdb"
)
DUCKDB_DATABASE = os.getenv("DUCKDB_DATABASE")
DBT_PROJECT_DIRECTORY = os.getenv("DBT_PROJECT_DIRECTORY")
DBT_MODELS_DIRECTORY = os.getenv("DBT_MODELS_DIRECTORY")
DBT_LOGS_DIRECTORY = os.getenv("DBT_LOGS_DIRECTORY")

# ── dlt (optional — eager) ───────────────────────────────────────────────
DLT_PIPELINES_DIR = os.getenv("DLT_PIPELINES_DIR", "dlt/pipelines_dir")
DLT_PIPELINE_RUN_LOG_FILE = os.getenv("DLT_PIPELINE_RUN_LOG_FILE")

# ── Azure AD (optional — eager) ─────────────────────────────────────────
AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID")
AZURE_RESOURCE_GROUP = os.getenv("AZURE_RESOURCE_GROUP")

# ── Storage target (optional — eager) ───────────────────────────────────
# Controls the Delta Lake export destination (Silver/Gold layer output).
#   local   — write to LOCAL_STORAGE_PATH on disk (default; no credentials needed)
#   s3      — write to S3-compatible storage (MinIO or AWS S3); requires S3_BUCKET_DELTA
#   onelake — write to Microsoft Fabric OneLake via abfss://; requires Azure SP
# Set LOCAL_STORAGE_PATH to override the default local base path.
_VALID_STORAGE_TARGETS = frozenset({"local", "onelake", "s3"})
_storage_target = os.getenv("STORAGE_TARGET", "local")
if _storage_target not in _VALID_STORAGE_TARGETS:
    raise OSError(
        f"Invalid STORAGE_TARGET={_storage_target!r}. "
        f"Must be one of: {sorted(_VALID_STORAGE_TARGETS)}"
    )
STORAGE_TARGET = _storage_target
LOCAL_STORAGE_PATH = os.getenv("LOCAL_STORAGE_PATH", "data")
DLT_PIPELINE_RUN_LOG_DIR = os.getenv("DLT_PIPELINE_RUN_LOG_DIR", f"{LOCAL_STORAGE_PATH}/logs")

# ── Silver storage format (optional — eager) ─────────────────────────────
# "duckdb"   — silver tables are native DuckDB tables (default)
# "ducklake" — silver tables are DuckLake-managed Parquet files stored locally;
#              requires DUCKLAKE_CATALOG_LOCATION (e.g. /data/ducklake/ducklake_catalog.db).
#              Selects the "local_ducklake" dbt target regardless of STORAGE_TARGET.
#              When RAW_STORAGE_TARGET=s3, DUCKLAKE_DATA_PATH is auto-derived
#              from S3_BUCKET_DUCKLAKE + S3_PREFIX_DUCKLAKE (no manual override needed).
_VALID_SILVER_STORAGE_FORMATS = frozenset({"duckdb", "ducklake"})
_silver_storage_format = os.getenv("SILVER_STORAGE_FORMAT", "duckdb")
if _silver_storage_format not in _VALID_SILVER_STORAGE_FORMATS:
    raise OSError(
        f"Invalid SILVER_STORAGE_FORMAT={_silver_storage_format!r}. "
        f"Must be one of: {sorted(_VALID_SILVER_STORAGE_FORMATS)}"
    )
SILVER_STORAGE_FORMAT = _silver_storage_format
DUCKLAKE_CATALOG_LOCATION = os.getenv("DUCKLAKE_CATALOG_LOCATION")
DUCKLAKE_DATA_PATH = os.getenv("DUCKLAKE_DATA_PATH")

# ── Raw storage target (optional — eager) ────────────────────────────────
# Controls where internal data lives: Bronze raw files written by dlt and
# DuckLake Silver Parquet files.  Independent of STORAGE_TARGET (Delta Lake
# export destination).  These files never go to OneLake.
#   local — local disk under LOCAL_STORAGE_PATH (default)
#   s3    — S3-compatible storage (MinIO locally, AWS S3 in production)
RAW_STORAGE_TARGET = os.getenv("RAW_STORAGE_TARGET", "local")
if RAW_STORAGE_TARGET not in {"local", "s3"}:
    raise OSError(f"RAW_STORAGE_TARGET must be 'local' or 's3', got: {RAW_STORAGE_TARGET!r}")

# ── S3 credentials (set when RAW_STORAGE_TARGET=s3 or STORAGE_TARGET=s3) ─
# Shared S3 credentials (endpoint, key, secret, region, ssl, url-style) are
# set whenever either switch is s3.  Bucket-specific variables are set only
# for the relevant switch.  Accessing any S3_* attribute when neither switch
# is "s3" raises AttributeError, mirroring how OneLake credentials are only
# resolved when STORAGE_TARGET=onelake.
# MinIO (local dev): set S3_ENDPOINT, S3_USE_SSL=false, S3_URL_STYLE=path.
# AWS S3 (production): omit S3_ENDPOINT (empty = AWS default), S3_USE_SSL=true, S3_URL_STYLE=vhost.
_any_s3 = RAW_STORAGE_TARGET == "s3" or STORAGE_TARGET == "s3"
if _any_s3:
    _s3_cred_missing = [v for v in ("S3_ACCESS_KEY_ID", "S3_SECRET_ACCESS_KEY") if not os.getenv(v)]
    if _s3_cred_missing:
        raise OSError(
            f"S3 storage (RAW_STORAGE_TARGET=s3 or STORAGE_TARGET=s3) requires: "
            f"{', '.join(_s3_cred_missing)}"
        )
    S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")
    S3_ACCESS_KEY_ID = os.getenv("S3_ACCESS_KEY_ID")
    S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
    S3_REGION = os.getenv("S3_REGION", "us-east-1")
    S3_USE_SSL = os.getenv("S3_USE_SSL", "false")
    S3_URL_STYLE = os.getenv("S3_URL_STYLE", "path")

if RAW_STORAGE_TARGET == "s3":
    if not os.getenv("S3_BUCKET_BRONZE"):
        raise OSError("RAW_STORAGE_TARGET=s3 requires S3_BUCKET_BRONZE")
    S3_BUCKET_BRONZE = os.getenv("S3_BUCKET_BRONZE")
    S3_PREFIX_BRONZE = os.getenv("S3_PREFIX_BRONZE", "")
    # When DuckLake is also active, auto-derive DUCKLAKE_DATA_PATH from the
    # S3 bucket/prefix vars.  Any manually-set DUCKLAKE_DATA_PATH is overridden
    # — the S3 bucket/prefix vars are the source of truth in s3 mode.
    if SILVER_STORAGE_FORMAT == "ducklake":
        S3_BUCKET_DUCKLAKE = os.getenv("S3_BUCKET_DUCKLAKE")
        if not S3_BUCKET_DUCKLAKE:
            raise OSError(
                "RAW_STORAGE_TARGET=s3 with SILVER_STORAGE_FORMAT=ducklake "
                "requires S3_BUCKET_DUCKLAKE"
            )
        S3_PREFIX_DUCKLAKE = os.getenv("S3_PREFIX_DUCKLAKE", "")
        _ducklake_prefix = S3_PREFIX_DUCKLAKE.strip("/") + "/" if S3_PREFIX_DUCKLAKE else ""
        DUCKLAKE_DATA_PATH = f"s3://{S3_BUCKET_DUCKLAKE}/{_ducklake_prefix}"

if STORAGE_TARGET == "s3":
    if not os.getenv("S3_BUCKET_DELTA"):
        raise OSError("STORAGE_TARGET=s3 requires S3_BUCKET_DELTA")
    S3_BUCKET_DELTA = os.getenv("S3_BUCKET_DELTA")
    S3_PREFIX_DELTA = os.getenv("S3_PREFIX_DELTA", "")

# ── Danish Democracy data retrieval (optional — eager) ───────────────────
DANISH_DEMOCRACY_BASE_URL = os.getenv("DANISH_DEMOCRACY_BASE_URL")
DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD = _int_env("DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD", 31)
DANISH_DEMOCRACY_TABLES_SILVER = os.getenv("DANISH_DEMOCRACY_TABLES_SILVER")
DANISH_DEMOCRACY_TABLES_GOLD = os.getenv("DANISH_DEMOCRACY_TABLES_GOLD")

# ── Danish Democracy data source (optional — eager) ──────────────────────
DANISH_DEMOCRACY_DATA_SOURCE = os.getenv("DANISH_DEMOCRACY_DATA_SOURCE")

# ── Rfam data retrieval (optional — eager) ───────────────────────────────
RFAM_CONNECTION_STRING = os.getenv(
    "RFAM_CONNECTION_STRING", "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam"
)
RFAM_DATA_SOURCE = os.getenv("RFAM_DATA_SOURCE")
RFAM_DEFAULT_DAYS_TO_LOAD = _int_env("RFAM_DEFAULT_DAYS_TO_LOAD", 365)
