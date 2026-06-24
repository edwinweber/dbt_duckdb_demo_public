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
    # dlt
    "DLT_PIPELINE_RUN_LOG_DIR": "DLT_PIPELINE_RUN_LOG_DIR",
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
DUCKDB_DATABASE_LOCATION = os.getenv("DUCKDB_DATABASE_LOCATION")
DUCKDB_DATABASE = os.getenv("DUCKDB_DATABASE")
DBT_PROJECT_DIRECTORY = os.getenv("DBT_PROJECT_DIRECTORY")
DBT_MODELS_DIRECTORY = os.getenv("DBT_MODELS_DIRECTORY")
DBT_LOGS_DIRECTORY = os.getenv("DBT_LOGS_DIRECTORY")
DBT_LOGS_DIRECTORY_FABRIC = os.getenv("DBT_LOGS_DIRECTORY_FABRIC")

# ── dlt (optional — eager) ───────────────────────────────────────────────
DLT_PIPELINES_DIR = os.getenv("DLT_PIPELINES_DIR", "dlt/pipelines_dir")
DLT_PIPELINES_LOG_DIR = os.getenv("DLT_PIPELINES_LOG_DIR")
DLT_PIPELINE_RUN_LOG_FILE = os.getenv("DLT_PIPELINE_RUN_LOG_FILE")

# ── Azure AD (optional — eager) ─────────────────────────────────────────
AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID")
AZURE_RESOURCE_GROUP = os.getenv("AZURE_RESOURCE_GROUP")

# ── Storage target (optional — eager) ───────────────────────────────────
# Set STORAGE_TARGET=local to write to a Docker volume instead of Fabric OneLake.
# Set LOCAL_STORAGE_PATH to override the default local base path.
_VALID_STORAGE_TARGETS = frozenset({"local", "onelake"})
_storage_target = os.getenv("STORAGE_TARGET", "local")
if _storage_target not in _VALID_STORAGE_TARGETS:
    raise OSError(
        f"Invalid STORAGE_TARGET={_storage_target!r}. "
        f"Must be one of: {sorted(_VALID_STORAGE_TARGETS)}"
    )
STORAGE_TARGET = _storage_target
LOCAL_STORAGE_PATH = os.getenv("LOCAL_STORAGE_PATH", "data")

# ── Silver storage format (optional — eager) ─────────────────────────────
# "duckdb"   — silver tables are native DuckDB tables (default)
# "ducklake" — silver tables are DuckLake-managed Parquet files stored locally;
#              requires DUCKLAKE_CATALOG_LOCATION (e.g. /data/ducklake/ducklake_catalog.db).
#              Selects the "local_ducklake" dbt target regardless of STORAGE_TARGET.
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
