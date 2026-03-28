import os
import types
import sys
from dotenv import load_dotenv

# Read secrets and variables from .env file
load_dotenv()


def _require(name: str) -> str:
    """Return the value of environment variable *name*, or raise if missing."""
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Required environment variable {name!r} is not set")
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
        raise EnvironmentError(
            f"Environment variable {name!r} must be an integer; got {raw!r}"
        ) from None


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


class _LazyEnv(types.ModuleType):
    """Module wrapper that defers ``_require()`` calls until first attribute access.

    Optional variables (``os.getenv`` with no required assertion) are set as
    plain instance attributes at module-load time — they never raise.  Required
    variables listed in ``_LAZY_REQUIRED`` are resolved via ``__getattr__``
    only when actually accessed, so importing this module for code generation
    or testing does not fail when Azure credentials are absent.

    Because required vars go through ``__getattr__`` (not ``@property``),
    ``unittest.mock.patch`` can freely set and delete instance attributes for
    testing without hitting property descriptor issues.
    """

    def __getattr__(self, name: str) -> str:
        env_var = _LAZY_REQUIRED.get(name)
        if env_var is not None:
            return _require(env_var)
        raise AttributeError(f"module {self.__name__!r} has no attribute {name!r}")


# Instantiate the lazy module and copy over the eager (optional) attributes.
_mod = _LazyEnv(__name__)
_mod.__file__ = __file__
_mod.__package__ = __package__
_mod.__path__ = []  # type: ignore[attr-defined]
_mod.__spec__ = __spec__
# Re-export the helper so tests can import it directly.
_mod._require = _require  # type: ignore[attr-defined]

# ── Fabric / OneLake (optional — eager) ──────────────────────────────────
_mod.FABRIC_CAPACITY_NAME = os.getenv("FABRIC_CAPACITY_NAME")  # type: ignore[attr-defined]
_mod.FABRIC_ONELAKE_MOUNT = os.getenv("FABRIC_ONELAKE_MOUNT")  # type: ignore[attr-defined]

# ── DuckDB / dbt (optional — eager) ─────────────────────────────────────
_mod.DUCKDB_DATABASE_LOCATION = os.getenv("DUCKDB_DATABASE_LOCATION")  # type: ignore[attr-defined]
_mod.DUCKDB_DATABASE = os.getenv("DUCKDB_DATABASE")  # type: ignore[attr-defined]
_mod.DBT_PROJECT_DIRECTORY = os.getenv("DBT_PROJECT_DIRECTORY")  # type: ignore[attr-defined]
_mod.DBT_MODELS_DIRECTORY = os.getenv("DBT_MODELS_DIRECTORY")  # type: ignore[attr-defined]
_mod.DBT_LOGS_DIRECTORY = os.getenv("DBT_LOGS_DIRECTORY")  # type: ignore[attr-defined]
_mod.DBT_LOGS_DIRECTORY_FABRIC = os.getenv("DBT_LOGS_DIRECTORY_FABRIC")  # type: ignore[attr-defined]

# ── dlt (optional — eager) ───────────────────────────────────────────────
_mod.DLT_PIPELINES_DIR = os.getenv("DLT_PIPELINES_DIR", "dlt/pipelines_dir")  # type: ignore[attr-defined]
_mod.DLT_PIPELINES_LOG_DIR = os.getenv("DLT_PIPELINES_LOG_DIR")  # type: ignore[attr-defined]
_mod.DLT_PIPELINE_RUN_LOG_FILE = os.getenv("DLT_PIPELINE_RUN_LOG_FILE")  # type: ignore[attr-defined]

# ── Azure AD (optional — eager) ─────────────────────────────────────────
_mod.AZURE_SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID")  # type: ignore[attr-defined]
_mod.AZURE_RESOURCE_GROUP = os.getenv("AZURE_RESOURCE_GROUP")  # type: ignore[attr-defined]

# ── Storage target (optional — eager) ───────────────────────────────────
# Set STORAGE_TARGET=local to write to a Docker volume instead of Fabric OneLake.
# Set LOCAL_STORAGE_PATH to override the default local base path.
_VALID_STORAGE_TARGETS = frozenset({"local", "onelake"})
_storage_target = os.getenv("STORAGE_TARGET", "local")
if _storage_target not in _VALID_STORAGE_TARGETS:
    raise EnvironmentError(
        f"Invalid STORAGE_TARGET={_storage_target!r}. "
        f"Must be one of: {sorted(_VALID_STORAGE_TARGETS)}"
    )
_mod.STORAGE_TARGET = _storage_target  # type: ignore[attr-defined]
_mod.LOCAL_STORAGE_PATH = os.getenv("LOCAL_STORAGE_PATH", "data")  # type: ignore[attr-defined]

# ── Danish Democracy data retrieval (optional — eager) ───────────────────
_mod.DANISH_DEMOCRACY_BASE_URL = os.getenv("DANISH_DEMOCRACY_BASE_URL")  # type: ignore[attr-defined]
_mod.DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD = _int_env("DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD", 31)  # type: ignore[attr-defined]
_mod.DANISH_DEMOCRACY_TABLES_SILVER = os.getenv("DANISH_DEMOCRACY_TABLES_SILVER")  # type: ignore[attr-defined]
_mod.DANISH_DEMOCRACY_TABLES_GOLD = os.getenv("DANISH_DEMOCRACY_TABLES_GOLD")  # type: ignore[attr-defined]

# ── Rfam data retrieval (optional — eager) ───────────────────────────────
_mod.RFAM_CONNECTION_STRING = os.getenv("RFAM_CONNECTION_STRING", "mysql+pymysql://rfamro@mysql-rfam-public.ebi.ac.uk:4497/Rfam")  # type: ignore[attr-defined]
_mod.RFAM_DATA_SOURCE = os.getenv("RFAM_DATA_SOURCE")  # type: ignore[attr-defined]
_mod.RFAM_DEFAULT_DAYS_TO_LOAD = _int_env("RFAM_DEFAULT_DAYS_TO_LOAD", 365)  # type: ignore[attr-defined]

# Replace this module in sys.modules so that all existing
# ``from ddd_python.ddd_utils import get_variables_from_env`` and
# ``get_variables_from_env.SOME_VAR`` accesses go through the lazy wrapper.
sys.modules[__name__] = _mod
