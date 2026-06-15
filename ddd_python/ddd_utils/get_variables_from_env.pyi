# Type stub for the _LazyEnv module wrapper in get_variables_from_env.py.
# Required vars resolve lazily through __getattr__; mypy cannot see them without this stub.

# ── Required (lazy — raise OSError when missing) ─────────────────────────────
FABRIC_ONELAKE_STORAGE_ACCOUNT: str
FABRIC_WORKSPACE: str
FABRIC_ONELAKE_FOLDER_BRONZE: str
FABRIC_ONELAKE_FOLDER_SILVER: str
FABRIC_ONELAKE_FOLDER_GOLD: str
DLT_PIPELINE_RUN_LOG_DIR: str
AZURE_TENANT_ID: str
AZURE_CLIENT_ID: str
AZURE_CLIENT_SECRET: str

# ── Optional / eager (None when not set) ─────────────────────────────────────
FABRIC_CAPACITY_NAME: str | None
FABRIC_ONELAKE_MOUNT: str | None

DUCKDB_DATABASE_LOCATION: str | None
DUCKDB_DATABASE: str | None
DBT_PROJECT_DIRECTORY: str | None
DBT_MODELS_DIRECTORY: str | None
DBT_LOGS_DIRECTORY: str | None
DBT_LOGS_DIRECTORY_FABRIC: str | None

DLT_PIPELINES_DIR: str  # has default "dlt/pipelines_dir"
DLT_PIPELINES_LOG_DIR: str | None
DLT_PIPELINE_RUN_LOG_FILE: str | None

AZURE_SUBSCRIPTION_ID: str | None
AZURE_RESOURCE_GROUP: str | None

STORAGE_TARGET: str  # validated; "local" | "onelake"
LOCAL_STORAGE_PATH: str  # has default "data"
SILVER_STORAGE_FORMAT: str  # validated; "duckdb" | "ducklake"
DUCKLAKE_CATALOG_LOCATION: str | None
DUCKLAKE_DATA_PATH: str | None

DANISH_DEMOCRACY_BASE_URL: str | None
DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD: int
DANISH_DEMOCRACY_TABLES_SILVER: str | None
DANISH_DEMOCRACY_TABLES_GOLD: str | None

RFAM_CONNECTION_STRING: str  # has default
RFAM_DATA_SOURCE: str | None
RFAM_DEFAULT_DAYS_TO_LOAD: int

def _require(name: str) -> str: ...
