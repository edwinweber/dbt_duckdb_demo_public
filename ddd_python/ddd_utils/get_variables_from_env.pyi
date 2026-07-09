# Type stub for the _LazyEnv module wrapper in get_variables_from_env.py.
# Required vars resolve lazily through __getattr__; mypy cannot see them without this stub.

# ── Required (lazy — raise OSError when missing) ─────────────────────────────
FABRIC_ONELAKE_STORAGE_ACCOUNT: str
FABRIC_WORKSPACE: str
FABRIC_ONELAKE_FOLDER_BRONZE: str
FABRIC_ONELAKE_FOLDER_SILVER: str
FABRIC_ONELAKE_FOLDER_GOLD: str
AZURE_TENANT_ID: str
AZURE_CLIENT_ID: str
AZURE_CLIENT_SECRET: str

# ── Optional / eager (None when not set) ─────────────────────────────────────
FABRIC_CAPACITY_NAME: str | None
FABRIC_ONELAKE_MOUNT: str | None

DUCKDB_DATABASE_LOCATION: str  # has default "duckdb/danish_democracy_data.duckdb"
DUCKDB_DATABASE: str | None
DBT_PROJECT_DIRECTORY: str | None
DBT_MODELS_DIRECTORY: str | None
DBT_LOGS_DIRECTORY: str | None

DLT_PIPELINES_DIR: str  # has default "dlt/pipelines_dir"
DLT_PIPELINE_RUN_LOG_FILE: str | None

AZURE_SUBSCRIPTION_ID: str | None
AZURE_RESOURCE_GROUP: str | None

STORAGE_TARGET: str  # validated; "local" | "s3" | "onelake"
LOCAL_STORAGE_PATH: str  # has default "data"
DLT_PIPELINE_RUN_LOG_DIR: str  # has default LOCAL_STORAGE_PATH/logs
SILVER_STORAGE_FORMAT: str  # validated; "duckdb" | "ducklake"
DUCKLAKE_CATALOG_LOCATION: str | None
DUCKLAKE_DATA_PATH: str | None

RAW_STORAGE_TARGET: str  # validated; "local" | "s3"

# ── S3 credentials (set when RAW_STORAGE_TARGET=s3 or STORAGE_TARGET=s3) ─────
# Shared credentials are set whenever either switch is "s3".
# Bucket-specific vars are set only for the relevant switch.
# Accessing any S3_* attr when neither switch is "s3" raises AttributeError.
S3_ENDPOINT: str  # empty string for AWS; non-empty for MinIO
S3_ACCESS_KEY_ID: str
S3_SECRET_ACCESS_KEY: str
S3_REGION: str  # default "us-east-1"
S3_USE_SSL: str  # "true" | "false"
S3_URL_STYLE: str  # "vhost" | "path"
S3_BUCKET_BRONZE: str  # set when RAW_STORAGE_TARGET=s3
S3_PREFIX_BRONZE: str  # default ""
S3_BUCKET_DUCKLAKE: str  # set when RAW_STORAGE_TARGET=s3 + SILVER_STORAGE_FORMAT=ducklake
S3_PREFIX_DUCKLAKE: str  # default ""
S3_BUCKET_DELTA: str  # set when STORAGE_TARGET=s3
S3_PREFIX_DELTA: str  # default ""

DANISH_DEMOCRACY_BASE_URL: str | None
DANISH_DEMOCRACY_DEFAULT_DAYS_TO_LOAD: int
DANISH_DEMOCRACY_DATA_SOURCE: str | None
DANISH_DEMOCRACY_TABLES_SILVER: str | None
DANISH_DEMOCRACY_TABLES_GOLD: str | None

RFAM_CONNECTION_STRING: str  # has default
RFAM_DATA_SOURCE: str | None
RFAM_DEFAULT_DAYS_TO_LOAD: int

def _require(name: str) -> str: ...
