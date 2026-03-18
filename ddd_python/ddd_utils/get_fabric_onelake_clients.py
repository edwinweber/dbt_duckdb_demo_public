from ddd_python.ddd_utils import get_variables_from_env
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    DataLakeDirectoryClient,
    DataLakeFileClient,
    FileSystemClient,
)

# Lazily initialised credential — created on first use so that importing this
# module does not fail when environment variables are absent (e.g. during
# code generation or testing).
_credential: ClientSecretCredential | None = None
_service_client: DataLakeServiceClient | None = None


def _get_credential() -> ClientSecretCredential:
    """Return the shared ``ClientSecretCredential``, creating it on first call.

    Although underscore-prefixed, this function is intentionally used by
    ``fabric_capacity_pause_resume`` to avoid creating a second credential
    instance.  A public alias ``get_credential`` is provided below.
    """
    global _credential
    if _credential is None:
        _credential = ClientSecretCredential(
            tenant_id=get_variables_from_env.AZURE_TENANT_ID,
            client_id=get_variables_from_env.AZURE_CLIENT_ID,
            client_secret=get_variables_from_env.AZURE_CLIENT_SECRET,
        )
    return _credential


def _get_service_client() -> DataLakeServiceClient:
    """Return the shared ``DataLakeServiceClient``, creating it on first call."""
    global _service_client
    if _service_client is None:
        account_url = f"https://{get_variables_from_env.FABRIC_ONELAKE_STORAGE_ACCOUNT}.dfs.fabric.microsoft.com"
        _service_client = DataLakeServiceClient(account_url, credential=_get_credential())
    return _service_client


# Public alias so other modules don't need to reach for underscore-prefixed internals.
get_credential = _get_credential


def get_fabric_token() -> str:
    """Get an OAuth2 token for Azure Storage using the service principal."""
    token = _get_credential().get_token("https://storage.azure.com/.default").token
    return token


def get_fabric_file_system_client(file_system_name: str) -> FileSystemClient:
    """Return a ``FileSystemClient`` for the given OneLake file system (workspace)."""
    return _get_service_client().get_file_system_client(file_system_name)


def get_fabric_directory_client(
    file_system_client: FileSystemClient, file_directory: str,
) -> DataLakeDirectoryClient:
    # Create a directory client connected to OneLake
    directory_client = file_system_client.get_directory_client(file_directory)
    directory_client.create_directory()  # Safe to call; it won't overwrite

    return directory_client


def get_fabric_file_client(
    directory_client: DataLakeDirectoryClient, file_name: str,
) -> DataLakeFileClient:
    # Create a file client connected to OneLake
    file_client = directory_client.get_file_client(file_name)

    return file_client

def get_fabric_file_client_default_workspace(
    destination_directory_path: str, destination_file_name: str
) -> DataLakeFileClient:

    file_system_client = get_fabric_file_system_client(get_variables_from_env.FABRIC_WORKSPACE)
    directory_client = get_fabric_directory_client(file_system_client, destination_directory_path)
    file_client = get_fabric_file_client(directory_client, destination_file_name)

    return file_client
