"""Pause or resume a Microsoft Fabric capacity via the Azure Management API.

Usage::

    python -m ddd_python.ddd_utils.fabric_capacity_pause_resume
"""

import logging
import time

import requests

from ddd_python.ddd_utils import get_variables_from_env, get_fabric_onelake_clients

logger = logging.getLogger(__name__)

AZURE_API_VERSION = "2022-07-01-preview"
AZURE_RESOURCE = "https://management.azure.com"


def _capacity_url(suffix: str = "") -> str:
    """Build the ARM URL for the Fabric capacity resource."""
    return (
        f"{AZURE_RESOURCE}"
        f"/subscriptions/{get_variables_from_env.AZURE_SUBSCRIPTION_ID}"
        f"/resourceGroups/{get_variables_from_env.AZURE_RESOURCE_GROUP}"
        f"/providers/Microsoft.Fabric/capacities/{get_variables_from_env.FABRIC_CAPACITY_NAME}"
        f"{suffix}?api-version={AZURE_API_VERSION}"
    )


def get_access_token() -> str:
    """Get an OAuth2 token using the shared service principal credential."""
    return get_fabric_onelake_clients.get_credential().get_token(f"{AZURE_RESOURCE}/.default").token


def get_capacity_status(access_token: str) -> str | None:
    """Retrieve the current status of the Fabric capacity.

    Returns:
        Current capacity status string, or ``None`` on failure.
    """
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    response = requests.get(_capacity_url(), headers=headers)

    if response.status_code == 200:
        return response.json().get("properties", {}).get("state")
    logger.error("Failed to retrieve capacity status: %d — %s", response.status_code, response.text)
    return None


def wait_for_status(target_status: str, access_token: str, poll_interval: int = 10, timeout: int = 300) -> None:
    """Poll the Fabric capacity status until it reaches *target_status* or timeout.

    Raises:
        RuntimeError: If *target_status* is not reached within *timeout* seconds.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        current_status = get_capacity_status(access_token)
        if current_status is None:
            logger.warning("Error checking status, retrying...")
        elif current_status == target_status:
            logger.info("Capacity is now %s.", target_status)
            return
        else:
            logger.info("Current status: %s. Waiting for %s...", current_status, target_status)
        time.sleep(poll_interval)

    raise RuntimeError(f"Capacity failed to reach {target_status} within {timeout}s.")


def change_capacity_state(action: str) -> None:
    """Change the state of the Fabric capacity (``'pause'`` or ``'resume'``)."""
    access_token = get_access_token()
    current_status = get_capacity_status(access_token)

    if action == "pause":
        target_status = "Paused"
        url = _capacity_url("/suspend")
    elif action == "resume":
        target_status = "Active"
        url = _capacity_url("/resume")
    else:
        raise ValueError(f"Invalid action {action!r}. Use 'pause' or 'resume'.")

    if current_status == target_status:
        logger.info("Capacity is already %s. No action needed.", target_status)
        return

    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    response = requests.post(url, headers=headers)

    if response.status_code == 202:
        logger.info("Starting to %s capacity...", action)
        wait_for_status(target_status=target_status, access_token=access_token)
    else:
        logger.error("Failed to %s capacity: %d — %s", action, response.status_code, response.text)
        raise RuntimeError(
            f"Failed to {action} capacity: HTTP {response.status_code} — {response.text}"
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    action = input("Enter 'pause' to pause capacity or 'resume' to resume capacity: ").strip().lower()
    change_capacity_state(action)
