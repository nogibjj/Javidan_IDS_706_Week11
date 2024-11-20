"""
Test Databricks Functionality
"""

import requests
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()
SERVER_HOSTNAME = os.getenv("SERVER_HOSTNAME")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
JOB_ID = 660776900051823
BASE_URL = f"https://{SERVER_HOSTNAME}/api/2.1"

# Validate environment variables
if not SERVER_HOSTNAME or not ACCESS_TOKEN:
    raise ValueError("SERVER_HOSTNAME and ACCESS_TOKEN must be set in the .env file.")


def check_pipeline_existence(job_id: str, headers: dict) -> bool:
    """
    Check if a file path exists in Databricks DBFS and validate authentication.

    Args:
        path (str): The file path to check.
        headers (dict): Headers including the authorization token.

    Returns:
        bool: True if the file path exists and authentication is valid, False otherwise.
    """
    try:
        response = requests.get(
            f"{BASE_URL}/jobs/get", headers=headers, params={"job_id": job_id}
        )
        response.raise_for_status()
        # Check if the response contains a valid file path
        return "job_id" in response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"Request error occurred: {req_err}")
    return False


def test_databricks():
    """
    Test if the Databricks file store path exists and is accessible.
    """
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}
    path_exists = check_pipeline_existence(JOB_ID, headers)
    assert path_exists, f"Job Id- {JOB_ID} not found"
    print(f"Test successful: Job Id -  '{JOB_ID}' exists and is accessible.")


if __name__ == "__main__":
    headers = {"Authorization": f"Bearer {ACCESS_TOKEN}"}

    print(check_pipeline_existence(JOB_ID, headers))
