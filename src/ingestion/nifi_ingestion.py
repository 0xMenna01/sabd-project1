import os
from api.nifi import NifiApi


def execute() -> None:
    """Executes the ingestion, by triggering the flow of a nifi processor group.
    It acquires the data from a cloud bucket and ingests it into HDFS, after a pre-processing step."""

    # Get nifi credentials from environment variables
    username = os.getenv('NIFI_USERNAME', 'admin')
    password = os.getenv('NIFI_PASSWORD')

    if password is None:
        raise Exception('NIFI_PASSWORD is required')
    # Initialize the nifi api, login and schedule the processor group
    NifiApi.init_api().login(username=username, password=password).schedule_hdfs_ingestion()
