from __future__ import annotations

import io
import os
from utils.config.factory import ConfigFactory
from utils.files import create_dir, load_bytes_from
from utils.crypto.aes_gcm import AES256GCM, bytes_to_dataframe
from utils.logging.factory import LoggerFactory
from b2sdk.v2 import B2Api
from b2sdk.v2 import InMemoryAccountInfo


APP_KEY_ID = os.getenv('B2_APP_KEY_ID')
# The defaults value are just for typing hints
APP_KEY = os.getenv('B2_APP_KEY', 'invalid_key')
DECRYPTION_DATA_KEY = os.getenv('DECRYPTION_DATA_KEY', '0xinvalid')


class MissingEnvironmentVariableError(Exception):
    """Custom exception raised when required environment variables are missing."""
    pass


class B2ApiBuilder:
    """Builder class for the B2 API. It authorizes the account using the provided app key ID and app key."""

    def __init__(self, app_key_id: str, app_key: str, decryption_key: str) -> None:
        # Needed to authorize the account
        self._app_key_id = app_key_id
        self._app_key = app_key
        # Needed to decrypt the data
        self._decryption_key = decryption_key

    @staticmethod
    def from_default_keys() -> B2ApiBuilder:
        if APP_KEY_ID is None or APP_KEY_ID is None or DECRYPTION_DATA_KEY is None:
            raise MissingEnvironmentVariableError(
                "Required environment variables 'B2_APP_KEY_ID', 'B2_APP_KEY', or 'DECRYPTION_DATA_KEY' are missing."
            )
        return B2ApiBuilder(APP_KEY_ID, APP_KEY, DECRYPTION_DATA_KEY)

    def build(self) -> B2ApiForDataset:
        info = InMemoryAccountInfo()
        b2_api = B2Api(info)
        b2_api.authorize_account(
            "production", self._app_key_id, self._app_key)
        aes_key = bytes.fromhex(self._decryption_key)
        return B2ApiForDataset(b2_api, decryption_key=aes_key)


class B2ApiForDataset:
    _instance = None

    def __init__(self, api: B2Api, decryption_key: bytes) -> None:
        self._api = api
        self._decryption_key = decryption_key

    @staticmethod
    def get() -> B2ApiForDataset:
        if B2ApiForDataset._instance is None:
            LoggerFactory.b2_logger().b2_api_loaded()
            B2ApiForDataset._instance = B2ApiBuilder.from_default_keys().build()
        return B2ApiForDataset._instance

    @property
    def api(self) -> B2Api:
        return self._api

    def dataset_as_dataframe(self):
        logger = LoggerFactory.b2_logger()
        config = ConfigFactory.config()

        # Load the bucket
        bucket = self._api.get_bucket_by_name(
            bucket_name=config.b2_bucket_name)
        logger.b2_bucket_loaded()

        # Download the file
        rcv_buf = io.BytesIO()
        dataset = bucket.download_file_by_name(file_name=config.b2_file_name)
        logger.downloading_file()
        dataset.save(rcv_buf, allow_seeking=True)

        # Decrypt the dataset in the buffer
        aes_gcm = AES256GCM.from_key(self._decryption_key)
        logger.decrypting_file()
        plaintext = aes_gcm.decrypt_and_verify(rcv_buf.getvalue())
        logger.file_decrypted()

        # Close the buffer
        rcv_buf.close()
        return bytes_to_dataframe(plaintext)
