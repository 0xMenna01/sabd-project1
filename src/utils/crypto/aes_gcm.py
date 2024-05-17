from __future__ import annotations

import io
from typing import Optional
from Crypto.Protocol.KDF import PBKDF2
from Crypto.Random import get_random_bytes
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad
from loguru import logger
import pandas as pd


class AES256GCM:
    """AES-256-GCM encryption and decryption"""

    def __init__(self, key) -> None:
        # AES-GCM key
        self._key = key

    @staticmethod
    def from_key(key: bytes) -> AES256GCM:
        return AES256GCM(key)

    def encrypt(self, plaintext: bytes) -> bytes:
        iv = get_random_bytes(16)
        cipher = AES.new(self._key, AES.MODE_GCM, iv)
        ciphertext, tag = cipher.encrypt_and_digest(plaintext)
        return iv + tag + ciphertext

    def decrypt_and_verify(self, ciphertext: bytes) -> bytes:
        iv = ciphertext[:16]
        tag = ciphertext[16:32]
        ciphertext = ciphertext[32:]
        cipher = AES.new(self._key, AES.MODE_GCM, iv)
        return cipher.decrypt_and_verify(ciphertext, tag)


# Just some conversion utilities
def csv_to_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode('utf-8')


def bytes_to_dataframe(data: bytes) -> pd.DataFrame:
    csv = data.decode('utf-8')
    return pd.read_csv(io.StringIO(csv))
