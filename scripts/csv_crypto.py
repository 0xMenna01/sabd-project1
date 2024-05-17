# A simple script to encrypt and decrypt a CSV file using AES-256-GCM.
# The obtained file is downloaded to the local system.
import sys
from loguru import logger
import pandas as pd
from Crypto.Random import get_random_bytes
from src.utils.crypto.aes_gcm import AES256GCM, csv_to_bytes, bytes_to_dataframe


def main() -> None:

    if len(sys.argv) == 3:
        src_path = sys.argv[1]
        dest_path = sys.argv[2]

        key = encrypt_csv_from(src_path, dest_path)

        logger.info(f"Compued AES key: {key}")
        logger.info(f"Encrypted CSV file written to {dest_path}")

    elif len(sys.argv) == 4:
        src_path = sys.argv[1]
        dest_path = sys.argv[2]
        key = sys.argv[3]

        decrypt_csv_from(src_path, dest_path, key)
        logger.info(f"Decrypted CSV file written to {dest_path}")

    else:
        print("Usage: python main.py <src_path> <dest_path>")
        sys.exit(1)


# A utility to download the encrypted version of the CSV file
def encrypt_csv_from(src_path: str, dest_path) -> str:
    """Encrypt a CSV file and download the encrypted bytes."""
    df = pd.read_csv(src_path)
    plaintext = csv_to_bytes(df)
    # Compute random 256-bit key
    aes_key = get_random_bytes(32)
    aes_gcm = AES256GCM.from_key(key=aes_key)
    # Encrypt and write to file
    with open(dest_path, "wb") as file:
        file.write(aes_gcm.encrypt(plaintext))

    return aes_key.hex()


def decrypt_csv_from(src_path: str, dest_path: str, key: str) -> None:
    """Decrypt the CSV file."""
    with open(src_path, "rb") as file:
        ciphertext = file.read()

    aes_key = bytes.fromhex(key)
    aes_gcm = AES256GCM.from_key(key=aes_key)
    plaintext = aes_gcm.decrypt_and_verify(ciphertext)
    df = bytes_to_dataframe(plaintext)
    df.to_csv(dest_path, index=False)


if __name__ == "__main__":
    main()
