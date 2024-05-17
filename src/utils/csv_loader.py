from __future__ import annotations

import pandas as pd
from Crypto.Random import get_random_bytes
from crypto.aes_gcm import AES256GCM, csv_to_bytes


class CSVLoader:
    def __init__(self, df: pd.DataFrame) -> None:
        # Load with pandas and make it a dataframe
        self.df = df
