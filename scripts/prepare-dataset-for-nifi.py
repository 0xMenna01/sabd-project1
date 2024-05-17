import os
from src.api.b2.api import B2ApiBuilder


DATASET_OUT_DIR = os.getenv('DATASET_OUT_DIR', '/tmp')
DATASET_FILE_NAME = '/raw_data_medium-utv_sorted.csv'


def main():
    b2_api = B2ApiBuilder.from_default_keys().build()
    dataset = b2_api.get().get_dataset()

    with open(DATASET_OUT_DIR + DATASET_FILE_NAME, 'w') as f:
        f.write(dataset)


if __name__ == "__main__":
    main()
