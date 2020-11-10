import logging
from data_lake_storage import AzureDataLakeUtils


def test_data_lake_storage():
    data_lake = AzureDataLakeUtils()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    test_data_lake_storage()
