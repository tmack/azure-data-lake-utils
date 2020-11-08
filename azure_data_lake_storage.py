import os
from azure.storage.blob import BlobServiceClient
import logging
import re


class AzureDataLakeUtils:

    def __init__(self):
        pass

    def upload_data(self, data, upload_path, overwrite=False, storage_url=None, storage_account_key=None):
        container_name = self._get_container_name(upload_path)
        upload_path = self._update_path_from_container_name(container_name, upload_path)
        data_lake_client = self._get_data_lake_client(storage_url, storage_account_key)
        blob_client = data_lake_client.get_blob_client(container=container_name, blob=upload_path)
        upload_results = blob_client.upload_blob(data, overwrite=overwrite)
        return upload_results

    def upload_file(self, file_path, upload_path, overwrite=False, storage_url=None, storage_account_key=None):
        container_name = self._get_container_name(upload_path)
        upload_path = self.get_blob_path(upload_path)
        data_lake_client = self._get_data_lake_client(storage_url, storage_account_key)
        blob_client = data_lake_client.get_blob_client(container=container_name, blob=upload_path)
        with open(file_path, 'rb') as f:
            file_content = f.read()
            upload_results = blob_client.upload_blob(file_content, overwrite=overwrite)
        return upload_results

    def download(self, file_path, download_path, storage_url=None, storage_account_key=None):
        container_name = self._get_container_name(file_path)
        data_lake_client = self._get_data_lake_client(storage_url, storage_account_key)
        blob_path = self.get_blob_path(file_path)
        blob_client = data_lake_client.get_blob_client(container=container_name, blob=blob_path)
        with open(download_path, "wb") as my_blob:
            download_stream = blob_client.download_blob()
            my_blob.write(download_stream.readall())
        return download_path

    def list(self, directory='', storage_url=None, storage_account_key=None):
        container_name = self._get_container_name(directory)
        data_lake_client = self._get_data_lake_client(storage_url, storage_account_key)
        container_client = data_lake_client.get_container_client(container_name)
        blob_path = self.get_blob_path(directory)
        files = container_client.list_blobs(name_starts_with=blob_path)
        return files

    def get_blob_path(self, directory):
        container_name = self._get_container_name(directory)
        path = directory[len(container_name)+1:]
        path = path if path.startswith('/') else '/' + path
        blob_path = path
        return blob_path

    def get_file_info(self, file_path):
        data_lake_client = self._get_data_lake_client()
        container_name = self._get_container_name(file_path)
        file_path = self._update_path_from_container_name(container_name, file_path)
        blob_client = data_lake_client.get_blob_client(container=container_name, blob=file_path)
        file_info = blob_client.get_blob_properties()
        return file_info

    def delete_file(self, file_path):
        data_lake_client = self._get_data_lake_client()
        container_name = self._get_container_name(file_path)
        file_path = self._update_path_from_container_name(container_name, file_path)
        blob_client = data_lake_client.get_blob_client(container=container_name, blob=file_path)
        delete_results = blob_client.delete_blob()
        return delete_results

    def undelete_file(self, file_path):
        data_lake_client = self._get_data_lake_client()
        container_name = self._get_container_name(file_path)
        file_path = self._update_path_from_container_name(container_name, file_path)
        blob_client = data_lake_client.get_blob_client(container=container_name, blob=file_path)
        undelete_results = blob_client.undelete_blob()
        return undelete_results

    @staticmethod
    def _get_container_name(upload_path, container_name='default'):
        container_name = AzureDataLakeUtils._get_container_name_from_env(container_name)
        container_name = AzureDataLakeUtils._get_container_name_from_string(upload_path, container_name)
        if len(container_name) > 63:
            logging.warning('The container name may be too long, look for errors from Azure')
        return container_name

    @staticmethod
    def _update_path_from_container_name(container_name, upload_path):
        if upload_path.startswith('%s:'.format(container_name)):
            upload_path = upload_path[len('%s:'.format(container_name)):]
        return upload_path

    @staticmethod
    def _get_container_name_from_env(container_name, container_env_key='DataLakeContainerName'):
        if container_env_key in os.environ:
            container_regex = r'(^[a-z0-9]+)'
            container_name_raw = os.environ[container_env_key]
            matches = re.findall(container_regex, container_name_raw)
            container_name_raw_is_valid = True if len(matches) == 1 else False
            if container_name_raw_is_valid:
                if len(container_name_raw) == len(matches[0]):
                    container_name = container_name_raw
        return container_name

    @staticmethod
    def _get_container_name_from_string(string, container_name=None):
        container_delimiter_regex = r'(\A[a-z]+)(?::|/)'
        matches = re.findall(container_delimiter_regex, string)
        upload_path_has_container_name = True if len(matches) == 1 else False
        if upload_path_has_container_name:
            container_name = matches[0]
        return container_name

    @staticmethod
    def _get_data_lake_client(storage_url=None, storage_account_key=None):
        try:
            if storage_url is None:
                storage_url = os.environ['StorageURL']
            if storage_account_key is None:
                storage_account_key = os.environ['StorageAccountKey']
            client = BlobServiceClient(account_url=storage_url, credential=storage_account_key)
            return client
        except:
            logging.exception('Unable to get data lake client')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)