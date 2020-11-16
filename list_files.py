import click
from azure_data_lake_storage import AzureStorageAccount
import logging
import json

@click.command()
@click.option('--directory', default="", help='The directory to list')
@click.option('--storage-account-name', help='The ADLS storage name')
@click.option('--settings-path', default="local.settings.json", help='The path to the local settings file')
def main(directory='', storage_account_name=None, settings_path=None):
    try:
        azure_data_lake = AzureStorageAccount()
        file_iterator = azure_data_lake.list(directory, storage_account_name, settings_path)
        listed_files = [{'size': x['size'], 'name': x['name']} for x in file_iterator]
        listed_files = json.dumps({'error_code': None, 'files': listed_files})
        click.echo(listed_files)
        return listed_files
    except Exception as e:
        click.echo({'error_code': e})
        raise Exception(e)


if __name__ == "__main__":
    logging.basicConfig(level=logging.CRITICAL)
    main()