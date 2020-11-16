import click
from azure_data_lake_storage import AzureStorageAccount
import logging
import json

@click.command()
@click.option('--adls-path', help='The directory to download')
@click.option('--save-path', help='The ADLS storage URL')
@click.option('--storage-account-name', help='The ADLS storage account name')
@click.option('--settings-path', default="local.settings.json", help='The file path to the local.settings')
def main(adls_path, save_path, storage_account_name=None, settings_path=None):
    try:
        azure_data_lake = AzureStorageAccount()
        save_path = azure_data_lake.download(adls_path, save_path, storage_account_name=storage_account_name, settings_path=settings_path)
        response = {'save_path': save_path, 'error_code': None}
        click.echo(json.dumps(response))
        return True

    except Exception as e:
        click.echo({'error_code': e})
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.CRITICAL)
    main()