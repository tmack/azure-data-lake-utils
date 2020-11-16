import click
from azure_data_lake_storage import AzureStorageAccount
import logging

@click.command()
@click.option('--file-path', help='The directory to list')
@click.option('--storage-account-name', help='The ADLS storage account name')
@click.option('--settings-path', default='local.settings.json', help='The file path to the local.settings')
def main(file_path, storage_account_name=None, settings_path=None):
    try:
        azure_data_lake = AzureStorageAccount()
        results = azure_data_lake.delete_file(file_path, storage_account_name, settings_path)
        click.echo(f'{results}')
        return True

    except Exception as e:
        click.echo({'error_code': e})
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.CRITICAL)
    main()
