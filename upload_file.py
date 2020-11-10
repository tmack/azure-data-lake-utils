import click
from data_lake_storage import AzureDataLakeUtils
import logging
import json


@click.command()
@click.option('--file-path', help='The directory to list')
@click.option('--upload-path', help='The ADLS storage URL')
@click.option('--overwrite', default=False, help='The ADLS storage URL')
@click.option('--storage-account-name', help='The ADLS storage account name')
@click.option('--settings-path', default='local.settings.json', help='The file path to the local.settings')
def main(file_path, upload_path, overwrite=False, storage_account_name=None, settings_path=None):
    try:
        azure_data_lake = AzureDataLakeUtils()
        upload_results = azure_data_lake.upload_file(file_path, upload_path, overwrite, storage_account_name, settings_path)
        results_to_return = {'date': upload_results.get('date'),
                             'error_code': upload_results.get('error_code'),
                             'upload_path': upload_path}
        click.echo(json.dumps(results_to_return, indent=4, sort_keys=True, default=str))  # date formats get messed up when converting, this converts the dates to strings
        return True

    except Exception as e:
        click.echo({'error_code': e})
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.CRITICAL)
    main()
