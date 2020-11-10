import logging
import os
import json


class AzureDataLakeCredentialManager:

	def __init__(self):
		pass

	@staticmethod
	def load_service_principal(storage_account_name, path=None):
		# tenant_id, client_id, client_secret, storage_account_name
		# first try to load from environment variables
		env_tenant_id, env_client_id, env_client_secret = AzureDataLakeCredentialManager._load_service_principal_from_env(storage_account_name)

		# try and load the settings file if they exists
		file_path_provided = True if path is None else False
		if file_path_provided:
			tenant_id, client_id, client_secret = AzureDataLakeCredentialManager._load_service_principal_from_file(storage_account_name)
		else:
			tenant_id, client_id, client_secret = AzureDataLakeCredentialManager._load_service_principal_from_file(storage_account_name, path=path)

		# if the file settings don't exist, then use the env variable settings
		if tenant_id is None:
			tenant_id = env_tenant_id
		if client_id is None:
			client_id = env_client_id
		if client_secret is None:
			client_secret = env_client_secret

		return tenant_id, client_id, client_secret

	@staticmethod
	def _load_service_principal_from_env(storage_account_name):
		tenant_id, client_id, client_secret = None, None, None
		if os.getenv(f'{storage_account_name}-TenantID'):
			tenant_id = os.environ[f'{storage_account_name}-TenantID']
		if os.getenv(f'{storage_account_name}-ClientID'):
			client_id = os.environ[f'{storage_account_name}-ClientID']
		if os.getenv(f'{storage_account_name}-ClientSecret'):
			client_secret = os.environ[f'{storage_account_name}-ClientSecret']
		return tenant_id, client_id, client_secret

	@staticmethod
	def _load_local_settings_from_file(path):
		if os.path.exists(path):
			with open(path, 'r') as f:
				raw_settings = f.read()
				settings = json.loads(raw_settings)
				return settings
		logging.warning(f'{path} doesn\'t exist. Not loading settings from file')
		return None

	@staticmethod
	def _file_storage_account_settings_exist(storage_account_settings):
		expected_settings = ['tenantID', 'clientID', 'clientSecret']
		missing_settings = [setting for setting in storage_account_settings.keys() if setting not in expected_settings]
		settings_are_missing = len(missing_settings) > 0
		if settings_are_missing:
			logging.warning(f'The following settings are missing {", ".join(missing_settings)}')
			return False
		return True

	@staticmethod
	def _load_service_principal_from_file(storage_account_name, path='local.settings.json'):
		settings = AzureDataLakeCredentialManager._load_local_settings_from_file(path)
		storage_account_is_in_settings = 'StorageAccounts' in settings.keys() and storage_account_name in settings['StorageAccounts']
		if storage_account_is_in_settings:
			storage_accounts_settings = settings['StorageAccounts'][storage_account_name]
			AzureDataLakeCredentialManager._file_storage_account_settings_exist(storage_accounts_settings)
			tenant_id = storage_accounts_settings['tenantID']
			client_id = storage_accounts_settings['clientID']
			client_secret = storage_accounts_settings['clientSecret']
			return tenant_id, client_id, client_secret
		logging.warning(f'Unable to load storage account {storage_account_name} from f{path}')
		return None



