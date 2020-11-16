import json
import os
import logging
from dataclasses import dataclass


@dataclass
class ServicePrincipal:
	"""Class for loading Azure Service Principals"""
	tenant_id: str
	client_id: str
	client_secret: str

	def __init__(self, tenant_id=None, client_id=None, client_secret=None, settings_file_path=None):
		self.tenant_id = self.load_setting('AZ_TENANT_ID', tenant_id, settings_file_path)
		self.client_id = self.load_setting('AZ_CLIENT_ID', client_id, settings_file_path)
		self.client_secret = self.load_setting('AZ_CLIENT_SECRET', client_secret, settings_file_path)

	def load_setting(self, secret_name, default_value, settings_file_path=None):
		# if default value is provided, just use that
		if default_value is not None:
			return default_value
		# if a default value is not provided, try to load from file
		if settings_file_path is not None:
			secret_value = self.load_secret_from_file(secret_name, settings_file_path)
			if secret_value is not None:
				return secret_value
		# if the file doesn't have a secret check the environment
		if os.getenv(secret_name) is not None:
			return os.environ[secret_name]

	@staticmethod
	def load_secret_from_file(secret_name, settings_file_path):
		if os.path.exists(settings_file_path) is False:
			logging.warning(f'settings_file_path: {settings_file_path} set but file does not exist')
			return None
		with open(settings_file_path, 'r') as f:
			raw_settings = f.read()
			settings = json.loads(raw_settings)
			if secret_name not in settings:
				logging.warning(f'secret_name: {secret_name} not found in file: {settings_file_path}')
				return None
			return settings[secret_name]

	def get_tenant_id_client_id_and_secret(self):
		return self.tenant_id, self.client_id, self.client_secret

	def validate(self, raise_exception=True):
		missing_settings = []
		if self.tenant_id is None:
			missing_settings.append('tenant_id')
		if self.client_id is None:
			missing_settings.append('client_id')
		if self.client_secret is None:
			missing_settings.append('client_secret')
		if len(missing_settings) > 0:
			error_message = f'Missing the following service principal settings: "{", ".join(missing_settings)}"'
			logging.warning(error_message)
			if raise_exception:
				raise Exception(error_message)
			else:
				return False
		return True


if __name__ == "__main__":
	logging.basicConfig(level=logging.INFO)
	service_principal = ServicePrincipal(settings_file_path='local.settings.json')
