import json
import os
import logging


def load_settings(path):
	local_settings_file_exists = os.path.exists(path)
	if local_settings_file_exists is False:
		logging.error(f'{path} doesn\'t exist. Unable to load local settings')
	with open(path, 'r') as f:
		raw_settings = f.read()
		settings = json.loads(raw_settings)
	return settings


def load_service_principal(path='local.settings.json'):
	settings = load_settings(path)
	tenant_id = settings.get('tenantID')
	client_id = settings.get('clientID')
	client_secret = settings.get('clientSecret')
	return tenant_id, client_id, client_secret


def load_app_config_name(path='local.settings.json'):
	settings = load_settings(path)
	app_config_name = settings['appConfig']
	return app_config_name
