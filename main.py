import logging
from azure.identity import ClientSecretCredential
from azure.appconfiguration import AzureAppConfigurationClient
import settings_manager


def test_app_conf_integration():
    tenant_id, client_id, client_secret = settings_manager.load_service_principal()
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    app_config_name = settings_manager.load_app_config_name()
    app_config_client = AzureAppConfigurationClient(base_url=f"https://{app_config_name}.azconfig.io", credential=credential)
    configuration_settings = app_config_client.list_configuration_settings()
    [print(x) for x in configuration_settings]


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    test_app_conf_integration()
