import unittest
from tests.test_helper import setup_test_local_settings
from azure_data_lake_credential_manager import AzureDataLakeCredentialManager
import os


class TestGetDataLakeClient(unittest.TestCase):

    def setUp(self):
        setup_test_local_settings()

    def test_load_storage_account_service_principal_from_file(self):
        # setup
        settings_path = 'test-local.settings.json'
        storage_account_name = 'teststorageaccount'

        # exercise
        tenant_id, client_id, client_secret = AzureDataLakeCredentialManager._load_service_principal_from_file(
                                                                        storage_account_name,
                                                                        settings_path)

        # validate
        expected_tenant_id = 'file_tenant_id'
        expected_client_id = 'file_client_id'
        expected_client_secret = 'file_client_secret'
        self.assertEqual(expected_tenant_id, tenant_id)
        self.assertEqual(expected_client_id, client_id)
        self.assertEqual(expected_client_secret, client_secret)

    def test_load_storage_account_service_principal_from_env(self):
        # setup
        storage_account_name = 'teststorageaccount'
        os.environ['teststorageaccount-TenantID'] = 'envTenantID'
        os.environ['teststorageaccount-ClientID'] = 'envClientID'
        os.environ['teststorageaccount-ClientSecret'] = 'envClientSecret'
        # exercise
        tenant_id, client_id, client_secret = AzureDataLakeCredentialManager._load_service_principal_from_env(
                                                                        storage_account_name)

        # validate
        expected_tenant_id = 'envTenantID'
        expected_client_id = 'envClientID'
        expected_client_secret = 'envClientSecret'
        self.assertEqual(expected_tenant_id, tenant_id)
        self.assertEqual(expected_client_id, client_id)
        self.assertEqual(expected_client_secret, client_secret)

    def test_load_storage_account_service_principal_from_file_override_env(self):
        # setup
        settings_path = 'test-local.settings.json'
        storage_account_name = 'teststorageaccount'
        os.environ['teststorageaccountTenantID'] = 'envTenantID'
        os.environ['teststorageaccountClientID'] = 'envClientID'
        os.environ['teststorageaccountClientSecret'] = 'envClientSecret'

        # exercise
        tenant_id, client_id, client_secret = AzureDataLakeCredentialManager.load_service_principal(
                                                                        storage_account_name,
                                                                        settings_path)

        # validate
        expected_tenant_id = 'file_tenant_id'
        expected_client_id = 'file_client_id'
        expected_client_secret = 'file_client_secret'
        self.assertEqual(expected_tenant_id, tenant_id)
        self.assertEqual(expected_client_id, client_id)
        self.assertEqual(expected_client_secret, client_secret)

