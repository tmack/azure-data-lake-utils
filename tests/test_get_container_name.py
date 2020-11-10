import unittest
from tests.test_helper import setup_test_local_settings
from azure_data_lake_storage import AzureDataLakeUtils
import os


class TestGetContainerName(unittest.TestCase):

    def setUp(self):
        setup_test_local_settings()

        # reset environmental variables before each test
        if os.getenv('DataLakeContainerName'):
            del os.environ['DataLakeContainerName']

    def test_get_default_container_name(self):
        # setup
        upload_path = 'test.txt'

        # exercise
        container_name = AzureDataLakeUtils._get_container_name(upload_path)

        # validate
        expected_container_name_container = 'default'
        self.assertEqual(expected_container_name_container, container_name)

    def test_get_container_name_from_env(self):
        # setup
        upload_path = 'test.txt'
        os.environ['DataLakeContainerName'] = 'testcontainername'

        # exercise
        container_name = AzureDataLakeUtils._get_container_name(upload_path)

        # validate
        expected_container_name_container = 'testcontainername'
        self.assertEqual(expected_container_name_container, container_name)

    def test_get_container_name_from_string_slash(self):
        # setup
        upload_path = 'testcontainername/test.txt'

        # exercise
        container_name = AzureDataLakeUtils._get_container_name(upload_path)

        # validate
        expected_container_name_container = 'testcontainername'
        self.assertEqual(expected_container_name_container, container_name)

    def test_get_container_name_from_string_colon(self):
        # setup
        upload_path = 'testcontainername:test.txt'

        # exercise
        container_name = AzureDataLakeUtils._get_container_name(upload_path)

        # validate
        expected_container_name_container = 'testcontainername'
        self.assertEqual(expected_container_name_container, container_name)

    def test_get_container_name_from_string_override(self):
        # setup
        upload_path = 'testcontainername:test.txt'
        os.environ['DataLakeContainerName'] = 'envcontainername'

        # exercise
        container_name = AzureDataLakeUtils._get_container_name(upload_path)

        # validate
        expected_container_name_container = 'testcontainername'
        self.assertEqual(expected_container_name_container, container_name)

    def test_get_container_name_long(self):
        # setup
        upload_path = 'test.txt'
        really_long_container_name = 'x' * 64
        os.environ['DataLakeContainerName'] = really_long_container_name

        # exercise
        container_name = AzureDataLakeUtils._get_container_name(upload_path)

        # validate
        self.assertEqual(really_long_container_name, container_name)

    def test_get_container_name_invalid_name_capitalization(self):
        # setup
        upload_path = 'test.txt'
        capitalized_container_name = 'ContainerName'
        os.environ['DataLakeContainerName'] = capitalized_container_name

        # exercise
        container_name = AzureDataLakeUtils._get_container_name(upload_path)

        # validate
        expected_container_name = 'default'
        self.assertEqual(expected_container_name, container_name)


    def test_get_container_name_invalid_name_special_chars(self):
        # setup
        upload_path = 'test.txt'
        capitalized_container_name = 'container_name'
        os.environ['DataLakeContainerName'] = capitalized_container_name

        # exercise
        container_name = AzureDataLakeUtils._get_container_name(upload_path)

        # validate
        expected_container_name = 'default'
        self.assertEqual(expected_container_name, container_name)
