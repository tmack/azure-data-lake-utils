# Azure Data Lake Utils
A set of utilities for interacting with Azure Data Lake Storage. Written in python 3.
## Installing Requirements
```bash
$ pip install -r requirements.txt
```

## Setting up configs
Create the json file 'test-local.settings.json', this is a place to put project settings you'd like to use. Each key/value pair is set as environmental variable during run time.
```json
{
"env_variable_name" : "env_variable_value"
}
```

## Running Tests
To run tests use the following command. 
```bash
 python -m unittest discover -s tests -p 'test_*.py'
```
Tests can also be configured to run from an IDE. See tests/example_of_pycharm_test_configurations.png for an example from pycharm

### Example Testing Output
Here's what testing should look like when its up and running

 ```bash
$ python -m unittest discover -s tests -p 'test_*.py'
............
----------------------------------------------------------------------
Ran 1 tests in 0.001s

OK
```

Example Usage
```python
import logging 
from data_lake_storage import AzureDataLakeUtils

data_lake = AzureDataLakeUtils()
data = 'testing testing'
upload_path = 'default/test.txt'
storage_account_name = 'storageaccountname'
results = data_lake.upload_data(data, upload_path, overwrite=True, storage_account_name=storage_account_name)
logging.info(results)
data_lake.download(file_path='default/test.txt', download_path='test.txt', storage_account_name=storage_account_name)
blobs = data_lake.list(storage_account_name=storage_account_name)
logging.info([blob for blob in blobs])
results = data_lake.upload_file(file_path='test.txt', overwrite=True, upload_path='default/test2.txt',
                                storage_account_name=storage_account_name)
logging.info(results)
file_info = data_lake.get_file_info(file_path='test2.txt', storage_account_name=storage_account_name)
logging.info(file_info)

data_lake.delete_file(file_path='default/test.txt', storage_account_name=storage_account_name)
results = data_lake.delete_file(file_path='default/test2.txt', storage_account_name=storage_account_name)
logging.info(results)
```

