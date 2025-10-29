# /home/ubuntu/LiveFetch/tests/conftest.py

import pytest
import tempfile
import os
import json
from api_server import app as flask_app
from api_server import config as api_config  # This imports the 'config' dict

@pytest.fixture(scope='module')
def client():
    """Create a test client for the Flask app."""
    with flask_app.test_client() as client:
        yield client

@pytest.fixture
def mock_data_file(monkeypatch):
    """
    Creates a temporary data file and monkeypatches the 
    api_server.JSON_DATA_FILE variable to point to it.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        # Get the configured file name (e.g., "live_data.json")
        file_name = api_config['Paths']['JSON_FILE_NAME']
        temp_path = os.path.join(tmpdir, file_name)
        
        # Monkeypatch the variable in the api_server module
        monkeypatch.setattr("api_server.JSON_DATA_FILE", temp_path)
        
        yield temp_path
