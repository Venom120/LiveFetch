# /home/ubuntu/LiveFetch/tests/test_settings.py

import pytest
import tempfile
import os
import json

from api_server import app as flask_app 

# Load the config manually from settings.json
# This assumes test_settings.py is in 'tests/' and settings.json is one level up
SETTINGS_PATH = os.path.join(os.path.dirname(__file__), '..', 'settings.json')
try:
    with open(SETTINGS_PATH, 'r') as f:
        api_config = json.load(f)
except Exception as e:
    print(f"FATAL ERROR in test_settings: Could not load {SETTINGS_PATH}. {e}")
    # Provide a minimal fallback so tests don't crash
    api_config = {"Paths": {"DATA_FILE": "live_data.json"}}


@pytest.fixture(scope='module')
def client():
    """Create a test client for the Flask app."""
    # This now uses the flask_app imported above
    with flask_app.test_client() as client:
        yield client

@pytest.fixture
def mock_data_file(monkeypatch):
    """
    Creates a temporary data file and monkeypatches the 
    api_server.DATA_FILE variable to point to it.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        # Get the configured file name from our loaded config
        file_name = api_config['Paths']['DATA_FILE']
        temp_path = os.path.join(tmpdir, file_name)
        
        # Monkeypatch the variable in the api_server module
        monkeypatch.setattr("api_server.DATA_FILE", temp_path)
        
        yield temp_path