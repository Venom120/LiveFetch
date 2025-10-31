import json
from http import HTTPStatus

def test_get_live_data_success(client, mock_data_file):
    """
    Test successful retrieval of data from a valid JSON file.
    """
    sample_data = [{"match_id": "123", "teams": "A vs B"}]
    with open(mock_data_file, 'w') as f:
        json.dump(sample_data, f)

    response = client.get('/api/livedata')

    assert response.status_code == HTTPStatus.OK
    assert response.json == sample_data

def test_get_live_data_empty(client, mock_data_file):
    """
    Test successful retrieval of empty data.
    """
    sample_data = []
    with open(mock_data_file, 'w') as f:
        json.dump(sample_data, f)

    response = client.get('/api/livedata')

    assert response.status_code == HTTPStatus.OK
    assert response.json == []

def test_get_live_data_file_not_found(client, mock_data_file):
    """
    Test the API's response when the JSON file does not exist.
    (mock_data_file fixture provides a path, but we don't write to it)
    """
    response = client.get('/api/livedata')

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert "error" in response.json
    assert "Data file not found" in response.json["error"]

def test_get_live_data_corrupt_json(client, mock_data_file):
    """
    Test the API's response when the JSON file contains invalid JSON.
    """
    with open(mock_data_file, 'w') as f:
        f.write("{'invalid_json': 'missing quotes}") # Write corrupt data

    response = client.get('/api/livedata')

    assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR
    assert "error" in response.json
    assert "Error decoding JSON" in response.json["error"]
