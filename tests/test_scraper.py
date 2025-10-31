import pytest
from unittest.mock import patch, MagicMock, call, ANY
import scraper
from scraper import (
    get_auth_token,
    get_market_data,
    _parse_book_string,
    parse_market_data,
    scrape_match_page_data
)
import requests
from selenium.common.exceptions import TimeoutException

# --- Mocks ---

@pytest.fixture
def mock_session():
    """Mocks the global requests.Session."""
    with patch('scraper.http_session', autospec=True) as mock_sess:
        yield mock_sess

# --- API Tests ---

def test_get_auth_token_success(mock_session):
    mock_response = MagicMock()
    mock_response.json.return_value = {"token": "test_token_123"}
    mock_session.post.return_value = mock_response

    token = get_auth_token()

    assert token == "test_token_123"
    mock_session.post.assert_called_once_with(
        scraper.AUTH_URL, headers=ANY, timeout=10
    )

def test_get_auth_token_fail(mock_session):
    mock_session.post.side_effect = requests.exceptions.RequestException("Network Error")
    token = get_auth_token()
    assert token is None

def test_get_market_data_success(mock_session):
    mock_response = MagicMock()
    mock_response.json.return_value = [{"event": {"id": "123"}}]
    mock_session.get.return_value = mock_response

    data = get_market_data("test_token")

    assert data == [{"event": {"id": "123"}}]
    mock_session.get.assert_called_once_with(
        scraper.MARKET_URL, headers=ANY, timeout=10
    )
    assert mock_session.get.call_args[1]['headers']['authorization'] == 'Bearer test_token'

def test_get_market_data_no_token(mock_session):
    data = get_market_data(None)
    assert data is None
    mock_session.get.assert_not_called()

# --- Parsing Tests ---

def test_parse_book_string():
    book_str = "1.249|...|False|16606~ACTIVE~1.38:33k:*1.37:19k:~1.39:1.2k:*1.4:79k:,414464~ACTIVE~3.5:31k:~3.6:11k:"
    runners = [
        {"id": 16606, "name": "Australia"},
        {"id": 414464, "name": "India"}
    ]
    
    result = _parse_book_string(book_str, runners)
    
    assert len(result) == 2
    
    assert result[0]['team_name'] == "Australia"
    assert result[0]['back'] == [
        {"price": "1.38", "size": "33k"},
        {"price": "1.37", "size": "19k"}
    ]
    assert result[0]['lay'] == [
        {"price": "1.39", "size": "1.2k"},
        {"price": "1.4", "size": "79k"}
    ]

    assert result[1]['team_name'] == "India"
    assert result[1]['back'] == [{"price": "3.5", "size": "31k"}]
    assert result[1]['lay'] == [{"price": "3.6", "size": "11k"}]

def test_parse_market_data():
    api_response = [
        {
            "event": {"id": "34886843", "name": "Australia v India"},
            "catalogue": {
                "marketType": "MATCH_ODDS", "inPlay": True, "status": "OPEN",
                "runners": [
                    {"id": 16606, "name": "Australia"},
                    {"id": 414464, "name": "India"}
                ]
            },
            "metadata": {"book": "1.249|...|16606~A~1.5:10k:~1.6:20k:,414464~A~3.0:5k:~3.1:6k:"}
        },
        {
            "event": {"id": "34886843", "name": "Australia v India"},
            "catalogue": {
                "marketType": "BOOKMAKER", "inPlay": True, "status": "OPEN",
                "runners": [
                    {"id": 16606, "name": "Australia"},
                    {"id": 414464, "name": "India"}
                ]
            },
            "metadata": {"book": "4369|...|16606~A~100:1L:~102:2L:,414464~A~-100:1L:~-102:2L:"}
        },
        {
            "event": {"id": "12345", "name": "Other Match"},
            "catalogue": {"marketType": "MATCH_ODDS", "inPlay": False, "status": "SUSPENDED", "runners": []},
            "metadata": {"book": None}
        }
    ]

    result = parse_market_data(api_response)

    assert len(result) == 2
    assert "34886843" in result
    assert "12345" in result

    match_aus_ind = result["34886843"]
    assert match_aus_ind['teams'] == "Australia v India"
    assert match_aus_ind['in_play'] is True
    assert match_aus_ind['result'] == "In Progress"
    
    assert len(match_aus_ind['odds']) == 2
    assert match_aus_ind['odds'][0]['team_name'] == "Australia"
    assert match_aus_ind['odds'][0]['back'][0]['price'] == "1.5"

    assert len(match_aus_ind['bookmarker']) == 2
    assert match_aus_ind['bookmarker'][0]['team_name'] == "Australia"
    assert match_aus_ind['bookmarker'][0]['back'][0]['price'] == "100"

    match_other = result["12345"]
    assert match_other['teams'] == "Other Match"
    assert match_other['in_play'] is False
    assert match_other['result'] == "SUSPENDED"
    assert len(match_other['odds']) == 0

# --- Hybrid Scraper Test ---

@patch('scraper.scrape_fancy_and_sessions', return_value=(["FANCY_DATA"], ["SESSION_DATA"]))
def test_scrape_match_page_data_hybrid(mock_scrape_fancy):
    """
    Tests that the modified scrape_match_page_data function
    correctly combines passed-in data with scraped data.
    """
    mock_driver = MagicMock()
    mock_wait = MagicMock()
    
    # Mock "Match Finished" check (to raise error, meaning 'in progress')
    mock_wait.until.side_effect = TimeoutException
    
    base_data = {
        "match_id": "123",
        "teams": "A v B",
        "odds": ["API_ODDS_DATA"],
        "bookmarker": ["API_BOOKMARKER_DATA"]
    }

    with patch('scraper.WebDriverWait', return_value=mock_wait):
        result = scrape_match_page_data(mock_driver, base_data)

    # Assert "In Progress"
    assert result['result'] == "In Progress"
    
    # Assert fancy/session were called
    mock_scrape_fancy.assert_called_once_with(mock_driver, mock_wait)
    
    # Assert data is a combination
    assert result['odds'] == ["API_ODDS_DATA"] # From base_data
    assert result['bookmarker'] == ["API_BOOKMARKER_DATA"] # From base_data
    assert result['fancy'] == ["FANCY_DATA"] # From scraper
    assert result['sessions'] == ["SESSION_DATA"] # From scraper
    assert result['match_id'] == "123" # From base_data

