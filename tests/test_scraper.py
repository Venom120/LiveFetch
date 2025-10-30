# /home/ubuntu/LiveFetch/tests/test_scraper.py

import pytest
from unittest.mock import patch, MagicMock, call
from selenium.common.exceptions import (
    NoSuchElementException, 
    TimeoutException, 
    StaleElementReferenceException
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

# Import functions to be tested
import scraper
from scraper import (
    setup_driver, 
    _parse_odds_table, 
    scrape_match_page_data, 
    scrape_match_odds, 
    scrape_bookmarker, 
    scrape_fancy_and_sessions
)

@patch('scraper.webdriver')
def test_setup_driver_deployed(mock_webdriver, monkeypatch):
    """Test that setup_driver uses ChromeOptions when DEPLOYED=True."""
    
    # Use monkeypatch to change the global 'DEPLOYED' variable in the scraper module
    monkeypatch.setattr(scraper, 'DEPLOYED', True)
    
    setup_driver()

    # Assert Chrome was called, not Edge
    mock_webdriver.Chrome.assert_called_once()
    mock_webdriver.Edge.assert_not_called()
    
    # Assert headless argument was added
    args = mock_webdriver.Chrome.call_args[1]['options'].arguments
    assert "--headless" in args
    assert "--no-sandbox" in args
    
    # No need to reset, monkeypatch automatically reverts after the test

@patch('scraper.webdriver')
def test_setup_driver_local(mock_webdriver, monkeypatch):
    """Test that setup_driver uses EdgeOptions when DEPLOYED=False."""
    
    # Use monkeypatch to change the global variables in the scraper module
    monkeypatch.setattr(scraper, 'DEPLOYED', False)
    monkeypatch.setattr(scraper, 'HEADLESS', False)

    setup_driver()

    # Assert Edge was called, not Chrome
    mock_webdriver.Edge.assert_called_once()
    mock_webdriver.Chrome.assert_not_called()

    # Assert headless was NOT added
    args = mock_webdriver.Edge.call_args[1]['options'].arguments
    assert "--headless" not in args

def test_parse_odds_table():
    """Test the _parse_odds_table helper function."""
    mock_table_body = MagicMock()
    
    # --- Mock Row 1 (Team A) ---
    mock_row_1 = MagicMock()
    mock_row_1.find_element.return_value.text = "Team A" # For span.in-play-title
    
    # Mock Back buttons
    mock_back_1 = MagicMock()
    mock_back_1.find_element.side_effect = [MagicMock(text="1.50"), MagicMock(text="100")]
    mock_back_2 = MagicMock()
    mock_back_2.find_element.side_effect = [MagicMock(text="1.51"), MagicMock(text="200")]
    
    # Mock Lay buttons
    mock_lay_1 = MagicMock()
    mock_lay_1.find_element.side_effect = [MagicMock(text="1.52"), MagicMock(text="300")]
    
    mock_row_1.find_elements.side_effect = [
        [mock_back_1, mock_back_2], # For a.btn-back
        [mock_lay_1]               # For a.btn-lay
    ]

    # --- Mock Row 2 (Team B) ---
    mock_row_2 = MagicMock()
    mock_row_2.find_element.return_value.text = "Team B"
    mock_row_2.find_elements.side_effect = [
        [], # No back buttons
        []  # No lay buttons
    ]

    # --- Mock Header Row (should be skipped by having no title) ---
    mock_row_3 = MagicMock()
    mock_row_3.find_element.side_effect = NoSuchElementException # No span.in-play-title

    # Mock XPath for finding rows
    mock_table_body.find_elements.return_value = [mock_row_1, mock_row_2, mock_row_3]
    
    # --- Act ---
    result = _parse_odds_table(mock_table_body)
    
    # --- Assert ---
    mock_table_body.find_elements.assert_called_with(By.XPATH, ".//tr[position() > 1 and count(td) > 1]")
    assert len(result) == 2
    assert result[0]['team_name'] == "Team A"
    assert len(result[0]['back']) == 2
    assert result[0]['back'][0] == {"price": "1.50", "size": "100"}
    assert result[0]['lay'][0] == {"price": "1.52", "size": "300"}
    
    assert result[1]['team_name'] == "Team B"
    assert len(result[1]['back']) == 0
    assert len(result[1]['lay']) == 0

@patch('scraper.scrape_match_odds', return_value="ODDS_DATA")
@patch('scraper.scrape_bookmarker', return_value="BOOKMARKER_DATA")
@patch('scraper.scrape_fancy_and_sessions', return_value=(["FANCY_DATA"], ["SESSION_DATA"]))
def test_scrape_match_page_data_live(mock_scrape_fancy, mock_scrape_bookmarker, mock_scrape_odds):
    """Test scrape_match_page_data for a LIVE match."""
    mock_driver = MagicMock()
    mock_wait = MagicMock()
    
    # Mock "Match Finished" check (to raise error, meaning 'in progress')
    # The first call to until (for result) raises Timeout
    mock_wait.until.side_effect = TimeoutException
    
    with patch('scraper.WebDriverWait', return_value=mock_wait) as mock_wait_constructor:
        result = scrape_match_page_data(mock_driver, is_live_match=True)
    
    # Assert "In Progress"
    assert result['result'] == "In Progress"
    
    # Assert all scraping functions were called
    mock_scrape_odds.assert_called_once_with(mock_driver, mock_wait)
    mock_scrape_bookmarker.assert_called_once_with(mock_driver, mock_wait)
    mock_scrape_fancy.assert_called_once_with(mock_driver, mock_wait)
    
    # Assert data is populated
    assert result['odds'] == "ODDS_DATA"
    assert result['bookmarker'] == "BOOKMARKER_DATA"
    assert result['fancy'] == ["FANCY_DATA"]
    assert result['sessions'] == ["SESSION_DATA"]
    assert "winner" not in result

@patch('scraper.scrape_match_odds', return_value="WINNER_DATA")
@patch('scraper.scrape_bookmarker', return_value="BOOKMARKER_DATA")
@patch('scraper.scrape_fancy_and_sessions')
def test_scrape_match_page_data_old(mock_scrape_fancy, mock_scrape_bookmarker, mock_scrape_odds):
    """Test scrape_match_page_data for an OLD match."""
    mock_driver = MagicMock()
    mock_wait = MagicMock()
    
    # Mock "Match Finished" check (to return "Finished")
    mock_result_element = MagicMock(text="Match Finished")
    mock_wait.until.return_value = mock_result_element
    
    with patch('scraper.WebDriverWait', return_value=mock_wait) as mock_wait_constructor:
        result = scrape_match_page_data(mock_driver, is_live_match=False)
    
    # Assert result text
    assert result['result'] == "Match Finished"
    
    # Assert correct functions were called
    mock_scrape_odds.assert_called_once_with(mock_driver, mock_wait) # Called for 'winner'
    mock_scrape_bookmarker.assert_called_once_with(mock_driver, mock_wait)
    mock_scrape_fancy.assert_not_called() # Not called for old matches
    
    # Assert data is populated
    assert result['winner'] == "WINNER_DATA"
    assert result['bookmarker'] == "BOOKMARKER_DATA"
    assert "odds" not in result
    assert "fancy" not in result
    assert "sessions" not in result

@patch('scraper._parse_odds_table', return_value="PARSED_DATA")
@patch('scraper.EC') # <-- Patch EC *within the scraper module*
def test_scrape_match_odds_success(mock_EC, mock_parse): # <-- Add mock_EC here
    """Test scrape_match_odds successfully finds the container."""
    mock_driver = MagicMock()
    mock_wait = MagicMock()
    mock_container = MagicMock()
    mock_wait.until.return_value = mock_container

    # --- This is the new, robust way to test this ---
    # 1. Create a mock for the *result* of the EC call
    mock_locator = MagicMock()
    mock_EC.presence_of_element_located.return_value = mock_locator
    
    result = scrape_match_odds(mock_driver, mock_wait)
    
    # 2. Assert that our *mocked* EC was called with the correct XPath
    expected_xpath = "//*[@id='root']/body/div[6]/div[2]/div/div[4]/div[1]/div[2]/table/tbody"
    mock_EC.presence_of_element_located.assert_called_once_with(
        (By.XPATH, expected_xpath)
    )
    
    # 3. Assert that wait.until was called with the mock_locator we created
    mock_wait.until.assert_called_once_with(mock_locator)
    # --- End of new assertions ---

    mock_parse.assert_called_once_with(mock_container)
    assert result == "PARSED_DATA"

def test_scrape_fancy_sessions_logic():
    """Test the classification logic of scrape_fancy_and_sessions."""
    mock_driver = MagicMock()
    mock_wait = MagicMock()
    mock_container = MagicMock()
    mock_wait.until.return_value = mock_container

    # Mock Row 1 (Session)
    mock_row_1 = MagicMock()
    mock_row_1.find_element.side_effect = [
        MagicMock(text="20 over run"), # Market name
        MagicMock(find_element=MagicMock(side_effect=[MagicMock(text="80"), MagicMock(text="1K")])), # Lay
        MagicMock(find_element=MagicMock(side_effect=[MagicMock(text="82"), MagicMock(text="1K")])), # Back
    ]
    
    # Mock Row 2 (Fancy)
    mock_row_2 = MagicMock()
    mock_row_2.find_element.side_effect = [
        MagicMock(text="Team A to win"), # Market name
        MagicMock(find_element=MagicMock(side_effect=[MagicMock(text="1.5"), MagicMock(text="10K")])), # Lay
        MagicMock(find_element=MagicMock(side_effect=[MagicMock(text="1.6"), MagicMock(text="5K")])), # Back
    ]
    
    # Mock Row 3 (Stale/Error)
    mock_row_3 = MagicMock()
    mock_row_3.find_element.side_effect = StaleElementReferenceException
    
    mock_container.find_elements.return_value = [mock_row_1, mock_row_2, mock_row_3]
    
    fancy, sessions = scrape_fancy_and_sessions(mock_driver, mock_wait)
    
    assert len(fancy) == 1
    assert fancy[0]['name'] == "Team A to win"
    assert fancy[0]['no_val'] == "1.5"
    assert fancy[0]['yes_val'] == "1.6"
    
    assert len(sessions) == 1
    assert sessions[0]['name'] == "20 over run"
    assert sessions[0]['no_val'] == "80"
    assert sessions[0]['yes_val'] == "82"