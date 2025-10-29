# /home/ubuntu/LiveFetch/tests/test_scraper.py

import pytest
from unittest.mock import patch, MagicMock
from selenium.common.exceptions import NoSuchElementException

# Import functions to be tested
# We import 'scraper' itself to allow monkeypatching its global variables
import scraper
from scraper import setup_driver, get_live_match_data, config

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
    
    # Use monkeypatch to change the global 'DEPLOYED' variable in the scraper module
    monkeypatch.setattr(scraper, 'DEPLOYED', False)

    setup_driver()

    # Assert Edge was called, not Chrome
    mock_webdriver.Edge.assert_called_once()
    mock_webdriver.Chrome.assert_not_called()

    # Assert headless was NOT added
    args = mock_webdriver.Edge.call_args[1]['options'].arguments
    assert "--headless=new" not in args

def test_get_live_match_data_match_finished():
    """
    Test that the scraper returns None if a match result is found.
    """
    # 1. Arrange: Create a mock driver
    mock_driver = MagicMock()
    
    # Configure find_element for "div.match-result-text"
    mock_result_element = MagicMock()
    mock_result_element.text = "Match Finished" # Set text
    mock_driver.find_element.return_value = mock_result_element
    
    # 2. Act
    result = get_live_match_data(mock_driver)
    
    # 3. Assert
    assert result is None
    # Verify it checked for the result text
    mock_driver.find_element.assert_called_with(
        'css selector', "div.match-result-text"
    )

@patch('scraper.WebDriverWait')
def test_get_live_match_data_scraping_logic(MockWebDriverWait):
    """
    Test the main scraping logic by mocking the driver's responses.
    """
    # 1. Arrange
    mock_driver = MagicMock()
    
    # --- Mock "Match Finished" check (to raise error)
    mock_driver.find_element.side_effect = NoSuchElementException
    
    # --- Mock Bookmaker Data
    mock_bookmaker_container = MagicMock()
    mock_bm_row = MagicMock()
    mock_bm_row.find_element.return_value.text = "Team A"
    
    # Mock Back prices
    mock_back_btn = MagicMock()
    mock_back_btn.find_element.side_effect = [
        MagicMock(text="1.50"), # Price
        MagicMock(text="100")   # Size
    ]
    mock_bm_row.find_elements.side_effect = [
        [mock_back_btn], # For 'a.btn-back'
        []               # For 'a.btn-lay'
    ]
    mock_bookmaker_container.find_elements.return_value = [mock_bm_row]

    # --- Mock Fancy/Session Data
    mock_fancy_container = MagicMock()
    mock_fancy_row = MagicMock()
    mock_fancy_row.find_element.side_effect = [
        MagicMock(text="20 over run"), # Market name
        MagicMock(find_element=MagicMock(side_effect=[MagicMock(text="80"), MagicMock(text="1K")])), # Lay btn
        MagicMock(find_element=MagicMock(side_effect=[MagicMock(text="82"), MagicMock(text="1K")])), # Back btn
    ]
    mock_fancy_container.find_elements.return_value = [mock_fancy_row]
    
    # --- Configure WebDriverWait
    # Make wait.until() return the correct mock container
    MockWebDriverWait.return_value.until.side_effect = [
        mock_bookmaker_container,
        mock_fancy_container
    ]

    # 2. Act
    result = get_live_match_data(mock_driver)

    # 3. Assert
    assert result is not None
    
    # Check Bookmaker
    assert len(result['bookmaker']) == 1
    assert result['bookmaker'][0]['team_name'] == "Team A"
    assert result['bookmaker'][0]['back'][0]['price'] == "1.50"
    assert result['bookmaker'][0]['lay'] == []
    
    # Check Session (because "over run" is in the name)
    assert len(result['sessions']) == 1
    assert len(result['fancy']) == 0
    assert result['sessions'][0]['name'] == "20 over run"
    assert result['sessions'][0]['no_val'] == "80"
    assert result['sessions'][0]['yes_val'] == "82"

