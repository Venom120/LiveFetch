# /home/ubuntu/LiveFetch/scraper.py
DEPLOYED = False  # Set to True when deploying to production

import json
import time
import os
import tempfile
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as chromeOptions
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException

# --- Configuration ---
TARGET_URL = "https://www.radheexch.xyz/game/4"
JSON_OUTPUT_FILE = "/app/data/live_data.json" if DEPLOYED else "./data/live_data.json" # Must match JSON_OUTPUT_FILE in api_server.py
SCRAPE_INTERVAL_SECONDS = 2
WEB_DRIVER_TIMEOUT = 10

# --- Selenium Driver Setup ---

def setup_driver():
    """Initializes and returns a headless Selenium WebDriver."""
    try:
        options = chromeOptions() if DEPLOYED else EdgeOptions()
        # options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36")
        
        # --- DNS Configuration ---
        local_state = {
            "dns_over_https.mode": "secure",
            "dns_over_https.templates": "https://chrome.cloudflare-dns.com/dns-query",
        }
        options.add_experimental_option('localState', local_state)

        driver = webdriver.Chrome(options=options) if DEPLOYED else webdriver.Edge(options=options) # type: ignore
            
        driver.set_page_load_timeout(WEB_DRIVER_TIMEOUT)
        return driver
    except Exception as e:
        print(f"Error initializing WebDriver: {e}")
        return None
    
# --- JSON File Handling ---

def write_to_json(data, filename):
    """Atomically writes data to a JSON file."""
    temp_path = None
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        temp_dir = os.path.dirname(filename)
        with tempfile.NamedTemporaryFile('w', delete=False, dir=temp_dir, encoding='utf-8') as temp_f:
            json.dump(data, temp_f, indent=4)
            temp_path = temp_f.name
        
        os.replace(temp_path, filename)
        print(f"Successfully updated {filename}")
    except (IOError, os.error, json.JSONDecodeError) as e:
        print(f"Error writing to JSON file {filename}: {e}")
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)

# --- Scraping Functions ---

def get_live_match_count(driver):
    """Finds all live matches and returns their count."""
    try:
        match_table_xpath = "//*[@id='root']/body/div[6]/div[2]/div[2]/div[2]/table/tbody"
        wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
        tbody = wait.until(EC.presence_of_element_located((By.XPATH, match_table_xpath)))
        
        # Find rows that are live
        live_rows = tbody.find_elements(By.CLASS_NAME, "livenownew")
        return len(live_rows)
        
    except (TimeoutException, NoSuchElementException):
        print("Could not find match table to count live matches.")
        return 0
    except Exception as e:
        print(f"Error in get_live_match_count: {e}")
        return 0

def get_live_match_data(driver):
    """
    Scrapes Bookmaker, Fancy, and Session odds from the *current* page.
    This function assumes the driver is already on the match detail page.
    """
    wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
    
    # --- Scrape Bookmaker ---
    bookmaker_data = []
    try:
        bookmaker_xpath = "//*[@id='root']/body/div[6]/div[2]/div/div[4]/div[2]"
        bookmaker_container = wait.until(EC.presence_of_element_located((By.XPATH, bookmaker_xpath)))
        team_rows = bookmaker_container.find_elements(By.XPATH, ".//table/tbody/tr[position() > 1 and count(td) > 1]")
        
        for row in team_rows:
            team_name = row.find_element(By.CSS_SELECTOR, "span.in-play-title").text
            
            back_prices = []
            back_elements = row.find_elements(By.CSS_SELECTOR, "a.btn-back")
            for el in back_elements:
                price = el.find_element(By.CSS_SELECTOR, "div").text.strip()
                size = el.find_element(By.CSS_SELECTOR, "span").text.strip()
                if price:
                    back_prices.append({"price": price, "size": size})

            lay_prices = []
            lay_elements = row.find_elements(By.CSS_SELECTOR, "a.btn-lay")
            for el in lay_elements:
                price = el.find_element(By.CSS_SELECTOR, "div").text.strip()
                size = el.find_element(By.CSS_SELECTOR, "span").text.strip()
                if price:
                    lay_prices.append({"price": price, "size": size})
            
            bookmaker_data.append({
                "team_name": team_name,
                "back": back_prices,
                "lay": lay_prices
            })

    except (TimeoutException, NoSuchElementException, Exception) as e:
        print(f"Could not scrape Bookmaker data: {e}")

    # --- Scrape Fancy & Sessions ---
    fancy_data = []
    session_data = []
    try:
        fancy_xpath = "//*[@id='root']/body/div[6]/div[2]/div/div[5]/div/div[4]/table/tbody"
        fancy_container = wait.until(EC.presence_of_element_located((By.XPATH, fancy_xpath)))
        
        # Select only data rows, skip headers/mobile separators
        market_rows = fancy_container.find_elements(By.XPATH, ".//tr[not(contains(@class, 'bet-all-new')) and not(contains(@class, 'brblumobile'))]")
        
        for row in market_rows:
            market_name = row.find_element(By.CSS_SELECTOR, "span.marketnamemobile").text.strip()
            
            # Find No/Lay values
            lay_btn = row.find_element(By.CSS_SELECTOR, "a.btn-lay")
            no_val = lay_btn.find_element(By.CSS_SELECTOR, "div").text.strip()
            no_size = lay_btn.find_element(By.CSS_SELECTOR, "span").text.strip()

            # Find Yes/Back values
            back_btn = row.find_element(By.CSS_SELECTOR, "a.btn-back")
            yes_val = back_btn.find_element(By.CSS_SELECTOR, "div").text.strip()
            yes_size = back_btn.find_element(By.CSS_SELECTOR, "span").text.strip()
            
            market_item = {
                "name": market_name, 
                "no_val": no_val, 
                "no_size": no_size, 
                "yes_val": yes_val, 
                "yes_size": yes_size
            }

            if "over" in market_name.lower() or "run" in market_name.lower():
                session_data.append(market_item)
            else:
                fancy_data.append(market_item)
                
    except (TimeoutException, NoSuchElementException, Exception) as e:
        print(f"Could not scrape Fancy/Session data: {e}")

    # --- Scrape Result ---
    result = "In Progress" # Default
    try:
        # --- PLACEHOLDER SELECTOR ---
        result_element = driver.find_element(By.CSS_SELECTOR, "div.match-result-text")
        result = result_element.text
    except NoSuchElementException:
        pass # No result yet, keep default
    
    return {
        "bookmaker": bookmaker_data,
        "fancy": fancy_data,
        "sessions": session_data,
        "result": result,
        "last_updated": time.time()
    }

# --- Login Function ---
def login_demo_account(driver):
    """Logs into the demo account."""
    try:
        login_button_xpath = "//*[@id='btnLoginb2cLogin']"
        wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
        login_button = wait.until(EC.element_to_be_clickable((By.XPATH, login_button_xpath)))
        login_button.click()

        demo_acc_button_xpath = "//*[@id='content1']/div[2]/button"
        demo_acc_button = wait.until(EC.element_to_be_clickable((By.XPATH, demo_acc_button_xpath)))
        demo_acc_button.click()

        print("Clicked demo login button.")
    except (TimeoutException, NoSuchElementException) as e:
        print(f"Could not find or click demo login button: {e}")


# --- Main Application Loop ---

def main_loop():
    """Main scraping loop."""
    driver = setup_driver()
    if not driver:
        print("Failed to start driver. Exiting.")
        return

    try:
        while True:
            print("="*30)
            print(f"Starting new scrape cycle at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            all_live_match_data = []
            
            try:
                driver.get(TARGET_URL)

                banner_close_xpath = "//*[@id='content']/div/button"
                try:
                    banner_close_btn = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, banner_close_xpath))
                    )
                    banner_close_btn.click()
                    print("Closed banner popup.")
                except (TimeoutException, NoSuchElementException):
                    pass # No banner appeared

                # trying to log in into demo account
                login_demo_account(driver)

                login_success = WebDriverWait(driver, WEB_DRIVER_TIMEOUT).until(
                    EC.presence_of_element_located((By.XPATH, "//*[@id='btnExpoRightMenu']"))
                )

                if login_success:
                    print("Login successful.")
                    live_match_count = get_live_match_count(driver)
                    
                    if live_match_count == 0:
                        print("No live matches found. Retrying...")
                        write_to_json([], JSON_OUTPUT_FILE) # Write empty list
                        time.sleep(SCRAPE_INTERVAL_SECONDS)
                        continue

                    print(f"Found {live_match_count} live matches. Processing...")

                    # Loop from 0 to count-1
                    for i in range(live_match_count):
                        print(f"Processing match {i + 1} of {live_match_count}...")
                        
                        # Re-find the match table to avoid StaleElementReferenceException
                        match_table_xpath = "//*[@id='root']/body/div[6]/div[2]/div[2]/div[2]/table/tbody"
                        wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
                        
                        try:
                            tbody = wait.until(EC.presence_of_element_located((By.XPATH, match_table_xpath)))
                            # Find all 'tr' elements that contain a 'livenownew' class
                            live_rows = tbody.find_elements(By.XPATH, ".//tr[.//div[contains(@class, 'livenownew')]]")
                            
                            if i >= len(live_rows):
                                print("Match index out of bounds, list may have changed. Restarting cycle.")
                                break
                                
                            row_to_click = live_rows[i]
                            
                            # Get teams text *before* clicking
                            teams_text = row_to_click.find_element(By.CSS_SELECTOR, ".event-title").text
                            # Clean up teams text: "28 Oct 08:00 | Western Australia v South Australia" -> "Western Australia v South Australia"
                            if "|" in teams_text:
                                teams = teams_text.split("|", 1)[-1].strip()
                            else:
                                teams = teams_text.strip()
                            
                            # Click the event info cell to navigate
                            clickable_cell = row_to_click.find_element(By.CSS_SELECTOR, "td.eventInfo")
                            clickable_cell.click()
                            
                            # Wait for URL to change to the event page
                            wait.until(EC.url_contains("/event/"))
                            
                            # Get Match ID from URL
                            current_url = driver.current_url
                            match_id = current_url.split('/')[-1].split('?')[0] # Get last part of URL, remove queries
                            
                            print(f"Scraping data for: {teams} (ID: {match_id})")

                            # Scrape the detailed data
                            live_data = get_live_match_data(driver)
                            live_data["match_id"] = match_id
                            live_data["teams"] = teams
                            all_live_match_data.append(live_data)

                        except (StaleElementReferenceException, TimeoutException, NoSuchElementException) as e:
                            print(f"Error processing match {i + 1}: {e}. Skipping.")
                        
                        # Go back to the main URL to process the next match
                        print("Navigating back to match list...")
                        driver.get(TARGET_URL)
                else:
                    print("Login failed or login element not found.")
            
            except Exception as e:
                print(f"Error in main scraping cycle: {e}")

            # Write all collected data to the JSON file
            write_to_json(all_live_match_data, JSON_OUTPUT_FILE)

            # Step 4: Repeat after interval
            print(f"Cycle complete. Waiting {SCRAPE_INTERVAL_SECONDS} seconds...")
            print("="*30)
            time.sleep(SCRAPE_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nScraping stopped by user.")
    except Exception as e:
        print(f"An uncaught error occurred in main loop: {e}")
    finally:
        if driver:
            print("Shutting down WebDriver.")
            driver.quit()


if __name__ == "__main__":
    main_loop()
