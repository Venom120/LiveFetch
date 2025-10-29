# /home/ubuntu/LiveFetch/scraper.py
import json
import time
import os
import tempfile
import threading
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException,
)

# --- Configuration ---
DEPLOYED = False  # Set to True for production/Docker
TARGET_URL = "https://www.radheexch.xyz"
JSON_OUTPUT_FILE = "/app/data/live_data.json" if DEPLOYED else "./data/live_data.json"
SCRAPE_INTERVAL_SECONDS = 2  # How often each match thread scrapes
LIST_REFRESH_INTERVAL_SECONDS = 20  # How often to check for *new* matches
WEB_DRIVER_TIMEOUT = 10

# --- Global Thread-Safe State ---
live_data_cache = {}
active_match_threads = {}
data_lock = threading.Lock()

# --- Selenium Driver Setup ---
def setup_driver():
    """Initializes and returns a headless Selenium WebDriver."""
    try:
        options = ChromeOptions()
        # options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        # options.add_argument("--window-size=1920,1080")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"
        )
        
        # --- DNS Configuration ---
        local_state = {
            "dns_over_https.mode": "secure",
            "dns_over_https.templates": "https://chrome.cloudflare-dns.com/dns-query",
        }
        options.add_experimental_option('localState', local_state)

        driver = webdriver.Chrome(options=options)
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
        if not DEPLOYED:
            print(f"Successfully updated {filename}") # Too noisy for production
    except (IOError, os.error, json.JSONDecodeError) as e:
        print(f"Error writing to JSON file {filename}: {e}")
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)

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
        
        # Wait for login to complete by checking for a known post-login element
        WebDriverWait(driver, WEB_DRIVER_TIMEOUT).until(
            EC.presence_of_element_located((By.XPATH, "//*[@id='btnExpoRightMenu']"))
        )
        print("Login successful.")
        return True
    except (TimeoutException, NoSuchElementException) as e:
        print(f"Could not find or click demo login button: {e}")
        return False
    except Exception as e:
        print(f"An unexpected error occurred during login: {e}")
        return False

# --- Core Scraping Functions ---
def get_live_match_data(driver):
    """
    Scrapes Bookmaker, Fancy, and Session odds from the *current* page.
    Returns None if the match appears to be over (result is posted).
    """
    wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
    
    # --- Scrape Result ---
    try:
        result_element = driver.find_element(By.CSS_SELECTOR, "div.match-result-text")
        if result_element and result_element.text.strip():
            print(f"Match result found ('{result_element.text}'). Stopping scrape.")
            return None  # Signal to stop scraping
    except NoSuchElementException:
        pass  # No result yet, continue

    # --- Scrape Bookmaker ---
    bookmaker_data = []
    try:
        bookmaker_xpath = "//*[@id='root']/body/div[6]/div[2]/div/div[4]/div[2]"
        bookmaker_container = wait.until(EC.presence_of_element_located((By.XPATH, bookmaker_xpath)))
        team_rows = bookmaker_container.find_elements(By.XPATH, ".//table/tbody/tr[position() > 1 and count(td) > 1]")
        
        for row in team_rows:
            team_name = row.find_element(By.CSS_SELECTOR, "span.in-play-title").text
            
            back_prices = []
            for el in row.find_elements(By.CSS_SELECTOR, "a.btn-back"):
                try:
                    price = el.find_element(By.CSS_SELECTOR, "div").text.strip()
                    size = el.find_element(By.CSS_SELECTOR, "span").text.strip()
                    if price:
                        back_prices.append({"price": price, "size": size})
                except NoSuchElementException:
                    pass  # Handle missing size span

            lay_prices = []
            for el in row.find_elements(By.CSS_SELECTOR, "a.btn-lay"):
                try:
                    price = el.find_element(By.CSS_SELECTOR, "div").text.strip()
                    size = el.find_element(By.CSS_SELECTOR, "span").text.strip()
                    if price:
                        lay_prices.append({"price": price, "size": size})
                except NoSuchElementException:
                    pass  # Handle missing size span
            
            bookmaker_data.append({
                "team_name": team_name,
                "back": back_prices,
                "lay": lay_prices
            })

    except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
        print(f"Could not scrape Bookmaker data: {e}")

    # --- Scrape Fancy & Sessions ---
    fancy_data = []
    session_data = []
    try:
        fancy_xpath = "//*[@id='root']/body/div[6]/div[2]/div/div[5]/div/div[4]/table/tbody"
        fancy_container = wait.until(EC.presence_of_element_located((By.XPATH, fancy_xpath)))
        market_rows = fancy_container.find_elements(By.XPATH, ".//tr[not(contains(@class, 'bet-all-new')) and not(contains(@class, 'brblumobile'))]")
        
        for row in market_rows:
            market_name = row.find_element(By.CSS_SELECTOR, "span.marketnamemobile").text.strip()
            
            lay_btn = row.find_element(By.CSS_SELECTOR, "a.btn-lay")
            no_val = lay_btn.find_element(By.CSS_SELECTOR, "div").text.strip()
            no_size = lay_btn.find_element(By.CSS_SELECTOR, "span").text.strip()

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
                
    except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
        print(f"Could not scrape Fancy/Session data: {e}")

    return {
        "bookmaker": bookmaker_data,
        "fancy": fancy_data,
        "sessions": session_data,
        "last_updated": time.time()
    }

def get_live_match_list(driver):
    """
    Scans the main page for all live matches and returns a dict of
    {match_id: teams_text}. Does not navigate away from the page.
    """
    matches = {}
    try:
        match_table_xpath = "//*[@id='root']/body/div[6]/div[2]/div[2]/div[2]/table/tbody"
        wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
        tbody = wait.until(EC.presence_of_element_located((By.XPATH, match_table_xpath)))

        live_rows = tbody.find_elements(By.CLASS_NAME, "livenownew")

        for row in live_rows:
            try:
                teams_text_raw = row.find_element(By.CSS_SELECTOR, ".event-title").text
                teams = teams_text_raw.split("|", 1)[-1].strip() if "|" in teams_text_raw else teams_text_raw.strip()
                
                # Find the link element and extract the match ID from its href
                link_element = row.find_element(By.CSS_SELECTOR, "td.eventInfo a[href*='/event/']")
                href = link_element.get_attribute('href')
                if href:
                    match_id = href.split('/')[-1].split('?')[0]
                else:
                    match_id = None
                
                if match_id and teams:
                    matches[match_id] = teams
            except (NoSuchElementException, StaleElementReferenceException) as e:
                print(f"Error parsing a match row: {e}")
                
    except (TimeoutException, NoSuchElementException) as e:
        print(f"Could not find or parse match table: {e}")
    except Exception as e:
        print(f"Error in get_live_match_list: {e}")
        
    return matches

# --- Threading Functions ---
def scrape_match_worker(match_id, teams):
    """
    A dedicated thread function to scrape a single match continuously.
    """
    print(f"[Thread-{match_id}] Starting for: {teams}")
    driver = setup_driver()
    if not driver:
        print(f"[Thread-{match_id}] Failed to start driver. Exiting.")
        return

    match_url = f"{TARGET_URL}/event/{match_id}"
    consecutive_errors = 0

    try:
        driver.get(match_url)
        
        while True:
            try:
                live_data = get_live_match_data(driver)
                
                if live_data is None:
                    print(f"[Thread-{match_id}] Match finished or result posted. Stopping.")
                    break
                
                live_data["match_id"] = match_id
                live_data["teams"] = teams
                
                with data_lock:
                    live_data_cache[match_id] = live_data
                
                consecutive_errors = 0  # Reset error count on success
                
            except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
                print(f"[Thread-{match_id}] Error scraping details: {e}. Retrying...")
                consecutive_errors += 1
                driver.refresh() # Try refreshing the page
            except WebDriverException as e:
                if "invalid session id" in str(e).lower():
                    print(f"[Thread-{match_id}] Invalid Session ID. Driver crashed. Exiting thread.")
                    break # Exit loop, driver is dead
                else:
                    print(f"[Thread-{match_id}] WebDriverException: {e}. Exiting thread.")
                    break
            except Exception as e:
                print(f"[Thread-{match_id}] Unhandled error: {e}. Exiting thread.")
                break # Exit loop for safety

            if consecutive_errors > 5:
                print(f"[Thread-{match_id}] Too many consecutive errors. Exiting thread.")
                break
                
            time.sleep(SCRAPE_INTERVAL_SECONDS)
            
    except Exception as e:
        print(f"[Thread-{match_id}] Critical error in worker: {e}")
    finally:
        if driver:
            driver.quit()
        
        # Clean up global state
        with data_lock:
            if match_id in live_data_cache:
                del live_data_cache[match_id]
            if match_id in active_match_threads:
                del active_match_threads[match_id]
                
        print(f"[Thread-{match_id}] Cleaned up and stopped.")

def write_data_loop():
    """
    A simple thread that periodically writes the contents of
    live_data_cache to the JSON file.
    """
    while True:
        try:
            with data_lock:
                # Create a snapshot of the data to write
                all_live_data = list(live_data_cache.values())
            
            write_to_json(all_live_data, JSON_OUTPUT_FILE)
            
        except Exception as e:
            print(f"[DataWriter] Error writing JSON: {e}")
            
        time.sleep(SCRAPE_INTERVAL_SECONDS)

# --- Main Application Manager ---
def main_manager():
    """
    Main loop to manage scraping threads.
    It finds new matches, starts worker threads for them,
    and cleans up dead threads.
    """
    print("Starting data writer thread...")
    writer_thread = threading.Thread(target=write_data_loop, daemon=True)
    writer_thread.start()

    driver = None
    while True:
        try:
            if not driver:
                print("Setting up main manager driver...")
                driver = setup_driver()
                if not driver:
                    print("Failed to start manager driver. Retrying in 60s...")
                    time.sleep(60)
                    continue
                
                driver.get(f"{TARGET_URL}/game/4")
                
                # Close banner popup if it appears
                try:
                    banner_close_xpath = "//*[@id='content']/div/button"
                    banner_close_btn = WebDriverWait(driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, banner_close_xpath))
                    )
                    banner_close_btn.click()
                    print("Closed banner popup.")
                except (TimeoutException, NoSuchElementException):
                    pass # No banner

                if not login_demo_account(driver):
                    print("Login failed. Retrying...")
                    driver.quit()
                    driver = None
                    time.sleep(10)
                    continue

            print(f"Checking for live matches...")
            live_matches = get_live_match_list(driver)
            
            if not live_matches:
                print("No live matches found. Retrying...")
                driver.refresh() # Refresh to see if they appear
            
            current_match_ids = set(live_matches.keys())
            
            with data_lock:
                active_match_ids = set(active_match_threads.keys())
            
            matches_to_start = current_match_ids - active_match_ids
            
            for match_id in matches_to_start:
                teams = live_matches[match_id]
                t = threading.Thread(target=scrape_match_worker, args=(match_id, teams))
                t.start()
                with data_lock:
                    active_match_threads[match_id] = t
            
            print(f"Manager: {len(current_match_ids)} matches live. {len(matches_to_start)} new. {len(active_match_ids)} being tracked.")

        except WebDriverException as e:
            print(f"Main manager driver error: {e}. Restarting driver.")
            if driver:
                driver.quit()
            driver = None
        except Exception as e:
            print(f"Error in main manager loop: {e}")
        
        time.sleep(LIST_REFRESH_INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        main_manager()
    except KeyboardInterrupt:
        print("\nShutting down scraper manager...")
        sys.exit(0)