# /home/ubuntu/LiveFetch/scraper.py
DEPLOYED = False  # Set to True when deploying to production

import json
import time
import os
import tempfile
import threading
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as chromeOptions
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException,
)

# --- Configuration ---
TARGET_URL = "https://www.radheexch.xyz"
JSON_OUTPUT_FILE = "/app/data/live_data.json" if DEPLOYED else "./data/live_data.json"
SCRAPE_INTERVAL_SECONDS = 2  # How often each match thread scrapes
LIST_REFRESH_INTERVAL_SECONDS = 20  # How often the manager checks for *new* matches
WEB_DRIVER_TIMEOUT = 10
LOGGED_IN = False

# --- Global Thread-Safe State ---
live_data_cache = {}
active_match_threads = {}
data_lock = threading.Lock()

# --- Selenium Driver Setup ---
def setup_driver():
    """Initializes and returns a headless Selenium WebDriver."""
    try:
        options = chromeOptions() if DEPLOYED else EdgeOptions()
        if DEPLOYED:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        # options.add_argument("--window-size=1920,1080")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"
        )
        
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
    except (IOError, os.error, json.JSONDecodeError) as e:
        print(f"Error writing to JSON file {filename}: {e}")
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)

# --- Login & Navigation Functions ---
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

# --- Core Scraping Function (for worker threads) ---
def get_live_match_data(driver):
    """
    Scrapes Bookmaker, Fancy, and Session odds from the *current* page.
    Returns None if the match appears to be over (result is posted).
    """
    wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
    
    try:
        result_element = driver.find_element(By.CSS_SELECTOR, "div.match-result-text")
        if result_element and result_element.text.strip():
            print(f"Match result found ('{result_element.text}'). Stopping scrape.")
            return None
    except NoSuchElementException:
        pass

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
                except NoSuchElementException: pass

            lay_prices = []
            for el in row.find_elements(By.CSS_SELECTOR, "a.btn-lay"):
                try:
                    price = el.find_element(By.CSS_SELECTOR, "div").text.strip()
                    size = el.find_element(By.CSS_SELECTOR, "span").text.strip()
                    if price:
                        lay_prices.append({"price": price, "size": size})
                except NoSuchElementException: pass
            
            bookmaker_data.append({
                "team_name": team_name,
                "back": back_prices,
                "lay": lay_prices
            })
    except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
        print(f"Could not scrape Bookmaker data: {e}")

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

# --- Threading Functions ---
def scrape_match_worker(match_id, teams):
    """
    A dedicated thread function to scrape a single match continuously.
    It runs in its own driver for isolation and parallelism.
    """
    print(f"\r[Thread-{match_id}] Starting for: {teams}")
    driver = setup_driver()
    if not driver:
        print(f"\r[Thread-{match_id}] Failed to start driver. Exiting.")
        return

    match_url = f"{TARGET_URL}/event/{match_id}"
    consecutive_errors = 0

    try:
        driver.get(match_url)
        
        while True:
            try:
                live_data = get_live_match_data(driver)
                
                if live_data is None:
                    print(f"\r[Thread-{match_id}] Match finished or result posted. Stopping.")
                    break
                
                live_data["match_id"] = match_id
                live_data["teams"] = teams
                
                with data_lock:
                    live_data_cache[match_id] = live_data
                
                consecutive_errors = 0
                
            except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
                print(f"\r[Thread-{match_id}] Error scraping details: {e}. Retrying...")
                consecutive_errors += 1
                driver.refresh()
            except WebDriverException as e:
                if "invalid session id" in str(e).lower():
                    print(f"\r[Thread-{match_id}] Invalid Session ID. Driver crashed. Exiting thread.")
                    break
                else:
                    print(f"\r[Thread-{match_id}] WebDriverException: {e}. Exiting thread.")
                    break
            except Exception as e:
                print(f"\r[Thread-{match_id}] Unhandled error: {e}. Exiting thread.")
                break

            if consecutive_errors > 5:
                print(f"\r[Thread-{match_id}] Too many consecutive errors. Exiting thread.")
                break
                
            time.sleep(SCRAPE_INTERVAL_SECONDS)
            
    except Exception as e:
        print(f"[Thread-{match_id}] Critical error in worker: {e}")
    finally:
        if driver:
            driver.quit()
        
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
                all_live_data = list(live_data_cache.values())
            
            write_to_json(all_live_data, JSON_OUTPUT_FILE)
            
        except Exception as e:
            print(f"[DataWriter] Error writing JSON: {e}")
            
        time.sleep(SCRAPE_INTERVAL_SECONDS)

# --- Main Application Manager ---
def main_manager():
    global LOGGED_IN
    """
    Main loop to manage scraping threads.
    It finds new matches, starts worker threads for them,
    and cleans up dead threads.
    """
    print("Starting data writer thread...")
    writer_thread = threading.Thread(target=write_data_loop, daemon=True)
    writer_thread.start()

    driver = setup_driver()

    try:
        while True:
            if not driver:
                print("Failed to start manager driver. Exiting.")
                return
            print("="*30)
            print(f"Starting new manager cycle at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            found_match_ids = set()
            try:
                # Always go to the main page
                driver.get(f"{TARGET_URL}/game/4")

                if not LOGGED_IN:
                    # Close banner popup if it exists, to get to login
                    banner_close_xpath = "//*[@id='content']/div/button"
                    try:
                        banner_close_btn = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, banner_close_xpath))
                        )
                        banner_close_btn.click()
                        print("Closed banner popup.")
                    except (TimeoutException, NoSuchElementException):
                        pass # No banner

                    print("Logging into demo account...")
                    LOGGED_IN = login_demo_account(driver)

                if not LOGGED_IN:
                    print("Manager login failed. Retrying cycle...")
                    time.sleep(10)
                    continue

                # --- This is the new, more efficient match-finding logic ---
                wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
                match_table_xpath = "//*[@id='root']/body/div[6]/div[2]/div[2]/div[2]/table/tbody"
                
                try:
                    tbody = wait.until(EC.presence_of_element_located((By.XPATH, match_table_xpath)))
                    # Find rows that are live
                    live_rows = tbody.find_elements(By.XPATH, ".//tr[.//div[contains(@class, 'livenownew')]]")
                    
                    if not live_rows:
                        print("No live matches found.")
                    else:
                        print(f"Found {len(live_rows)} live matches. Checking for new ones...")


                    for row in live_rows:
                        try:
                            teams_text_raw = row.find_element(By.CSS_SELECTOR, ".event-title").text
                            teams = teams_text_raw.split("|", 1)[-1].strip() if "|" in teams_text_raw else teams_text_raw.strip()

                            clickable_cell = row.find_element(By.CSS_SELECTOR, ".event-title")
                            clickable_cell.click()
                            
                            wait.until(EC.url_contains("/event/"))
                            match_id = driver.current_url.split('/')[-1].split('?')[0]
                            
                            if not match_id:
                                continue

                            found_match_ids.add(match_id)

                            with data_lock:
                                is_active = match_id in active_match_threads
                            
                            if not is_active:
                                print(f"Found new match: {teams} (ID: {match_id}). Starting thread.")
                                t = threading.Thread(target=scrape_match_worker, args=(match_id, teams))
                                t.start()
                                with data_lock:
                                    active_match_threads[match_id] = t
                        
                        except (StaleElementReferenceException, NoSuchElementException) as e:
                            print(f"Error processing a match row: {e}. Skipping row.")

                except (TimeoutException, NoSuchElementException):
                    print("Could not find match table to count live matches.")
                    # Invalidate login state to force re-login next cycle
                    LOGGED_IN = False
            
            except WebDriverException as e:
                print(f"Manager driver error: {e}. Restarting driver.")
                LOGGED_IN = False # Force re-login
                if driver: driver.quit()
                driver = setup_driver()
                if not driver:
                    print("Failed to restart manager driver. Waiting 60s.")
                    time.sleep(60)
                    driver = None # Ensure it retries setup
            except Exception as e:
                print(f"Error in main manager cycle: {e}")
                LOGGED_IN = False # Force re-login

            # Clean up dead threads
            with data_lock:
                active_ids = set(active_match_threads.keys())
            
            # Find threads that are no longer live
            ids_to_stop = active_ids - found_match_ids
            if ids_to_stop:
                print(f"Matches no longer in list, stopping threads for: {ids_to_stop}")
                # Note: The threads will stop themselves once they see a "result"
                # This is just a log. We also clean up threads that died.

            dead_threads = []
            with data_lock:
                for match_id, t in active_match_threads.items():
                    if not t.is_alive():
                        dead_threads.append(match_id)
                
            for match_id in dead_threads:
                print(f"Cleaning up dead/finished thread for match ID: {match_id}")
                with data_lock:
                    del active_match_threads[match_id]
                    if match_id in live_data_cache:
                         del live_data_cache[match_id]

            with data_lock:
                active_count = len(active_match_threads)

            print(f"Manager cycle complete. Active threads: {active_count}. Waiting {LIST_REFRESH_INTERVAL_SECONDS}s...")
            time.sleep(LIST_REFRESH_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("\nShutting down scraper manager...")
    finally:
        if driver:
            print("Shutting down manager WebDriver.")
            driver.quit()
        print("Exiting.")
        sys.exit(0)


if __name__ == "__main__":
    main_manager()