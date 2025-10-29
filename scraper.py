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
    global LOGGED_IN
    try:

        if not LOGGED_IN:
            banner_close_xpath = "//*[@id='content']/div/button"
            try:
                banner_close_btn = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.XPATH, banner_close_xpath))
                )
                banner_close_btn.click()
                print("Closed banner popup.")
            except (TimeoutException, NoSuchElementException):
                pass # No banner


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
        LOGGED_IN = True

    except (TimeoutException, NoSuchElementException) as e:
        print(f"Could not find or click demo login button: {e}")
        LOGGED_IN = False
    except Exception as e:
        print(f"An unexpected error occurred during login: {e}")
        LOGGED_IN = False
    finally:
        if not LOGGED_IN:
            print("Manager login failed. Retrying cycle...")
            time.sleep(5)
            login_demo_account(driver)

# --- Core Scraping Function (for worker threads) ---
def get_live_match_data(driver):
    """
    Scrapes Bookmaker, Fancy, and Session odds from the *current* page.
    Returns None if the match appears to be over (result is posted).
    """
    wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
    driver.refresh()
    try:
        # Check for match result/end
        result_element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.match-result-text")))
        if result_element and result_element.text.strip():
            print(f"Match result found ('{result_element.text}'). Stopping scrape.")
            return None
    except (TimeoutException, NoSuchElementException):
        # This is good, it means no result is posted and the match is live
        pass 

    bookmaker_data = []
    try:
        # --- FIX: Using the correct XPath for "match odds" provided by user ---
        bookmaker_xpath = "//*[@id='root']/body/div[6]/div[2]/div/div[4]/div[1]/div[2]/table/tbody"
        
        # Wait for the tbody element directly
        bookmaker_tbody = wait.until(EC.presence_of_element_located((By.XPATH, bookmaker_xpath)))
        
        # Find rows inside the tbody. Skip first row (header)
        team_rows = bookmaker_tbody.find_elements(By.XPATH, ".//tr[position() > 1 and count(td) > 1]")
        
        for row in team_rows:
            team_name = row.find_element(By.CSS_SELECTOR, "span.in-play-title").text
            back_prices = []
            # Find all 3 back buttons
            for el in row.find_elements(By.CSS_SELECTOR, "a.btn-back"):
                try:
                    price = el.find_element(By.CSS_SELECTOR, "div").text.strip()
                    size = el.find_element(By.CSS_SELECTOR, "span").text.strip()
                    if price:
                        back_prices.append({"price": price, "size": size})
                except NoSuchElementException: pass

            lay_prices = []
             # Find all 3 lay buttons
            for el in row.find_elements(By.CSS_SELECTOR, "a.btn-lay"):
                try:
                    price = el.find_element(By.CSS_SELECTOR, "div").text.strip()
                    size = el.find_element(By.CSS_SELECTOR, "span").text.strip()
                    if price:
                        lay_prices.append({"price": price, "size": size})
                except NoSuchElementException: pass
            
            if team_name: # Only append if we found a team
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
        # --- NO CHANGE: This XPath was already correct ---
        fancy_xpath = "//*[@id='root']/body/div[6]/div[2]/div/div[5]/div/div[4]/table/tbody"
        fancy_container = wait.until(EC.presence_of_element_located((By.XPATH, fancy_xpath)))
        
        # Skip header row and mobile-only rows
        market_rows = fancy_container.find_elements(By.XPATH, ".//tr[not(contains(@class, 'bet-all-new')) and not(contains(@class, 'brblumobile'))]")
        
        for row in market_rows:
            try:
                market_name = row.find_element(By.CSS_SELECTOR, "span.marketnamemobile").text.strip()
                
                lay_btn = row.find_element(By.CSS_SELECTOR, "a.btn-lay")
                no_val = lay_btn.find_element(By.CSS_SELECTOR, "div").text.strip()
                no_size = lay_btn.find_element(By.CSS_SELECTOR, "span").text.strip()

                back_btn = row.find_element(By.CSS_SELECTOR, "a.btn-back")
                yes_val = back_btn.find_element(By.CSS_SELECTOR, "div").text.strip()
                yes_size = back_btn.find_element(By.CSS_SELECTOR, "span").text.strip()
                
                # Only add if we have data
                if market_name and no_val and yes_val:
                    market_item = {
                        "name": market_name, 
                        "no_val": no_val, 
                        "no_size": no_size, 
                        "yes_val": yes_val, 
                        "yes_size": yes_size
                    }

                    if "over" in market_name.lower() or "run" in market_name.lower() or "session" in market_name.lower():
                        session_data.append(market_item)
                    else:
                        fancy_data.append(market_item)
            except (NoSuchElementException, StaleElementReferenceException):
                # This can happen if a single fancy market row is suspended or disappears
                pass # Just skip this row and continue with the next one
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
    thread_driver = setup_driver()
    if not thread_driver:
        print(f"\r[Thread-{match_id}] Failed to start driver. Exiting.")
        return

    match_url = f"{TARGET_URL}/event/4/{match_id}"
    consecutive_errors = 0

    try:
        thread_driver.get(match_url)
        
        WebDriverWait(thread_driver, WEB_DRIVER_TIMEOUT).until(
            EC.presence_of_element_located((By.ID, "root")) 
        )
        
        while True:
            try:
                live_data = get_live_match_data(thread_driver)
                
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
                thread_driver.refresh()
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
        if thread_driver:
            thread_driver.quit()
        
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
                print("Failed to start manager driver. Retrying setup...")
                time.sleep(10) # Wait before retrying setup
                driver = setup_driver()
                continue # Skip to the next loop iteration

            print("="*30)
            print(f"Starting new manager cycle at {time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            found_match_ids = set() 
            matches_to_start = [] 

            try:
                driver.get(f"{TARGET_URL}/game/4")
                # login_demo_account(driver) # Still commented out as in your original
                wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
                match_table_xpath = "//*[@id='root']/body/div[6]/div[2]/div[2]/div[2]/table/tbody"
                live_rows_xpath = ".//tr[.//div[contains(@class, 'livenownew')]]"

                # --- PASS 1: Click each match to get its ID ---
                num_matches = 0
                try:
                    wait.until(EC.presence_of_element_located((By.XPATH, match_table_xpath)))
                    live_row_elements = driver.find_elements(By.XPATH, live_rows_xpath)
                    num_matches = len(live_row_elements)
                    
                    if num_matches == 0:
                        print("No live matches found.")
                        driver.refresh()
                        continue # Skip to cleanup/next cycle
                    else:
                        print(f"Found {num_matches} live matches. Iterating to get IDs...")
                        
                except TimeoutException:
                    print("Match table not found on page load.")
                    continue # Skip to cleanup/next cycle
                
                for i in range(num_matches):
                    teams=[]
                    try:
                        # 1. Re-find all rows to ensure they are fresh after driver.back()
                        wait.until(EC.presence_of_element_located((By.XPATH, match_table_xpath)))
                        all_rows = driver.find_elements(By.XPATH, live_rows_xpath)
                        
                        if i >= len(all_rows):
                            print("Match list changed during iteration, stopping ID collection.")
                            break
                        
                        # 2. Get info from the i-th row
                        row_to_process = all_rows[i]
                        clickable_cell = row_to_process.find_element(By.CSS_SELECTOR, ".event-title")
                        
                        teams_text_raw = clickable_cell.text
                        teams = teams_text_raw.split("|", 1)[-1].strip() if "|" in teams_text_raw else teams_text_raw.strip()

                        # 3. Click the cell to navigate
                        clickable_cell.click()
                        
                        # 4. Wait for new URL and extract ID
                        wait.until(EC.url_contains("/event/"))
                        match_id = driver.current_url.split('/')[-1].split('?')[0]
                        
                        if not match_id:
                            print(f"Could not parse match_id for {teams}. Skipping.")
                        else:
                            print(f"Found ID: {match_id} for {teams}")
                            matches_to_start.append({"id": match_id, "teams": teams})
                            found_match_ids.add(match_id)
                        
                        # 5. Go back and wait for table to reload
                        driver.back()
                        wait.until(EC.presence_of_element_located((By.XPATH, match_table_xpath)))

                    except (StaleElementReferenceException, TimeoutException, NoSuchElementException) as e:
                        print(f"Error processing match at index {i} ('{teams}'): {e}.")
                        print("Stopping ID collection and retrying manager cycle.")
                        driver.get(f"{TARGET_URL}/game/4") # Reset driver state
                        matches_to_start.clear() # Discard partial data
                        found_match_ids.clear()
                        break # Break from the 'for' loop to start a fresh cycle
                
                # --- PASS 2: Start threads based on extracted data ---
                if matches_to_start:
                    print(f"Extracted {len(matches_to_start)} IDs. Starting new threads...")
                
                for match_info in matches_to_start:
                    match_id = match_info['id']
                    teams = match_info['teams']

                    with data_lock:
                        is_active = match_id in active_match_threads
                    
                    if not is_active:
                        print(f"Starting thread for: {teams} (ID: {match_id}).")
                        t = threading.Thread(
                            target=scrape_match_worker, 
                            args=(match_id, teams)
                        )
                        t.start()
                        with data_lock:
                            active_match_threads[match_id] = t
            
            except WebDriverException as e:
                print(f"Manager driver error: {e}. Restarting driver.")
                if driver: driver.quit()
                driver = setup_driver() # It will be re-checked at the start of the 'while True' loop
                if not driver:
                    print("Failed to restart manager driver. Waiting 60s.")
                    time.sleep(60)
            except Exception as e:
                print(f"Unhandled error in main manager cycle: {e}")

            # --- Cleanup Logic (Unchanged) ---
            with data_lock:
                active_ids = set(active_match_threads.keys())
            
            # Find threads for matches that are no longer in the live list
            ids_to_stop = active_ids - found_match_ids
            if ids_to_stop:
                print(f"Matches no longer in live list: {ids_to_stop}")
                # Note: This doesn't actively stop threads.
                # The worker threads should stop on their own when the match ends.

            # Find and clean up threads that have died
            dead_threads = []
            with data_lock:
                for match_id, t in active_match_threads.items():
                    if not t.is_alive():
                        dead_threads.append(match_id)
                
            for match_id in dead_threads:
                print(f"Cleaning up dead/finished thread for match ID: {match_id}")
                with data_lock:
                    if match_id in active_match_threads:
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