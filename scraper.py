# /home/ubuntu/LiveFetch/scraper.py

# --- !! IMPORTANT !! ---
# DEPLOYED setting is now controlled by settings.json
# ---

import json
import time
import os
import tempfile
import threading
import sys
import signal
import logging
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
with open('settings.json', 'r') as f:
    config = json.load(f)

DEPLOYED = config['DEFAULT']['DEPLOYED']
TARGET_URL = config['Scraper']['TARGET_URL']
SCRAPE_INTERVAL_SECONDS = config['Scraper']['SCRAPE_INTERVAL_SECONDS']
LIST_REFRESH_INTERVAL_SECONDS = config['Scraper']['LIST_REFRESH_INTERVAL_SECONDS']
WEB_DRIVER_TIMEOUT = config['Scraper']['WEB_DRIVER_TIMEOUT']
HEADLESS = config['Scraper']['HEADLESS']

# Determine JSON output file path based on DEPLOYED flag
if DEPLOYED:
    data_dir = config['Paths']['DEPLOYED_DATA_DIR']
else:
    data_dir = config['Paths']['DATA_DIR']
JSON_OUTPUT_FILE = os.path.join(data_dir, config['Paths']['JSON_FILE_NAME'])


# --- Global Thread-Safe State ---
live_data_cache = {}
active_match_threads = {}
match_stop_events = {}
data_lock = threading.Lock()
main_shutdown_event = threading.Event()

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- Shutdown Handler ---
def shutdown_handler(sig, frame):
    """Gracefully handle SIGINT (Ctrl+C) and SIGTERM (from Docker)."""
    logging.info("Shutdown signal received. Telling all threads to stop...")
    main_shutdown_event.set()

# --- Selenium Driver Setup ---
def setup_driver():
    """Initializes and returns a headless Selenium WebDriver."""
    try:
        options = chromeOptions() if DEPLOYED else EdgeOptions()
        if HEADLESS:
            options.add_argument("--headless") # Use "new" headless mode
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage") # Crucial for Docker
        options.add_argument("--disable-gpu")
        # options.add_argument("--window-size=1920,1080")
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"
        )
        
        # Add experimental options for DNS over HTTPS (as in original)
        local_state = {
            "dns_over_https.mode": "secure",
            "dns_over_https.templates": "https://chrome.cloudflare-dns.com/dns-query",
        }
        options.add_experimental_option('localState', local_state)

        driver = webdriver.Chrome(options=options) if DEPLOYED else webdriver.Edge(options=options) # type: ignore
            
        driver.set_page_load_timeout(WEB_DRIVER_TIMEOUT)
        return driver
    except WebDriverException as e:
        if "session not created" in str(e) and "DevToolsActivePort" in str(e):
            logging.error(f"Driver setup failed: Chrome/Chromium might have crashed. {e}")
        else:
            logging.error(f"Error initializing WebDriver: {e}")
        return None
    except Exception as e:
        logging.error(f"Error initializing WebDriver: {e}")
        return None

# --- JSON File Handling ---
def write_to_json(data, filename):
    """Atomically writes data to a JSON file."""
    temp_path = None
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        temp_dir = os.path.dirname(filename)
        # Use a unique temp file name
        temp_fd, temp_path = tempfile.mkstemp(dir=temp_dir, text=True)
        
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as temp_f:
            json.dump(data, temp_f, indent=4)
        
        os.replace(temp_path, filename)
        temp_path = None # Avoid double deletion
    except (IOError, os.error, json.JSONDecodeError) as e:
        logging.error(f"Error writing to JSON file {filename}: {e}")
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError as e:
                logging.error(f"Error removing temp file {temp_path}: {e}")

# --- Core Scraping Function (for worker threads) ---
def get_live_match_data(driver):
    """
    Scrapes Bookmaker, Fancy, and Session odds from the *current* page.
    Returns None if the match appears to be over (result is posted).
    Does NOT refresh the driver; the worker loop manages refreshes.
    """
    wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
    try:
        # Check for match result text, which indicates the match is over
        result_element = driver.find_element(By.CSS_SELECTOR, "div.match-result-text")
        if result_element and result_element.text.strip():
            logging.info(f"Match result found ('{result_element.text}'). Stopping scrape.")
            return None
    except NoSuchElementException:
        pass # No result found, match is live

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
        logging.warning(f"Could not scrape Bookmaker data: {e}")

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
        logging.warning(f"Could not scrape Fancy/Session data: {e}")

    return {
        "bookmaker": bookmaker_data,
        "fancy": fancy_data,
        "sessions": session_data,
        "last_updated": time.time()
    }

# --- Threading Functions ---
def scrape_match_worker(match_id, teams, stop_event):
    """
    A robust, resilient worker thread.
    Manages its own driver lifecycle. Restarts driver on crash.
    Stops when stop_event is set or match is finished.
    """
    logging.info(f"Starting for: {teams}")
    thread_driver = None
    match_url = f"{TARGET_URL}/event/4/{match_id}"
    consecutive_scrape_errors = 0
    consecutive_driver_errors = 0
    match_finished = False

    while not stop_event.is_set() and not match_finished:
        try:
            # --- Driver Setup Loop ---
            if not thread_driver:
                logging.info("Setting up new driver...")
                thread_driver = setup_driver()
                if not thread_driver:
                    consecutive_driver_errors += 1
                    logging.error(f"Failed to setup driver. Attempt {consecutive_driver_errors}/5.")
                    if consecutive_driver_errors >= 5:
                        logging.critical("Failed to setup driver 5 times. Exiting thread.")
                        break # Exit outer loop, killing thread
                    time.sleep(10)
                    continue # Retry driver setup
                
                thread_driver.get(match_url)
                WebDriverWait(thread_driver, WEB_DRIVER_TIMEOUT).until(
                    EC.presence_of_element_located((By.ID, "root")) 
                )
                logging.info("Driver setup and page load successful.")
                consecutive_driver_errors = 0 # Reset driver error count

            # --- Inner Scraping Loop ---
            while not stop_event.is_set():
                try:
                    live_data = get_live_match_data(thread_driver)
                    
                    if live_data is None:
                        logging.info("Match finished or result posted. Stopping.")
                        match_finished = True
                        break # Exit inner loop
                    
                    live_data["match_id"] = match_id
                    live_data["teams"] = teams
                    
                    with data_lock:
                        live_data_cache[match_id] = live_data
                    
                    consecutive_scrape_errors = 0 # Reset scrape error count
                
                except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
                    logging.warning(f"Error scraping details: {e}. Retrying scrape...")
                    consecutive_scrape_errors += 1
                    if consecutive_scrape_errors > 5:
                        logging.error("Too many consecutive scrape errors. Breaking to restart driver.")
                        consecutive_scrape_errors = 0
                        break # Exit inner loop to force driver restart
                    thread_driver.refresh() # Try a simple refresh first

                except WebDriverException as e:
                    logging.error(f"Driver exception (e.g., crash, timeout): {e}")
                    logging.error("Breaking to restart driver.")
                    break # Exit inner loop to force driver restart
                
                except Exception as e:
                    logging.critical(f"Unhandled error in scrape loop: {e}. Breaking to restart driver.")
                    break # Exit inner loop

                # Wait for the interval, but be interruptible by stop_event
                stop_event.wait(timeout=SCRAPE_INTERVAL_SECONDS)
                
                if not stop_event.is_set() and not match_finished:
                    # Refresh the page *before* the next scrape
                    try:
                        thread_driver.refresh()
                    except WebDriverException as e:
                        logging.error(f"Driver exception on refresh: {e}. Breaking to restart driver.")
                        break # Exit inner loop

        except Exception as e:
            logging.critical(f"Unhandled error in outer worker loop: {e}")
        
        finally:
            # This block executes when the inner loop breaks (e.g., driver crash)
            if thread_driver:
                logging.info("Quitting current driver instance.")
                thread_driver.quit()
                thread_driver = None
            if not stop_event.is_set() and not match_finished:
                logging.info("Waiting 5s before creating new driver...")
                time.sleep(5) # Wait a bit before restarting the driver

    # --- Thread Cleanup ---
    logging.info(f"Cleaned up and stopped (Match Finished: {match_finished}).")
    with data_lock:
        if match_id in live_data_cache:
            del live_data_cache[match_id]
        if match_id in active_match_threads:
            del active_match_threads[match_id]
        if match_id in match_stop_events:
            del match_stop_events[match_id]

def write_data_loop():
    """
    A simple thread that periodically writes the contents of
    live_data_cache to the JSON file.
    """
    logging.info("Data writer started.")
    while not main_shutdown_event.is_set():
        try:
            with data_lock:
                # Create a snapshot of the data to write
                all_live_data = list(live_data_cache.values())
            
            write_to_json(all_live_data, JSON_OUTPUT_FILE)
            
        except Exception as e:
            logging.error(f"[DataWriter] Error writing JSON: {e}")
        
        # Wait for the interval, but be interruptible
        main_shutdown_event.wait(timeout=SCRAPE_INTERVAL_SECONDS)
    logging.info("Data writer stopped.")

# --- Main Application Manager ---
def main_manager():
    """
    Main loop to manage scraping threads.
    It finds new matches, starts worker threads for them,
    and cleans up dead/stale threads.
    """
    manager_driver = None
    
    while not main_shutdown_event.is_set():
        try:
            if not manager_driver:
                logging.info("Starting manager driver...")
                manager_driver = setup_driver()
                if not manager_driver:
                    logging.critical("Failed to start manager driver. Retrying in 60s.")
                    main_shutdown_event.wait(timeout=60)
                    continue

            logging.info("="*30)
            logging.info(f"Starting new manager cycle")
            
            found_match_ids = set() 
            matches_to_start = [] 

            manager_driver.get(f"{TARGET_URL}/game/4")

            if main_shutdown_event.is_set():
                break

            # --- Find Live Matches ---
            wait = WebDriverWait(manager_driver, WEB_DRIVER_TIMEOUT)
            match_table_xpath = "//*[@id='root']/body/div[6]/div[2]/div[2]/div[2]/table/tbody"
            live_rows_xpath = ".//tr[.//div[contains(@class, 'livenownew')]]"

            try:
                # --- PASS 1: Get the *count* of matches first ---
                tbody = wait.until(EC.presence_of_element_located((By.XPATH, match_table_xpath)))
                live_rows = tbody.find_elements(By.XPATH, live_rows_xpath)

                if not live_rows:
                    logging.info("No live matches found.")
                else:
                    logging.info(f"Found {len(live_rows)} matches. Iterating by index...")

                for i in range(len(live_rows)):
                    try:
                        tbody = wait.until(EC.presence_of_element_located((By.XPATH, match_table_xpath)))
                        live_rows = tbody.find_elements(By.XPATH, live_rows_xpath)
                        
                        # Handle race condition if list changes mid-iteration
                        if i >= len(live_rows):
                            logging.warning("Match list changed mid-iteration. Stopping list extraction.")
                            break
                            
                        # 2. Get the i-th row
                        clickable_cell = live_rows[i].find_element(By.CSS_SELECTOR, ".event-title")

                        teams_text_raw = clickable_cell.text
                        teams = teams_text_raw.split("|", 1)[-1].strip() if "|" in teams_text_raw else teams_text_raw.strip()

                        # 3. Click the cell to navigate
                        clickable_cell.click()
                        
                        # 4. Wait for new URL and extract ID
                        wait.until(EC.url_contains("/event/"))
                        match_id = manager_driver.current_url.split('/')[-1].split('?')[0]
                        
                        if not match_id:
                            logging.warning(f"Could not parse match_id for {teams}. Skipping.")
                        else:
                            logging.info(f"Found ID: {match_id} for {teams}")
                            matches_to_start.append({"id": match_id, "teams": teams})
                            found_match_ids.add(match_id)
                        
                        # 5. Go back to the list page (using .get() is often more stable than .back())
                        manager_driver.get(f"{TARGET_URL}/game/4")

                    except (StaleElementReferenceException, NoSuchElementException, TimeoutException) as e:
                        logging.warning(f"Error processing match index {i}: {e}. Skipping.")
                        # Reset driver state by going back to the list page
                        try:
                            manager_driver.get(f"{TARGET_URL}/game/4")
                        except Exception as e_nav:
                            logging.error(f"Failed to navigate back to list page after error: {e_nav}. Breaking cycle.")
                            break # Critical failure, exit the for loop
                        continue # Continue to the next 'i'
                
                # --- PASS 2: Start new threads ---
                logging.info(f"Extracted {len(matches_to_start)} live matches. Checking for new threads to start...")
                for match_info in matches_to_start:
                    match_id = match_info['id']
                    teams = match_info['teams']

                    with data_lock:
                        is_active = match_id in active_match_threads
                    
                    if not is_active:
                        logging.info(f"Found new match: {teams} (ID: {match_id}). Starting thread.")
                        stop_event = threading.Event()
                        
                        t = threading.Thread(
                            target=scrape_match_worker, 
                            args=(match_id, teams, stop_event),
                            name=f"Worker-{match_id}" # Set thread name for logging
                        )
                        t.start()
                        with data_lock:
                            active_match_threads[match_id] = t
                            match_stop_events[match_id] = stop_event
                
            except (TimeoutException, NoSuchElementException):
                logging.warning("Could not find match table to list live matches.")
            except WebDriverException as e:
                logging.error(f"Manager driver error: {e}. Restarting driver.")
                if manager_driver: manager_driver.quit()
                manager_driver = None
                continue # Skip to next manager cycle
            except Exception as e:
                logging.error(f"Error in main manager cycle: {e}")

            # --- Cleanup Logic ---
            
            # --- Stop Stale Threads ---
            # Find threads for matches that are *no longer* in the live list
            with data_lock:
                active_ids = set(active_match_threads.keys())
            
            ids_to_stop = active_ids - found_match_ids
            if ids_to_stop:
                logging.info(f"Matches no longer in list. Signaling threads to stop: {ids_to_stop}")
                with data_lock:
                    for match_id in ids_to_stop:
                        if match_id in match_stop_events:
                            match_stop_events[match_id].set() # <-- Signal thread to stop

            # --- Clean Up Dead Threads ---
            dead_threads = []
            with data_lock:
                for match_id, t in list(active_match_threads.items()):
                    if not t.is_alive():
                        dead_threads.append(match_id)
                
            for match_id in dead_threads:
                logging.info(f"Cleaning up dead/finished thread entry for match ID: {match_id}")
                with data_lock:
                    # Thread is already dead, just clean up dicts
                    if match_id in active_match_threads:
                        del active_match_threads[match_id]
                    if match_id in live_data_cache:
                         del live_data_cache[match_id]
                    if match_id in match_stop_events:
                         del match_stop_events[match_id]

            with data_lock:
                active_count = len(active_match_threads)

            logging.info(f"Manager cycle complete. Active threads: {active_count}. Waiting {LIST_REFRESH_INTERVAL_SECONDS}s...")
            # Wait for interval, but be interruptible
            main_shutdown_event.wait(timeout=LIST_REFRESH_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            # This is caught by the signal handler, but good as a failsafe
            logging.info("\nKeyboardInterrupt caught in manager loop. Shutting down.")
            main_shutdown_event.set()
        except Exception as e:
            logging.critical(f"Unhandled exception in main_manager: {e}")
            if manager_driver:
                manager_driver.quit()
                manager_driver = None
            main_shutdown_event.wait(timeout=30) # Wait before retrying

    # --- Manager Shutdown ---
    if manager_driver:
        logging.info("Shutting down manager WebDriver.")
        manager_driver.quit()
    logging.info("Main manager loop exited.")

if __name__ == "__main__":
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    logging.info("Starting scraper manager...")
    
    writer_thread = threading.Thread(target=write_data_loop, name="DataWriter", daemon=True)
    writer_thread.start()

    try:
        main_manager()
    except Exception as e:
        logging.critical(f"Main manager crashed unexpectedly: {e}")
    finally:
        logging.info("Main manager has exited. Initiating final shutdown.")
        main_shutdown_event.set()
        
        # Give worker threads a moment to clean up
        logging.info("Waiting for data writer to finish...")
        writer_thread.join(timeout=5.0) # Wait for writer to finish last write
        
        logging.info("Shutdown complete.")
        sys.exit(0)
