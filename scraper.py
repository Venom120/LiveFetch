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
try:
    with open('settings.json', 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    print("FATAL ERROR: settings.json not found. Please create it.", file=sys.stderr)
    sys.exit(1)
except json.JSONDecodeError:
    print("FATAL ERROR: settings.json is not valid JSON.", file=sys.stderr)
    sys.exit(1)

DEPLOYED = config.get('DEFAULT', {}).get('DEPLOYED', False)
TARGET_URL = config.get('Scraper', {}).get('TARGET_URL', 'http://example.com')
SCRAPE_INTERVAL_SECONDS = config.get('Scraper', {}).get('SCRAPE_INTERVAL_SECONDS', 1)
LIST_REFRESH_INTERVAL_SECONDS = config.get('Scraper', {}).get('LIST_REFRESH_INTERVAL_SECONDS', 60)
WEB_DRIVER_TIMEOUT = config.get('Scraper', {}).get('WEB_DRIVER_TIMEOUT', 10)
HEADLESS = config.get('Scraper', {}).get('HEADLESS', True)
LEVEL = config.get('DEFAULT', {}).get('LEVEL', 'INFO').upper()
LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
if LEVEL not in LEVELS:
    LEVEL = 'INFO'

# --- Logging Setup ---
logging.basicConfig(
    level=getattr(logging, LEVEL),
    format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
if DEPLOYED:
    data_dir = config.get('Paths', {}).get('DEPLOYED_DATA_DIR', '.')
else:
    data_dir = config.get('Paths', {}).get('DATA_DIR', '.')

DATA_FILE = os.path.join(data_dir, config.get('Paths', {}).get('DATA_FILE', 'live_data.json'))


live_data_cache = {}
active_match_threads = {}
match_stop_events = {}
data_lock = threading.Lock() # Lock for live_data_cache and active_match_threads
main_shutdown_event = threading.Event()


# --- Shutdown Handler ---
def shutdown_handler(sig, frame):
    """Gracefully handle SIGINT (Ctrl+C) and SIGTERM (from Docker)."""
    # Prevent multiple shutdown signals from running
    if main_shutdown_event.is_set():
        logging.warning("Shutdown already in progress. Please be patient.")
        return
    
    logging.info("Shutdown signal received. Telling all threads to stop...")
    main_shutdown_event.set()

    with data_lock:
        # Create a list of items to avoid 'dict changed size during iteration'
        stop_events_list = list(match_stop_events.items())
    
    logging.info(f"Signaling {len(stop_events_list)} active worker thread(s)...")
    for match_id, stop_event in stop_events_list:
        stop_event.set()

# --- Selenium Driver Setup ---
def setup_driver():
    """Initializes and returns a headless Selenium WebDriver."""
    try:
        options = chromeOptions() if DEPLOYED else EdgeOptions()
        if HEADLESS:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage") # Crucial for Docker
        options.add_argument("--disable-gpu")
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


# --- Scraping Helper Functions ---
def _parse_odds_table(table_body):
    """Helper to parse a standard odds/bookmarker table body element."""
    data = []
    try:
        team_rows = table_body.find_elements(By.XPATH, ".//tr[position() > 1 and count(td) > 1]")
        
        for row in team_rows:
            try:
                team_name_element = row.find_element(By.CSS_SELECTOR, "span.in-play-title")
                team_name = team_name_element.text
                if not team_name:
                    continue
            except NoSuchElementException:
                continue

            # Find all prices and sizes in bulk
            try:
                back_price_els = row.find_elements(By.CSS_SELECTOR, "a.btn-back div")
                back_size_els = row.find_elements(By.CSS_SELECTOR, "a.btn-back span")
                back_prices = [
                    {"price": price.text.strip(), "size": size.text.strip()}
                    for price, size in zip(back_price_els, back_size_els)
                    if price.text.strip()
                ]

                lay_price_els = row.find_elements(By.CSS_SELECTOR, "a.btn-lay div")
                lay_size_els = row.find_elements(By.CSS_SELECTOR, "a.btn-lay span")
                lay_prices = [
                    {"price": price.text.strip(), "size": size.text.strip()}
                    for price, size in zip(lay_price_els, lay_size_els)
                    if price.text.strip()
                ]
            except StaleElementReferenceException:
                logging.warning("Stale element while parsing prices in odds table.")
                continue
            
            data.append({
                "team_name": team_name,
                "back": back_prices,
                "lay": lay_prices
            })
    except StaleElementReferenceException:
        logging.warning("Stale element reference while parsing odds table.")
        return []
    except Exception as e:
        logging.error(f"Error parsing odds table: {e}")
        return []
    return data

def scrape_match_odds(driver, wait):
    """
    Scrapes Match Odds (user calls it 'odds' or 'winner').
    Returns: List of scraped data, or empty list on failure.
    """
    try:
        odds_xpath = "//*[@id='root']/body/div[6]/div[2]/div/div[4]/div[1]/div[2]/table/tbody"
        odds_container = wait.until(EC.presence_of_element_located((By.XPATH, odds_xpath)))
        return _parse_odds_table(odds_container)
    except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
        logging.warning("Could not find or scrape Match Odds data.")
        return []
    except Exception as e:
        logging.error(f"Unexpected error scraping Match Odds: {e}")
        return []

def scrape_bookmarker(driver, wait):
    """
    Scrapes Bookmarker data.
    Returns: List of scraped data, or empty list on failure.
    """
    try:
        bookmarker_xpath = "//*[@id='root']/body/div[6]/div[2]/div/div[4]/div[2]"
        bookmarker_container = wait.until(EC.presence_of_element_located((By.XPATH, bookmarker_xpath)))
        
        try:
            table_body = bookmarker_container.find_element(By.XPATH, ".//div[2]/table/tbody")
        except NoSuchElementException:
            table_body = bookmarker_container.find_element(By.XPATH, ".//table/tbody")

        return _parse_odds_table(table_body)
    except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
        logging.warning("Could not find or scrape Bookmarker data.")
        return []
    except Exception as e:
        logging.error(f"Unexpected error scraping Bookmarker: {e}")
        return []

def scrape_fancy_and_sessions(driver, wait):
    """
    Scrapes Fancy and Session odds.
    Returns: (fancy_data_list, session_data_list), or ([], []) on failure.
    """
    fancy_data = []
    session_data = []
    try:
        fancy_xpath = "//*[@id='root']/body/div[6]/div[2]/div/div[5]/div/div[4]/table/tbody"
        fancy_container = wait.until(EC.presence_of_element_located((By.XPATH, fancy_xpath)))

        base_selector = "tr:not(.bet-all-new):not(.brblumobile)"
        
        # Find all elements in bulk.
        market_names = fancy_container.find_elements(By.CSS_SELECTOR, f"{base_selector} span.marketnamemobile")
        lay_vals = fancy_container.find_elements(By.CSS_SELECTOR, f"{base_selector} a.btn-lay div")
        lay_sizes = fancy_container.find_elements(By.CSS_SELECTOR, f"{base_selector} a.btn-lay span")
        back_vals = fancy_container.find_elements(By.CSS_SELECTOR, f"{base_selector} a.btn-back div")
        back_sizes = fancy_container.find_elements(By.CSS_SELECTOR, f"{base_selector} a.btn-back span")

        if not (len(market_names) == len(lay_vals) == len(lay_sizes) == len(back_vals) == len(back_sizes)):
            logging.warning(
                f"Fancy/Session element count mismatch. "
                f"Names: {len(market_names)}, LayVals: {len(lay_vals)}, BackVals: {len(back_vals)}. "
                f"This may happen during a page update. Skipping this cycle."
            )
            return [], [] 

        # Process the bulk-fetched data
        for i in range(len(market_names)):
            try:
                market_name = market_names[i].text.strip()
                if not market_name:
                    continue

                market_item = {
                    "name": market_name, 
                    "no_val": lay_vals[i].text.strip(), 
                    "no_size": lay_sizes[i].text.strip(), 
                    "yes_val": back_vals[i].text.strip(), 
                    "yes_size": back_sizes[i].text.strip()
                }

                # Simple check for session markets
                if "over" in market_name.lower() or "run" in market_name.lower():
                    session_data.append(market_item)
                else:
                    fancy_data.append(market_item)
            except (NoSuchElementException, StaleElementReferenceException):
                logging.warning(f"Could not parse a row in Fancy/Session bulk parse. Skipping row.")
                continue

    except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
        logging.warning("Could not find or scrape Fancy/Session data.")
        return [], []
    except Exception as e:
        logging.error(f"Unexpected error scraping Fancy/Session: {e}")
        return [], []
    
    return fancy_data, session_data


# --- Core Scraping Function (Replaces get_live_match_data) ---
def scrape_match_page_data(driver, is_live_match):
    """
    Scrapes data from the *current* match page.
    Differentiates logic based on whether the match is live or old.
    """
    wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
    scraped_data: dict = {}
    
    result_text = "In Progress"
    try:
        short_wait = WebDriverWait(driver, 2)
        result_element = short_wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.match-result-text"))
        )
        if result_element:
            result_text = result_element.text.strip()
            if not result_text:
                result_text = "Finished"
            if is_live_match:
                logging.info(f"Match result found: '{result_text}'")
    except (TimeoutException, NoSuchElementException):
        pass
    
    scraped_data["result"] = result_text

    if is_live_match:
        scraped_data["odds"] = scrape_match_odds(driver, wait)
        scraped_data["bookmarker"] = scrape_bookmarker(driver, wait)
        fancy_data, session_data = scrape_fancy_and_sessions(driver, wait)
        scraped_data["fancy"] = fancy_data
        scraped_data["sessions"] = session_data
    else:
        scraped_data["winner"] = scrape_match_odds(driver, wait)
        scraped_data["bookmarker"] = scrape_bookmarker(driver, wait)
    scraped_data["last_updated"] = time.time()

    return scraped_data

# --- Threading Functions ---
def scrape_match_worker(match_id, teams, stop_event):
    """
    A robust, resilient worker thread for a single LIVE match.
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
            if not thread_driver:
                logging.info("Setting up new driver...")
                thread_driver = setup_driver()
                if not thread_driver:
                    consecutive_driver_errors += 1
                    logging.error(f"Failed to setup driver. Attempt {consecutive_driver_errors}/5.")
                    if consecutive_driver_errors >= 5:
                        logging.critical("Failed to setup driver 5 times. Exiting thread.")
                        break

                    stop_event.wait(timeout=10) 
                    continue
                
                thread_driver.get(match_url)
                WebDriverWait(thread_driver, WEB_DRIVER_TIMEOUT).until(
                    EC.presence_of_element_located((By.ID, "root")) 
                )
                logging.info("Driver setup and page load successful.")
                consecutive_driver_errors = 0

            # --- Inner Scraping Loop ---
            while not stop_event.is_set():
                try:
                    live_data = scrape_match_page_data(thread_driver, is_live_match=True)
                    
                    if live_data["result"] != "In Progress":
                        logging.info(f"Match result found ('{live_data['result']}'). Stopping.")
                        match_finished = True
                        break
                    
                    live_data["match_id"] = match_id
                    live_data["teams"] = teams
                    
                    with data_lock:
                        live_data_cache[match_id] = live_data
                    
                    consecutive_scrape_errors = 0
                
                except (TimeoutException, NoSuchElementException, StaleElementReferenceException) as e:
                    logging.warning(f"Error scraping details: {e}. Retrying scrape...")
                    consecutive_scrape_errors += 1
                    if consecutive_scrape_errors > 5:
                        logging.error("Too many consecutive scrape errors. Breaking to restart driver.")
                        consecutive_scrape_errors = 0
                        break
                    try:
                        thread_driver.refresh()
                    except WebDriverException as e_refresh:
                        logging.error(f"Driver exception on refresh: {e_refresh}. Breaking to restart driver.")
                        break

                except WebDriverException as e:
                    logging.error(f"Driver exception (e.g., crash, timeout): {e}")
                    logging.error("Breaking to restart driver.")
                    break
                
                except Exception as e:
                    logging.critical(f"Unhandled error in scrape loop: {e}. Breaking to restart driver.")
                    break

                stop_event.wait(timeout=SCRAPE_INTERVAL_SECONDS)
        
        except WebDriverException as e:
            logging.critical(f"Unhandled WebDriver error in outer worker loop: {e}")

        except Exception as e:
            logging.critical(f"Unhandled error in outer worker loop: {e}")
        
        finally:
            if thread_driver:
                logging.info("Quitting current driver instance.")
                try:
                    thread_driver.quit()
                except Exception:
                    pass
                thread_driver = None
            if not stop_event.is_set() and not match_finished:
                logging.info("Waiting 5s before creating new driver...")
                stop_event.wait(timeout=5)

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
    live_data_cache to the LIVE data JSON file (CURR_JSON_FILE).
    """
    logging.info("Data writer started.")
    while not main_shutdown_event.is_set():
        try:
            with data_lock:
                all_live_data = list(live_data_cache.values())

            if len(all_live_data) == 0:
                pass
            else:
                write_to_json(all_live_data, DATA_FILE)
            
        except Exception as e:
            logging.error(f"[DataWriter] Error writing JSON: {e}")
        
        main_shutdown_event.wait(timeout=SCRAPE_INTERVAL_SECONDS)
    logging.info("Data writer stopped.")

# --- Main Application Manager ---
def main_manager():
    """
    Main loop to manage scraping.
    It finds all matches:
    - Starts worker threads for LIVE matches.
    - Cleans up dead/stale threads.
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

            # --- Find Live Matches ---
            match_table_xpath = "//*[@id='root']/body/div[6]/div[2]/div[2]/div[2]/table/tbody"
            live_rows_xpath = ".//tr[.//div[contains(@class, 'livenownew')]]"

            try:
                manager_driver.get(f"{TARGET_URL}/game/4")

                if main_shutdown_event.is_set():
                    break

                wait = WebDriverWait(manager_driver, WEB_DRIVER_TIMEOUT)

                tbody = wait.until(EC.presence_of_element_located((By.XPATH, match_table_xpath)))
                live_rows = tbody.find_elements(By.XPATH, live_rows_xpath)

                if not live_rows:
                    logging.info("No live matches found.")
                else:
                    logging.info(f"Found {len(live_rows)} matches. Iterating by index...")

                for i in range(len(live_rows)):
                    if main_shutdown_event.is_set():
                        break
                    try:
                        # 1. Re-fetch the tbody and live rows to avoid stale references
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
                        try:
                            manager_driver.get(f"{TARGET_URL}/game/4")
                        except Exception as e_nav:
                            logging.error(f"Failed to navigate back to list page after error: {e_nav}. Breaking cycle.")
                            break
                        continue
                
                if main_shutdown_event.is_set():
                    break

                # --- Start new threads ---
                logging.info(f"Extracted {len(matches_to_start)} live matches. Checking for new threads to start...")
                for match_info in matches_to_start:
                    if main_shutdown_event.is_set():
                        break
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
                        logging.info("Waiting 1s before starting next thread...")
                        main_shutdown_event.wait(timeout=1.0)
                
            except (TimeoutException, NoSuchElementException):
                logging.warning("Could not find match table to list live matches.")
            except WebDriverException as e:
                logging.error(f"Manager driver error: {e}. Restarting driver.")
                if manager_driver:
                    try:
                        manager_driver.quit()
                    except Exception as e_quit:
                        logging.warning(f"Error during driver quit, may be unstable: {e_quit}")
                manager_driver = None
                continue # Skip to next manager cycle
            except Exception as e:
                logging.error(f"Error in main manager cycle: {e}")

            # --- Cleanup Logic ---

            with data_lock:
                active_ids = set(active_match_threads.keys())
            
            ids_to_stop = active_ids - found_match_ids
            if ids_to_stop:
                logging.info(f"Matches no longer in list. Signaling threads to stop: {ids_to_stop}")
                with data_lock:
                    for match_id in ids_to_stop:
                        if match_id in match_stop_events:
                            match_stop_events[match_id].set()

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
            main_shutdown_event.wait(timeout=LIST_REFRESH_INTERVAL_SECONDS)

        except KeyboardInterrupt:
            logging.info("\nKeyboardInterrupt caught in manager loop. Shutting down.")
            main_shutdown_event.set()
        except Exception as e:
            logging.critical(f"Unhandled exception in main_manager: {e}")
            if manager_driver:
                try:
                    manager_driver.quit()
                except Exception as e_quit:
                    logging.warning(f"Error during critical driver quit: {e_quit}")
                manager_driver = None
            main_shutdown_event.wait(timeout=30)

    # --- Manager Shutdown ---
    if manager_driver:
        logging.info("Shutting down manager WebDriver.")
        try:
            manager_driver.quit()
        except Exception:
            pass
    logging.info("Main manager loop exited.")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    logging.info("Starting scraper manager...")
    logging.info(f"DEPLOYED: {DEPLOYED}")
    logging.info(f"Writing LIVE data to: {DATA_FILE}")
    
    writer_thread = threading.Thread(target=write_data_loop, name="DataWriter", daemon=True)
    writer_thread.start()

    try:
        main_manager()
    except Exception as e:
        logging.critical(f"Main manager crashed unexpectedly: {e}")
    finally:
        logging.info("Main manager has exited. Initiating final shutdown.")
        
        if not main_shutdown_event.is_set():
            logging.info("Main loop exited cleanly. Signaling all threads to stop.")
            shutdown_handler(None, None)
        
        # Wait for data writer
        logging.info("Waiting for data writer to finish...")
        writer_thread.join(timeout=max(1.0, SCRAPE_INTERVAL_SECONDS * 1.5))
    
        with data_lock:
            active_threads_list = list(active_match_threads.values())
        
        logging.info(f"Waiting for {len(active_threads_list)} worker thread(s) to join...")
        for t in active_threads_list:
            t.join(timeout=5.0)
            if t.is_alive():
                logging.warning(f"Thread {t.name} did not exit cleanly.")
        
        logging.info("Shutdown complete.")
        sys.exit(0)