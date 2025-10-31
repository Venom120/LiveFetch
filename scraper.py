import json
import time
import os
import tempfile
import threading
import sys
import signal
import logging
import requests
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

# --- General Config ---
DEPLOYED = config.get('DEFAULT', {}).get('DEPLOYED', False)
LEVEL = config.get('DEFAULT', {}).get('LEVEL', 'INFO').upper()
LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
if LEVEL not in LEVELS:
    LEVEL = 'INFO'

# --- Scraper Config ---
SCRAPE_INTERVAL_SECONDS = config.get('Scraper', {}).get('SCRAPE_INTERVAL_SECONDS', 2)
LIST_REFRESH_INTERVAL_SECONDS = config.get('Scraper', {}).get('LIST_REFRESH_INTERVAL_SECONDS', 10)
WEB_DRIVER_TIMEOUT = config.get('Scraper', {}).get('WEB_DRIVER_TIMEOUT', 10)
HEADLESS = config.get('Scraper', {}).get('HEADLESS', True)
BASE_URL = config.get('Scraper', {}).get('TARGET_URL', 'https://www.radheexch.xyz')

# --- API Config ---
AUTH_URL = config.get('Api', {}).get('AUTH_URL', 'https://api.radheexch.xyz/sso/auth/demo')
MARKET_URL = config.get('Api', {}).get('MARKET_URL', 'https://api.radheexch.xyz/marketprovider/markets/eventtype/4')
API_OP_KEY = config.get('Api', {}).get('OP_KEY', 'RDE')

# --- Path Config ---
if DEPLOYED:
    data_dir = config.get('Paths', {}).get('DEPLOYED_DATA_DIR', '.')
else:
    data_dir = config.get('Paths', {}).get('DATA_DIR', '.')
DATA_FILE = os.path.join(data_dir, config.get('Paths', {}).get('DATA_FILE', 'live_data.json'))


# --- Logging Setup ---
logging.basicConfig(
    level=getattr(logging, LEVEL),
    format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- Globals ---
live_data_cache = {}
active_match_threads = {}
match_stop_events = {}
data_lock = threading.Lock() # Lock for live_data_cache and active_match_threads
main_shutdown_event = threading.Event()
http_session = requests.Session() # Use a session for connection pooling


# --- Shutdown Handler ---
def shutdown_handler(sig, frame):
    if main_shutdown_event.is_set():
        logging.warning("Shutdown already in progress. Please be patient.")
        return
    
    logging.info("Shutdown signal received. Telling all threads to stop...")
    main_shutdown_event.set()

    with data_lock:
        stop_events_list = list(match_stop_events.items())
    
    logging.info(f"Signaling {len(stop_events_list)} active worker thread(s)...")
    for match_id, stop_event in stop_events_list:
        stop_event.set()

# --- Selenium Driver Setup ---
def setup_driver():
    try:
        options = chromeOptions() if DEPLOYED else EdgeOptions()
        if HEADLESS:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
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
    temp_path = None
    try:
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        temp_dir = os.path.dirname(filename)
        temp_fd, temp_path = tempfile.mkstemp(dir=temp_dir, text=True)
        
        with os.fdopen(temp_fd, 'w', encoding='utf-8') as temp_f:
            json.dump(data, temp_f, indent=4)
        
        os.replace(temp_path, filename)
        temp_path = None
    except (IOError, os.error, json.JSONDecodeError) as e:
        logging.error(f"Error writing to JSON file {filename}: {e}")
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError as e:
                logging.error(f"Error removing temp file {temp_path}: {e}")

# --- API & Parsing Functions ---

def get_auth_token():
    """Gets a demo auth token."""
    headers = {
        'accept': '*/*',
        'content-type': 'application/x-www-form-urlencoded;charset=UTF-8',
        'origin': BASE_URL,
        'referer': f'{BASE_URL}/',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36 Edg/141.0.0.0',
        'x-op-key': API_OP_KEY,
    }
    response = None
    try:
        response = http_session.get(AUTH_URL, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get('token')
    except requests.exceptions.RequestException as e:
        logging.error(f"Auth token request failed: {e}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Failed to decode auth token response: {response.text if response else 'No response'}")
        return None

def get_market_data(token):
    """Gets market data using the auth token."""
    if not token:
        return None
    headers = {
        'accept': '*/*',
        'authorization': f'Bearer {token}',
        'origin': BASE_URL,
        'referer': f'{BASE_URL}/',
        'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36 Edg/141.0.0.0',
        'x-op-key': API_OP_KEY,
    }
    response = None
    try:
        response = http_session.get(MARKET_URL, headers=headers, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Market data request failed: {e}")
        return None
    except json.JSONDecodeError:
        logging.error(f"Failed to decode market data response: {response.text if response else 'No response'}")
        return None

def _parse_book_string(book_string, runners_list):
    """Helper to parse the complex 'book' string from metadata."""
    runner_map = {str(runner['id']): runner['name'] for runner in runners_list}
    parsed_data = []

    try:
        data_part = book_string.split('|')[-1]
        runner_parts = data_part.split(',')
        
        for part in runner_parts:
            if not part or '~' not in part:
                continue
            
            segments = part.split('~')
            if len(segments) < 4:
                continue # Expecting ID~STATUS~BACK~LAY

            runner_id = segments[0]
            team_name = runner_map.get(runner_id, f"Unknown ID {runner_id}")
            back_str = segments[2]
            lay_str = segments[3]

            back_prices = []
            for item in back_str.split(':*'):
                if item and ':' in item:
                    try:
                        price, size = item.split(':', 1)
                        # FIX: strip trailing colons from size
                        back_prices.append({"price": price.strip(), "size": size.strip().strip(':')})
                    except ValueError:
                        pass

            lay_prices = []
            for item in lay_str.split(':*'):
                if item and ':' in item:
                    try:
                        price, size = item.split(':', 1)
                        # FIX: strip trailing colons from size
                        lay_prices.append({"price": price.strip(), "size": size.strip().strip(':')})
                    except ValueError:
                        pass
            
            parsed_data.append({
                "team_name": team_name,
                "back": back_prices,
                "lay": lay_prices
            })
        return parsed_data
    except Exception as e:
        logging.error(f"Error parsing book string '{book_string}': {e}")
        return []

def parse_market_data(market_list):
    """Parses the full API response and groups data by match."""
    matches = {}
    if not isinstance(market_list, list):
        logging.error("Market data is not a list, cannot parse.")
        return {}

    for item in market_list:
        try:
            event_id = item.get('event', {}).get('id')
            if not event_id:
                continue
            
            if event_id not in matches:
                matches[event_id] = {
                    "match_id": event_id,
                    "last_updated": time.time(),
                    "teams": item.get('event', {}).get('name', 'Unknown Match').strip(),
                    "odds": [],
                    "bookmarker": [],
                    "fancy": [],
                    "sessions": [],
                    "in_play": False,
                    "result": "Scheduled",
                }

            catalogue = item.get('catalogue', {})
            metadata = item.get('metadata', {})
            runners = catalogue.get('runners', [])
            book_string = metadata.get('book')

            if catalogue.get('inPlay'):
                matches[event_id]["in_play"] = True
                matches[event_id]["result"] = "In Progress"
            else:
                 matches[event_id]["result"] = catalogue.get('status', 'Scheduled')

            if not book_string or not runners:
                continue

            market_type = catalogue.get('marketType')
            parsed_book = _parse_book_string(book_string, runners)

            if market_type == 'MATCH_ODDS':
                matches[event_id]['odds'] = parsed_book
            elif market_type == 'BOOKMAKER':
                matches[event_id]['bookmarker'] = parsed_book

        except Exception as e:
            logging.error(f"Failed to parse market item: {e} - Item: {item}")
    
    return matches

# --- Selenium Scraping Functions ---

def scrape_fancy_and_sessions(driver, wait):
    fancy_data = []
    session_data = []
    try:
        fancy_xpath = "//*[@id='root']/body/div[6]/div[2]/div/div[5]/div/div[4]/table/tbody"
        fancy_container = wait.until(EC.presence_of_element_located((By.XPATH, fancy_xpath)))

        base_selector = "tr:not(.bet-all-new):not(.brblumobile)"
        
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


def scrape_match_page_data(driver, base_data):
    """
    Scrapes data from the *current* match page.
    ONLY scrapes Fancy/Session. Odds/Bookmarker are passed in.
    """
    wait = WebDriverWait(driver, WEB_DRIVER_TIMEOUT)
    
    # Start with the data fetched from the API
    scraped_data = base_data.copy()
    
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
    except (TimeoutException, NoSuchElementException):
        pass # Keep default "In Progress"
    
    scraped_data["result"] = result_text

    # Only scrape fancy/session. Odds/Bookmarker came from API.
    fancy_data, session_data = scrape_fancy_and_sessions(driver, wait)
    scraped_data["fancy"] = fancy_data
    scraped_data["sessions"] = session_data
    scraped_data["last_updated"] = time.time()

    return scraped_data

# --- Threading Functions ---
def scrape_match_worker(match_id, base_data, stop_event):
    logging.info(f"Starting for: {base_data['teams']}")
    thread_driver = None
    match_url = f"{BASE_URL}/event/4/{match_id}"
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
                    # Pass in the base data (odds/bookmarker) from the API
                    live_data = scrape_match_page_data(thread_driver, base_data)
                    
                    if live_data["result"] != "In Progress":
                        logging.info(f"Match result found ('{live_data['result']}'). Stopping.")
                        match_finished = True
                        break
                    
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
    logging.info("Data writer started.")
    while not main_shutdown_event.is_set():
        try:
            with data_lock:
                all_live_data = list(live_data_cache.values())

            if len(all_live_data) > 0:
                write_to_json(all_live_data, DATA_FILE)
            
        except Exception as e:
            logging.error(f"[DataWriter] Error writing JSON: {e}")
        
        main_shutdown_event.wait(timeout=SCRAPE_INTERVAL_SECONDS)
    logging.info("Data writer stopped.")

# --- Main Application Manager ---
def main_manager():
    """
    Main loop to manage scraping.
    - Uses API to get match list, odds, and bookmarker.
    - Spawns Selenium workers only for fancy/session data.
    """
    logging.info("Starting main manager loop.")
    
    while not main_shutdown_event.is_set():
        try:
            logging.info("="*30)
            logging.info("Starting new manager cycle. Getting auth token...")
            token = get_auth_token()
            if not token:
                logging.error("Failed to get auth token. Retrying in 60s.")
                main_shutdown_event.wait(timeout=60)
                continue
            
            logging.info("Got auth token. Fetching market data...")
            market_list = get_market_data(token)
            if not market_list:
                logging.error("Failed to get market data. Retrying in 10s.")
                main_shutdown_event.wait(timeout=10)
                continue
            
            logging.info(f"Fetched {len(market_list)} market entries. Parsing...")
            found_matches = parse_market_data(market_list)
            live_match_ids = set()

            # --- Start new threads ---
            for match_id, match_data in found_matches.items():
                if main_shutdown_event.is_set():
                    break
                
                if match_data["in_play"]:
                    live_match_ids.add(match_id)
                    with data_lock:
                        is_active = match_id in active_match_threads
                    
                    if not is_active:
                        logging.info(f"Found new live match: {match_data['teams']} (ID: {match_id}). Starting thread.")
                        stop_event = threading.Event()
                        
                        # Pass the API-fetched data (odds, bookmarker) to the worker
                        t = threading.Thread(
                            target=scrape_match_worker, 
                            args=(match_id, match_data, stop_event),
                            name=f"Worker-{match_id}"
                        )
                        t.start()
                        with data_lock:
                            active_match_threads[match_id] = t
                            match_stop_events[match_id] = stop_event
                        main_shutdown_event.wait(timeout=1.0) # Stagger thread starts
                    else:
                        # Match already active, update its base data in the cache
                        with data_lock:
                            if match_id in live_data_cache:
                                # Update cache with fresh API data, Selenium worker will add fancy/session
                                live_data_cache[match_id].update(match_data)
                            else:
                                # Worker is active but cache is empty, populate it
                                live_data_cache[match_id] = match_data


            # --- Cleanup Logic ---
            with data_lock:
                active_ids = set(active_match_threads.keys())
            
            ids_to_stop = active_ids - live_match_ids
            if ids_to_stop:
                logging.info(f"Matches no longer in live list. Signaling threads to stop: {ids_to_stop}")
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
            logging.critical(f"Unhandled exception in main_manager: {e}", exc_info=True)
            main_shutdown_event.wait(timeout=30)

    # --- Manager Shutdown ---
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
        logging.critical(f"Main manager crashed unexpectedly: {e}", exc_info=True)
    finally:
        logging.info("Main manager has exited. Initiating final shutdown.")
        
        if not main_shutdown_event.is_set():
            logging.info("Main loop exited cleanly. Signaling all threads to stop.")
            shutdown_handler(None, None)
        
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

