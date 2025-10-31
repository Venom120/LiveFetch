import json
import time
import os
import tempfile
import threading
import sys
import signal
import logging
import requests
from requests.exceptions import RequestException

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
# Use a new setting, fallback to old one
SCRAPE_PULL_INTERVAL = config.get('Scraper', {}).get('SCRAPE_INTERVAL_SECONDS', 2)
REQUEST_TIMEOUT = config.get('Scraper', {}).get('WEB_DRIVER_TIMEOUT', 10) # Re-use timeout
LEVEL = config.get('DEFAULT', {}).get('LEVEL', 'INFO').upper()
LEVELS = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
if LEVEL not in LEVELS:
    LEVEL = 'INFO'

# --- Logging Setup ---
logging.basicConfig(
    level=getattr(logging, LEVEL),
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
if DEPLOYED:
    data_dir = config.get('Paths', {}).get('DEPLOYED_DATA_DIR', '.')
else:
    data_dir = config.get('Paths', {}).get('DATA_DIR', '.')

DATA_FILE = os.path.join(data_dir, config.get('Paths', {}).get('DATA_FILE', 'live_data.json'))

# --- API & Session Setup ---
API_URL = 'https://api.radheexch.xyz/delaymarkets/markets/eventtype/4'

# All headers from your cURL command
API_HEADERS = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9,en-IN;q=0.8,hi;q=0.7',
    'content-type': 'application/json',
    'dnt': '1',
    'origin': 'https://www.radheexch.xyz',
    'priority': 'u=1, i',
    'referer': 'https://www.radheexch.xyz/',
    'sec-ch-ua': '"Microsoft Edge";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
    'sec-ch-ua-mobile': '?1',
    'sec-ch-ua-platform': '"Android"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36 Edg/141.0.0.0'
}

# Use a persistent session
http_session = requests.Session()
http_session.headers.update(API_HEADERS)

# --- Shutdown Handler ---
main_shutdown_event = threading.Event()

def shutdown_handler(sig, frame):
    """Gracefully handle SIGINT (Ctrl+C) and SIGTERM."""
    if not main_shutdown_event.is_set():
        logging.info("Shutdown signal received. Stopping loop...")
        main_shutdown_event.set()

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
        temp_path = None 
    except (IOError, os.error, json.JSONDecodeError) as e:
        logging.error(f"Error writing to JSON file {filename}: {e}")
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except OSError as e:
                logging.error(f"Error removing temp file {temp_path}: {e}")

# --- New Parsing Functions ---
def parse_price_string(price_str):
    """
    Parses a price string like '2.02:5034.66:*2:28620.56:'
    into a list of {'price': '...', 'size': '...'}
    """
    prices = []
    if not price_str:
        return prices
        
    parts = price_str.split(':*')
    for part in parts:
        if ':' in part:
            try:
                price, size = part.split(':', 1)
                if price and size:
                    prices.append({"price": price, "size": size.rstrip(':*')})
            except ValueError:
                logging.warning(f"Could not parse price part: {part}")
    return prices

def parse_book_metadata(metadata_book, runners_list):
    """
    Parses the complex 'metadata.book' string to get odds.
    """
    try:
        # Create a quick lookup map for runner IDs to names
        runner_id_to_name = {str(r.get('id')): r.get('name') for r in runners_list}
        
        # Data is in the last part of the '|' separated string
        odds_data_str = metadata_book.split('|')[-1]
        
        # Split by ',' for each runner
        runner_data_parts = odds_data_str.split(',')
        
        parsed_runners = []
        
        for runner_str in runner_data_parts:
            parts = runner_str.split('~')
            if len(parts) < 4:
                continue
                
            runner_id = parts[0]
            team_name = runner_id_to_name.get(runner_id, runner_id) # Default to ID if not found
            
            back_str = parts[2]
            lay_str = parts[3]
            
            back_prices = parse_price_string(back_str)
            lay_prices = parse_price_string(lay_str)
            
            parsed_runners.append({
                "team_name": team_name,
                "back": back_prices,
                "lay": lay_prices
            })
            
        return parsed_runners
        
    except Exception as e:
        logging.error(f"Failed to parse metadata.book: {e} -- Data: {metadata_book}")
        return []

# --- Main Scraper Loop ---
def main_scrape_loop():
    """
    The new main loop.
    Fetches all data, processes it, writes to JSON, and sleeps.
    """
    logging.info("Starting API scraper loop...")
    
    while not main_shutdown_event.is_set():
        try:
            # 1. Fetch all market data from the API
            response = http_session.get(API_URL, timeout=REQUEST_TIMEOUT)
            response.raise_for_status() # Raise error for bad status
            all_markets = response.json()
            
            processed_matches = {}
            
            # 2. Process the market data
            for market in all_markets:
                catalogue = market.get('catalogue', {})
                event = market.get('event', {})
                
                # Filter for live matches only
                if not catalogue.get('inPlay'):
                    continue
                    
                event_id = str(event.get('id'))
                if not event_id:
                    continue
                    
                # Get or create the match entry in our dictionary
                if event_id not in processed_matches:
                    processed_matches[event_id] = {
                        "last_updated": time.time(),
                        "match_id": event_id,
                        "teams": event.get('name', 'Unknown Teams'),
                        "result": "In Progress", # Assume "In Progress" if inPlay=true
                        "odds": [],
                        "bookmarker": [],
                        "fancy": [],  # This API doesn't seem to provide this
                        "sessions": [], # This API doesn't seem to provide this
                    }
                
                # Get market type and runners
                market_type = catalogue.get('marketType')
                runners_list = catalogue.get('runners', [])
                metadata_book = market.get('metadata', {}).get('book')

                if not metadata_book:
                    continue

                # Parse the odds from the metadata string
                parsed_runners_data = parse_book_metadata(metadata_book, runners_list)
                
                # Assign to the correct category
                if market_type == "MATCH_ODDS":
                    processed_matches[event_id]["odds"] = parsed_runners_data
                elif market_type == "BOOKMAKER":
                    processed_matches[event_id]["bookmarker"] = parsed_runners_data
                elif market_type == "WINNING_ODDS":
                    # This is for things like "ODD/EVEN". We'll classify as a session.
                    processed_matches[event_id]["sessions"].append({
                        "name": catalogue.get('marketName', 'Session'),
                        "runners": parsed_runners_data
                    })
                #
                # TODO: Add logic for Fancy/Session data if you find
                # the other API endpoint. You would make another API
                # call here (for each match_id) and populate
                # processed_matches[event_id]["fancy"] and ["sessions"]
                #
            
            # 3. Convert dictionary values to a list for the final JSON
            final_data_list = list(processed_matches.values())
            
            # 4. Write to the JSON file
            write_to_json(final_data_list, DATA_FILE)
            
            if not final_data_list:
                logging.info("No live matches found in this cycle.")
            else:
                logging.info(f"Successfully processed {len(final_data_list)} live match(es).")

        except RequestException as e:
            logging.error(f"API request failed: {e}")
        except json.JSONDecodeError:
            logging.error("Failed to decode JSON response from API. Response may be invalid.")
        except Exception as e:
            logging.critical(f"Unhandled error in main loop: {e}", exc_info=True)
            
        # 5. Wait for the next interval
        main_shutdown_event.wait(timeout=SCRAPE_PULL_INTERVAL)

    logging.info("Scraper loop stopped.")

if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    logging.info(f"Starting API-based scraper. Writing to {DATA_FILE}")
    logging.info(f"Data will update every {SCRAPE_PULL_INTERVAL} seconds.")
    
    try:
        main_scrape_loop()
    except Exception as e:
        logging.critical(f"Main loop crashed unexpectedly: {e}", exc_info=True)
    finally:
        logging.info("Shutdown complete.")
        sys.exit(0)
