import sys
import json
import os
from flask import Flask, jsonify, make_response
from flask_cors import CORS
from http import HTTPStatus

# --- Configuration ---
try:
    with open('settings.json', 'r') as f:
        config = json.load(f)
except FileNotFoundError:
    print("FATAL ERROR: settings.json not found. Please create it.", file=sys.stderr)
    config = {}
except json.JSONDecodeError:
    print("FATAL ERROR: settings.json is not valid JSON.", file=sys.stderr)
    config = {}


DEPLOYED = config.get('DEFAULT', {}).get('DEPLOYED', False)

# Determine JSON data file path
if DEPLOYED:
    data_dir = config.get('Paths', {}).get('DEPLOYED_DATA_DIR', '/app/data')
else:
    data_dir = config.get('Paths', {}).get('DATA_DIR', './data')
DATA_FILE = os.path.join(data_dir, config.get('Paths', {}).get('DATA_FILE', 'live_data.json'))


app = Flask(__name__)
# Allow all origins for simplicity, tighten this in production if needed
CORS(app)


# --- API Endpoint ---

@app.route('/api/livedata', methods=['GET'])
def get_live_data():
    """
    Reads the JSON file and returns its contents.
    """
    try:
        # Open and read the data file
        with open(DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if len(data) == 0:
            # Return empty list instead of error
            return jsonify([])
        return jsonify(data)

    except FileNotFoundError:
        error_msg = {"error": "Data file not found. The scraper may not have run yet."}
        return make_response(jsonify(error_msg), HTTPStatus.NOT_FOUND)
        
    except json.JSONDecodeError:
        error_msg = {"error": "Error decoding JSON. The data file might be corrupted or empty."}
        return make_response(jsonify(error_msg), HTTPStatus.INTERNAL_SERVER_ERROR)
        
    except Exception as e:
        error_msg = {"error": "An unexpected error occurred.", "details": str(e)}
        return make_response(jsonify(error_msg), HTTPStatus.INTERNAL_SERVER_ERROR)


if __name__ == "__main__":
    # For production, use a WSGI server like Gunicorn
    # Example: gunicorn -w 4 -b 0.0.0.0:5100 api_server:app
    print(f"Starting Flask server on http://0.0.0.0:5100")
    print(f"Serving data from: {DATA_FILE}")
    print(f"CORS enabled for all origins.")
    print("Endpoint available at: /api/livedata")
    app.run(host='0.0.0.0', port=5100, debug=not DEPLOYED)
