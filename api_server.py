# /home/ubuntu/LiveFetch/api_server.py

import json
from flask import Flask, jsonify, make_response
from flask_cors import CORS
from http import HTTPStatus

# --- Configuration ---

DEPLOYED = False  # Set to True when deploying to production
JSON_DATA_FILE = "/app/data/live_data.json" if DEPLOYED else "./data/live_data.json"# Must match JSON_OUTPUT_FILE in scraper.py

app = Flask(__name__)
CORS(app, origins=["http://localhost:5100", "https://livefetch.venoms.app"])


# --- API Endpoint ---

@app.route('/api/livedata', methods=['GET'])
def get_live_data():
    """
    Reads the JSON file and returns its contents.
    (Fulfills Step 4 of the new task)
    """
    try:
        # Open and read the data file
        with open(JSON_DATA_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Return the data as a JSON response
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

# --- Run the Server ---

if __name__ == "__main__":
    # For production, use a WSGI server like Gunicorn
    # Example: gunicorn -w 4 -b 0.0.0.0:5100 api_server:app
    print(f"Starting Flask server on http://127.0.0.1:5100")
    print(f"Serving data from: {JSON_DATA_FILE}")
    print("Endpoint available at: https://livefetch.venoms.app/api/livedata")
    app.run(host='0.0.0.0', port=5100, debug=True)
