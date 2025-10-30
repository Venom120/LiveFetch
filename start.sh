#!/bin/bash
gunicorn -w 4 -b 0.0.0.0:5100 api_server:app &
python scraper.py
wait -n
exit $?