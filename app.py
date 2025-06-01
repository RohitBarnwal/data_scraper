from flask import Flask, jsonify
from dhan_scraper import DhanStockScraper
import threading
import os

app = Flask(__name__)

def run_scraper():
    scraper = DhanStockScraper()
    scraper.run_once()

@app.route('/')
def home():
    return jsonify({
        "status": "ok",
        "message": "Dhan Scraper Service is running",
        "usage": {
            "trigger_scraping": "GET /trigger-scrape",
            "description": "Visit /trigger-scrape to start a scraping job. The results will be emailed to the configured recipient."
        }
    })

@app.route('/trigger-scrape', methods=['GET'])
def trigger_scrape():
    # Start scraping in a background thread
    thread = threading.Thread(target=run_scraper)
    thread.start()
    return jsonify({
        "status": "ok", 
        "message": "Scraping job started. Results will be emailed when complete."
    })

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port) 