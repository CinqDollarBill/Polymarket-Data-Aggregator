import requests
from datetime import datetime, timezone
import json
import pytz
import csv
from collections import defaultdict

API_URL = "https://gamma-api.polymarket.com/events?slug=will-trump-remove-jerome-powell"
BASE_URL = "https://clob.polymarket.com"
OUTPUT_FILENAME = "output.csv"
EASTERN_TZ = pytz.timezone("US/Eastern")
FIDELITY = 10
INTERVAL = "1m"

def fetch_event_data(url):
    """Fetches event data from the specified API endpoint."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching event data: {e}")
        return None

def process_market(market, index):
    """Processes a single market, printing details and fetching historical prices."""
    token_id_str = market.get('clobTokenIds', 'N/A')
    question = market.get('question', f'Market {index+1}')
    
    print(f"\n📊 Market {index + 1}")
    print(f"❓ ID: {token_id_str}")
    print(f"❓ Question: {question}")
    print(f"💰 Outcome Prices: {market.get('outcomePrices', 'N/A')}")
    print(f"📈 Best Bid: {market.get('bestBid', 'N/A')}")
    print(f"📉 Best Ask: {market.get('bestAsk', 'N/A')}")
    print("─" * 50)

    try:
        token_ids = json.loads(token_id_str)
        first_token_id = int(token_ids[0])
        return question, fetch_historical_prices(first_token_id)
    except (json.JSONDecodeError, IndexError, ValueError) as e:
        print(f"⚠️ Error processing token ID: {e}")
        return question, None

def fetch_historical_prices(token_id):
    """Fetches historical price data for a given token ID."""
    params = {
        "market": token_id,
        "interval": INTERVAL,
        "fidelity": FIDELITY
    }
    
    try:
        response = requests.get(f"{BASE_URL}/prices-history", params=params)
        response.raise_for_status()
        return response.json().get("history", [])
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Error fetching historical prices: {e}")
        return []

def convert_to_eastern(utc_timestamp):
    """Converts UTC timestamp to Eastern Time string."""
    utc_dt = datetime.fromtimestamp(utc_timestamp, tz=timezone.utc)
    est_dt = utc_dt.astimezone(EASTERN_TZ)
    return est_dt.strftime('%Y-%m-%d %H:%M:%S') + " ET"

def export_combined_csv(filename, all_data, questions):
    """Exports combined market data to a CSV file with timestamps as rows."""
    sorted_timestamps = sorted(all_data.keys())
    
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['Timestamp (ET)'] + questions)
        
        for timestamp in sorted_timestamps:
            row = [timestamp]
            for question in questions:
                row.append(all_data[timestamp].get(question, ''))
            writer.writerow(row)
    
    print(f"✅ Combined CSV exported to {filename}")

def main():
    """Main function to execute the market data processing pipeline."""
    print("🌟 Elon Tweet Prediction Markets 🌟")
    print("=" * 50 + "\n")
    
    event_data = fetch_event_data(API_URL)
    if not event_data:
        print("❌ No event data available. Exiting.")
        return

    primary_event = event_data[0] if isinstance(event_data, list) else event_data
    markets = primary_event.get('markets', [])
    
    if not markets:
        print("❌ No markets found in event data.")
        return
    
    combined_data = defaultdict(dict)
    market_questions = []
    
    for idx, market in enumerate(markets):
        question, price_history = process_market(market, idx)
        market_questions.append(question)
        
        if not price_history:
            print(f"⚠️ No price history for market: {question}")
            continue
        
        print(f"\n🕒 Historical Prices for: {question}")
        print(f"{'⏰ Timestamp (ET)':<25} | {'💵 Price':>10}")
        print("─" * 40)
        
        for data_point in price_history:
            timestamp_str = convert_to_eastern(data_point["t"])
            price = data_point['p']
            print(f"{timestamp_str} — Price: {price}")
            combined_data[timestamp_str][question] = price
    
    if combined_data:
        export_combined_csv(OUTPUT_FILENAME, combined_data, market_questions)
    else:
        print("❌ No data collected for CSV export.")

if __name__ == "__main__":
    main()
