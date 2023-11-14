import requests
import pandas as pd
import os
from datetime import datetime, timedelta
import time

def fetch_orderbook():
    url = "https://api.upbit.com/v1/orderbook"
    params = {'markets': 'KRW-BTC'}
    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        return response.json()[0]
    else:
        return None

def process_orderbook_data(orderbook):
    timestamp = datetime.fromtimestamp(orderbook['timestamp']/1000).strftime('%Y-%m-%d %H:%M:%S')
    orderbook_units = orderbook['orderbook_units']

    bids = [{'price': unit['bid_price'], 'quantity': unit['bid_size'], 'type': 0, 'timestamp': timestamp} for unit in orderbook_units]
    asks = [{'price': unit['ask_price'], 'quantity': unit['ask_size'], 'type': 1, 'timestamp': timestamp} for unit in orderbook_units]

    return bids + asks

def main():
    start_time = datetime.now()
    end_time = start_time + timedelta(hours=48)

    while datetime.now() < end_time:
        orderbook = fetch_orderbook()
        if orderbook:
            orderbook_data = process_orderbook_data(orderbook)
            df = pd.DataFrame(orderbook_data)
            filename = f"{start_time.strftime('%Y-%m-%d')}-upbit-btc-orderbook.csv"
            df.to_csv(filename, mode='a', index=False, header=not os.path.exists(filename))
        time.sleep(1)

if __name__ == "__main__":
    main()
