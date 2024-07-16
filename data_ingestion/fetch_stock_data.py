import requests
import json
from kafka import KafkaProducer
import time

def fetch_stock_data(api_url, api_key, symbol):
    response = requests.get(f"{api_url}?symbol={symbol}&apikey={api_key}")
    data = response.json()
    return data

def produce_messages(producer, topic, data):
    producer.send(topic, json.dumps(data).encode('utf-8'))

if __name__ == "__main__":
    api_url = "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY"
    api_key = "your_api_key"
    symbol = "AAPL"
    kafka_topic = "stock"
    kafka_producer = KafkaProducer(bootstrap_servers='localhost:9092')

    while True:
        data = fetch_stock_data(api_url, api_key, symbol)
        produce_messages(kafka_producer, kafka_topic, data)
        time.sleep(300)  # Fetch new data every 5 minutes
