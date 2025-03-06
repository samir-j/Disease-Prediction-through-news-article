# from ensurepip import bootstrap

import requests
from kafka import KafkaProducer
import time
import json


NEWS_API_URL = " https://content.guardianapis.com/search?api-key=0b3ef29e-e871-427b-a5b2-0c7cba88e9f2"
API_KEY = "0b3ef29e-e871-427b-a5b2-0c7cba88e9f2"
TOPIC_NAME = "news-articles"
KAFKA_SERVER = "localhost:9092"

#
producer = KafkaProducer(
    bootstrap_servers = KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_news():
    headers = {
        'Authorization' : f'Bearer {API_KEY}'
    }
    response = requests.get(NEWS_API_URL, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to fetch news: ", response.status_code)
        return []


def publish_news():
    while True:
        news_data = fetch_news()
        if news_data and 'response' in news_data:
            articles = news_data['response'].get('results', [])
            for article in articles:
                # Prepare the message to be sent to Kafka
                message = {
                    'title': article['webTitle'],
                    'url': article['webUrl'],
                    'section': article['sectionName'],
                    'publication_date': article['webPublicationDate'],
                }
                producer.send(TOPIC_NAME, message)
                print("Sent: ", message)

        time.sleep(30)


if __name__ == "__main__":
    publish_news()