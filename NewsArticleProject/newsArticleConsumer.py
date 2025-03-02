from kafka import KafkaConsumer
import json
import pandas as pd

from newsArticleProducer import TOPIC_NAME

KAFKA_SERVER = "localhost:9092"


consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers = KAFKA_SERVER,
    auto_offset_reset = "earliest",
    enable_auto_commit = True,
    value_deserializer = lambda x: json.loads(x.decode('utf-8'))
)

def preprocess_article(article):
    title = article.get('title', '').strip().lower()
    content = article.get('content', '').strip().lower()

    return {
        "title" : title,
        "content" : content
    }

def consume_news():
    for message in consumer:
        print("Message----", message)
        article = message.value
        preprocess_article = preprocess_article(article)
        print("Preprocessed: ", preprocess_article)

if __name__ == "__main__":
    consume_news()
#
# from kafka import KafkaConsumer
# import json
#
# # Kafka Consumer
# consumer = KafkaConsumer(
#     'who_articles',
#     bootstrap_servers='103.182.161.2',
#     auto_offset_reset='earliest',
#     enable_auto_commit=True,
#     value_deserializer=lambda x: json.loads(x.decode('utf-8'))
# )
#
# print("Listening for WHO articles...")
# for message in consumer:
#     who_data = message.value
#     print(f"Received WHO article data: {who_data}")