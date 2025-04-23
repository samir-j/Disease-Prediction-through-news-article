import json
from kafka import KafkaProducer
from download_kaggle_dataset import check_url

KAFKA_SERVER = "localhost:9092"

apiKey = "438247b4-ab8e-4b1a-8c68-a0de27803e97"
url = "https://content.guardianapis.com/search?from-date=2020-03-11&order-by=oldest&q=disease&api-key=438247b4-ab8e-4b1a-8c68-a0de27803e97"

producer = KafkaProducer(bootstrap_servers = KAFKA_SERVER,value_serializer=lambda v: json.dumps(v).encode('utf-8'))
data = check_url(url)
print(data)
if data:
    for articles in data:
        producer.send(topic ='news-events',value=articles)
    producer.flush()
else:
    print("No articles found: ")

if __name__ == "__main__":
    check_url(url)