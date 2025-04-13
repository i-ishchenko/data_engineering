import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['kafka1:9092', 'kafka2:9093'], 
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('Divvy_Trips_2019_Q4.csv', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        message = row 
        producer.send('Topic1', value=message)
        producer.send('Topic2', value=message)
        print(f"Відправлено повідомлення: {message}")
        time.sleep(1)

producer.flush()
