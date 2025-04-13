import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'Topic1',
    bootstrap_servers=['kafka1:9092'], 
    auto_offset_reset='earliest',
    group_id='consumer-group1',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Споживач consumer1 запущений для Topic1. Очікування повідомлень...")

for message in consumer:
    print(f"Отримано повідомлення: {message.value}")
