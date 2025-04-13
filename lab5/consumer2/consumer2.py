import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'Topic2',
    bootstrap_servers=['kafka2:9093'], 
    auto_offset_reset='earliest',
    group_id='consumer-group2',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Споживач consumer2 запущений для Topic2. Очікування повідомлень...")

for message in consumer:
    print(f"Отримано повідомлення: {message.value}")
