
from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for i in range(100):
    data = {'id': i, 'value': f'data {i}'}
    producer.send('real_time_topic', value=data)
    time.sleep(1)  # Espera 1 segundo entre mensajes

producer.close()
