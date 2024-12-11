from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers='kafka:9092',
    group_id='my_group',
    auto_offset_reset='earliest'  # start from the earliest message
)

for message in consumer:
    print(f"{message.key}: {message.value}")
    print(message)