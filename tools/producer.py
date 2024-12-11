from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='kafka:9092', max_block_ms=1000)

# Sending a simple string message
producer.send('test-topic', b'Hello, Kafka!')

# Key value send
print(producer.send('test-topic', key=b'my_key', value=b'Some message'))

# Ensure all messages are sent before exiting
producer.flush()