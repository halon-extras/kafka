from kafka import KafkaAdminClient
from kafka.admin import NewTopic

admin_client = KafkaAdminClient(bootstrap_servers='kafka:9092')

res = admin_client.create_topics(new_topics=[
        NewTopic('test-topic', 1, 1)
    ],
    validate_only=False
)
for topic, f in res.items():
    f.result()