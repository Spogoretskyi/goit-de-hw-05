from kafka import KafkaConsumer
from configs import kafka_config, topics_configurations
from functions import listen_alerts
import json


temperature_alerts_topic = topics_configurations["topic_temperature"]["name"]
humidity_alerts_topic = topics_configurations["topic_humidity"]["name"]

consumer = KafkaConsumer(
    temperature_alerts_topic,
    humidity_alerts_topic,
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="alerts_consumer",
    auto_offset_reset="earliest",
)


listen_alerts(consumer)
