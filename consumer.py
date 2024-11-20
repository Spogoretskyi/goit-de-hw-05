from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config, topics_configurations
from functions import filter_alerts
import json


topic_name = topics_configurations["topic_sensors"]["name"]
temperature_alerts_topic = topics_configurations["topic_temperature"]["name"]
humidity_alerts_topic = topics_configurations["topic_humidity"]["name"]

producer = KafkaProducer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    key_deserializer=lambda v: json.loads(v.decode("utf-8")),
    group_id="alerts_processor",
    auto_offset_reset="earliest",
)

consumer.subscribe([topic_name])


filter_alerts(producer, consumer, temperature_alerts_topic, humidity_alerts_topic)
