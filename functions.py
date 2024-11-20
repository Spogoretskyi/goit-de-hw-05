from kafka.admin import KafkaAdminClient, NewTopic
import time
import random


def create_topics(admin_client, topic_configs):
    for topic_id, config in topic_configs.items():
        topic_name = config.get("name")
        num_partitions = config.get("num_partitions", 1)
        replication_factor = config.get("replication_factor", 1)

        if not topic_name:
            print(f"Error: Missing 'name' for topic '{topic_id}'. Skipping.")
            continue

        new_topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )

        try:
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            print(f"Topic '{topic_name}' created successfully.")
        except Exception as e:
            print(f"An error occurred while creating topic '{topic_name}': {e}")


def show_topics(admin_client):
    try:
        while True:
            print("my_topics")
            print("all_topics")
            print("close (exit)")

            user_input = input("Enter your choice: ").strip().lower()

            if user_input == "my_topics":
                print(
                    [
                        print(topic)
                        for topic in admin_client.list_topics()
                        if user["username"] in topic
                    ]
                )
                continue

            if user_input == "all_topics":
                print("Existing topics:\n", admin_client.list_topics())
                continue

            if user_input == "close" or user_input == "exit":
                print("Closing connection and exiting.")
                admin_client.close()
                break

            else:
                print("Invalid choice. Please try again.")
    except KeyboardInterrupt:
        print("Simulation stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        admin_client.close()


def produce_alerts(producer, sensor_id, topic_name):
    print(f"Starting sensor simulation with ID: {sensor_id}")
    print(f"Sending data to topic: {topic_name}")

    try:
        while True:
            data = {
                "sensor_id": sensor_id,
                "timestamp": time.time(),
                "temperature": random.uniform(25, 45),
                "humidity": random.uniform(15, 85),
            }

            try:
                producer.send(topic_name, key=sensor_id, value=data)
                producer.flush()
                print(f"Sent: {data}")
            except Exception as e:
                print(f"Error sending message: {e}")

            time.sleep(20)
    except KeyboardInterrupt:
        print("Simulation stopped by user.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        producer.close()
        print("Producer closed.")


def process_message(producer, message, temperature_alerts_topic, humidity_alerts_topic):
    sensor_id = message.get("sensor_id")
    temperature = message.get("temperature")
    humidity = message.get("humidity")
    timestamp = message.get("timestamp")

    if temperature > 40:
        alert = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature": temperature,
            "message": "Temperature exceeds threshold (40°C)",
        }
        try:
            producer.send(temperature_alerts_topic, key=sensor_id, value=alert)
            producer.flush()
            print(f"Temperature alert sent: {alert}")
        except Exception as e:
            print(f"Error sending temperature alert: {e}")

    if humidity > 80 or humidity < 20:
        alert = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "humidity": humidity,
            "message": "Humidity out of range (20-80%)",
        }
        try:
            producer.send(humidity_alerts_topic, key=sensor_id, value=alert)
            producer.flush()
            print(f"Humidity alert sent: {alert}")
        except Exception as e:
            print(f"Error sending humidity alert: {e}")


def filter_alerts(producer, consumer, temperature_alerts_topic, humidity_alerts_topic):
    print("Starting filtering alerts...")
    try:
        for message in consumer:
            try:
                key = message.key
                value = message.value
                # print(f"Received message: Key={key}, Value={value}")

                process_message(
                    producer, value, temperature_alerts_topic, humidity_alerts_topic
                )
            except Exception as e:
                print(f"An error occurred while processing message: {e}")
    except KeyboardInterrupt:
        print("Processor stopped by user.")
    finally:
        producer.close()
        consumer.close()
        print("Producer and consumer closed.")


def listen_alerts(consumer):
    print(
        "Subscribed to temperature_alerts and humidity_alerts. Listening for messages..."
    )
    try:
        for message in consumer:
            try:
                key = message.key
                value = message.value
                topic = message.topic
                print(f"[{topic}] Key={key}, Value={value}")
            except Exception as e:
                print(f"An error occurred while processing message: {e}")
    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        consumer.close()
        print("Consumer closed.")
