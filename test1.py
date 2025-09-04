from kafka import KafkaConsumer, TopicPartition

def consume_from_partition(bootstrap_servers, topic, partition, offset, output_file):
    """
    Consumes messages from a specific Kafka partition and writes them to a file,
    including the offset and the message value.
    """
    # Initialize Kafka consumer
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id="scriptTest",
        auto_offset_reset='earliest',  # Ignored for manually assigned offsets
        enable_auto_commit=False       # Avoid auto-committing offsets
    )

    # Assign the consumer to the specific partition
    tp = TopicPartition(topic, partition)
    consumer.assign([tp])

    # Set the offset to start reading from
    consumer.seek(tp, offset)

    print(f"Consuming from topic: {topic}, partition: {partition}, starting at offset: {offset}")

    try:
        with open(output_file, 'w') as file:
            for message in consumer:
                # Write the offset and message to the file
                file.write(f"Offset: {message.offset}, Message: {message.value.decode('utf-8')}\n")
                print(f"current Offset : {message.offset}")

    except KeyboardInterrupt:
        print("Consumption interrupted by user")
    finally:
        consumer.close()
        print("Consumer closed")

# Example usage
if __name__ == "__main__":
    BOOTSTRAP_SERVERS = "kafka-connectplus:9092"  # Replace with your Kafka broker address
    TOPIC = "glue-nifi-provenance-events"         # Replace with your Kafka topic name
    PARTITION = 20                                # Replace with the desired partition
    OFFSET = 22600                                # Replace with a specific offset
    OUTPUT_FILE = "kafka_output.txt"             # File to save the messages

    consume_from_partition(BOOTSTRAP_SERVERS, TOPIC, PARTITION, OFFSET, OUTPUT_FILE)
