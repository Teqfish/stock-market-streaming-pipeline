import json

from confluent_kafka import Consumer, Producer, TopicPartition


def publish_events(
    *,
    events: list[dict],
    broker: str,
    topic: str,
) -> int:
    """Publish canonical trade events to Redpanda."""

    delivery_errors = []

    def delivery_report(err, msg):
        if err is not None:
            delivery_errors.append(str(err))

    producer = Producer(
        {
            "bootstrap.servers": broker,
        }
    )

    for event in events:
        producer.produce(
            topic=topic,
            key=event["symbol"],
            value=json.dumps(event),
            callback=delivery_report,
        )

        producer.poll(0)

    remaining = producer.flush()

    if remaining:
        raise RuntimeError(
            f"{remaining} Redpanda messages were not delivered"
        )

    if delivery_errors:
        raise RuntimeError(
            "Redpanda delivery failed: "
            + "; ".join(delivery_errors)
        )

    return len(events)


def get_high_watermarks(
    *,
    broker: str,
    topic: str,
) -> dict[int, int]:
    """Return the next available offset for every topic partition."""

    consumer = Consumer(
        {
            "bootstrap.servers": broker,
            "group.id": "offset-inspector",
            "enable.auto.commit": False,
        }
    )

    try:
        metadata = consumer.list_topics(
            topic=topic,
            timeout=10,
        )

        topic_metadata = metadata.topics.get(topic)

        if topic_metadata is None:
            raise RuntimeError(
                f"Topic does not exist: {topic}"
            )

        if topic_metadata.error is not None:
            raise RuntimeError(
                f"Could not inspect topic {topic}: "
                f"{topic_metadata.error}"
            )

        offsets = {}

        for partition_id in sorted(
            topic_metadata.partitions
        ):
            _, high = consumer.get_watermark_offsets(
                TopicPartition(
                    topic,
                    partition_id,
                ),
                timeout=10,
                cached=False,
            )

            offsets[partition_id] = high

        return offsets

    finally:
        consumer.close()
