import json
import logging
import os
import time
from typing import Any

from confluent_kafka import Consumer, KafkaError, Producer
from sqlalchemy.exc import IntegrityError

from app.database import SessionLocal, engine
from app.models import Base, ProcessedEvent


logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

KAFKA_TOPIC = "orders.events"
KAFKA_DLT_TOPIC = "orders.events.dlt"
KAFKA_GROUP_ID = "notification-service"

MAX_RETRIES = 3
RETRY_DELAY_SECONDS = 2


def create_consumer() -> Consumer:
    """Create the Kafka consumer."""

    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def create_dlt_producer() -> Producer:
    """Create a Kafka producer for dead-letter events."""

    return Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "notification-service-dlt",
        }
    )


def publish_to_dlt(
    producer: Producer,
    event: dict[str, Any],
    error_message: str,
) -> None:
    """Publish an unprocessable event to the dead-letter topic."""

    dlt_event = {
        "original_event": event,
        "error": error_message,
        "source_topic": KAFKA_TOPIC,
        "dead_letter_topic": KAFKA_DLT_TOPIC,
    }

    producer.produce(
        topic=KAFKA_DLT_TOPIC,
        key=str(event.get("order_id", "unknown")),
        value=json.dumps(dlt_event),
    )

    remaining = producer.flush(5)

    if remaining:
        raise RuntimeError(
            "Failed to publish event to the dead-letter topic."
        )

    logger.error(
        "Event moved to DLT: event_id=%s event_type=%s",
        event.get("event_id"),
        event.get("event_type"),
    )


def process_event(event: dict[str, Any]) -> None:
    """Process an order event with persistent idempotency."""

    event_id = event.get("event_id")
    event_type = event.get("event_type")
    order_id = event.get("order_id")
    user_id = event.get("user_id")

    if not event_id:
        raise ValueError("Event is missing event_id.")

    with SessionLocal() as session:
        existing_event = session.get(
            ProcessedEvent,
            event_id,
        )

        if existing_event:
            logger.info(
                "Skipping already processed event: event_id=%s",
                event_id,
            )
            return

        if event_type == "order.created":
            logger.info(
                "NOTIFICATION: Order %s created for user %s",
                order_id,
                user_id,
            )

        elif event_type == "order.cancelled":
            logger.info(
                "NOTIFICATION: Order %s cancelled for user %s",
                order_id,
                user_id,
            )

        elif event_type == "order.payment_failed":
            logger.info(
                "NOTIFICATION: Payment failed for order %s for user %s",
                order_id,
                user_id,
            )

        else:
            raise ValueError(
                f"Unsupported event type: {event_type}"
            )

        processed_event = ProcessedEvent(
            event_id=event_id,
            event_type=event_type,
        )

        session.add(processed_event)

        try:
            session.commit()

        except IntegrityError:
            session.rollback()

            logger.info(
                "Event was already recorded by another consumer attempt: "
                "event_id=%s",
                event_id,
            )

            return

        logger.info(
            "Event recorded as processed: event_id=%s",
            event_id,
        )


def process_with_retry(
    event: dict[str, Any],
    dlt_producer: Producer,
) -> bool:
    """Process an event with retries and move permanent failures to DLT."""

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            process_event(event)

            logger.info(
                "Event processed successfully: event_id=%s attempt=%s",
                event.get("event_id"),
                attempt,
            )

            return True

        except Exception as exc:
            logger.exception(
                "Event processing failed: event_id=%s attempt=%s/%s",
                event.get("event_id"),
                attempt,
                MAX_RETRIES,
            )

            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY_SECONDS)
                continue

            error_message = str(exc)

            publish_to_dlt(
                producer=dlt_producer,
                event=event,
                error_message=error_message,
            )

            return False

    return False


def run_consumer() -> None:
    """Start consuming order events."""

    Base.metadata.create_all(bind=engine)

    consumer = create_consumer()
    dlt_producer = create_dlt_producer()

    consumer.subscribe([KAFKA_TOPIC])

    logger.info(
        "Notification service started. "
        "Listening to topic '%s' with group '%s'.",
        KAFKA_TOPIC,
        KAFKA_GROUP_ID,
    )

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                logger.error(
                    "Kafka consumer error: %s",
                    message.error(),
                )
                continue

            try:
                event = json.loads(
                    message.value().decode("utf-8")
                )

            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.exception(
                    "Invalid event payload received from Kafka."
                )

                raw_payload = message.value().decode(
                    "utf-8",
                    errors="replace",
                )

                invalid_event = {
                    "raw_payload": raw_payload,
                    "partition": message.partition(),
                    "offset": message.offset(),
                }

                publish_to_dlt(
                    producer=dlt_producer,
                    event=invalid_event,
                    error_message=str(exc),
                )

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                continue

            logger.info(
                "Received event: event_id=%s event_type=%s "
                "order_id=%s partition=%s offset=%s",
                event.get("event_id"),
                event.get("event_type"),
                event.get("order_id"),
                message.partition(),
                message.offset(),
            )

            processed_successfully = process_with_retry(
                event=event,
                dlt_producer=dlt_producer,
            )

            if processed_successfully:
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                logger.info(
                    "Kafka offset committed: event_id=%s "
                    "partition=%s offset=%s",
                    event.get("event_id"),
                    message.partition(),
                    message.offset(),
                )

            else:
                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                logger.info(
                    "Kafka offset committed after DLT routing: "
                    "event_id=%s partition=%s offset=%s",
                    event.get("event_id"),
                    message.partition(),
                    message.offset(),
                )

    except KeyboardInterrupt:
        logger.info(
            "Notification service stopping."
        )

    finally:
        consumer.close()
        dlt_producer.flush(5)

        logger.info(
            "Kafka consumer closed."
        )


if __name__ == "__main__":
    run_consumer()