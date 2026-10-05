import json
import logging
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.error import KafkaException

from app.core.config import settings

logger = logging.getLogger(__name__)

TOPIC = "orders.events"

producer = Producer(
    {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "client.id": settings.app_name,
    }
)


def publish_event(
    event: dict[str, Any],
    timeout: float = 5.0,
) -> None:
    delivery_result: dict[str, Any] = {
        "error": None,
        "delivered": False,
    }

    def delivery_report(err: Any, message: Any) -> None:
        if err is not None:
            delivery_result["error"] = err

            logger.error(
                "Kafka delivery failed: topic=%s error=%s",
                message.topic(),
                err,
            )
            return

        delivery_result["delivered"] = True

        logger.info(
            "Kafka event delivered: topic=%s partition=%s offset=%s",
            message.topic(),
            message.partition(),
            message.offset(),
        )

    try:
        producer.produce(
            topic=TOPIC,
            key=str(event["order_id"]),
            value=json.dumps(event),
            callback=delivery_report,
        )

        producer.poll(0)

        logger.info(
            "Kafka event queued: event_type=%s order_id=%s event_id=%s",
            event["event_type"],
            event["order_id"],
            event["event_id"],
        )

        remaining = producer.flush(timeout)

        if remaining:
            raise RuntimeError(
                "Kafka delivery timed out with "
                f"{remaining} undelivered event(s)."
            )

        if delivery_result["error"] is not None:
            raise RuntimeError(
                "Kafka delivery failed: "
                f"{delivery_result['error']}"
            )

        if not delivery_result["delivered"]:
            raise RuntimeError(
                "Kafka producer finished without "
                "receiving delivery confirmation."
            )

    except BufferError:
        logger.exception(
            "Kafka producer local queue is full."
        )
        raise

    except KafkaException:
        logger.exception(
            "Kafka error while publishing event."
        )
        raise


def flush_events(timeout: float = 5.0) -> None:
    remaining = producer.flush(timeout)

    if remaining:
        raise RuntimeError(
            "Kafka producer still has "
            f"{remaining} undelivered event(s)."
        )