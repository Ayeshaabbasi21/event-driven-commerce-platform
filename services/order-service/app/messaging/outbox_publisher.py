import json
import logging
import time
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from app.db.session import SessionLocal
from app.messaging.kafka import publish_event
from app.models.outbox import OutboxEvent

logger = logging.getLogger(__name__)

PUBLISH_INTERVAL_SECONDS = 2


def publish_pending_events() -> None:
    db = SessionLocal()

    try:
        events = list(
            db.scalars(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.id)
                .limit(100)
            ).all()
        )

        if not events:
            return

        logger.info(
            "Found %s pending outbox event(s).",
            len(events),
        )

        for event in events:
            try:
                payload = json.loads(event.payload)

                publish_event(payload)

                event.published_at = datetime.utcnow()
                db.commit()

                logger.info(
                    "Outbox event published successfully: "
                    "event_id=%s event_type=%s",
                    event.event_id,
                    event.event_type,
                )

            except Exception:
                db.rollback()

                logger.exception(
                    "Failed to publish outbox event. "
                    "It will be retried: event_id=%s",
                    event.event_id,
                )

    except SQLAlchemyError:
        db.rollback()

        logger.exception(
            "Database error while reading pending outbox events."
        )

    finally:
        db.close()


def run_publisher() -> None:
    logger.info("Outbox publisher started.")

    while True:
        publish_pending_events()
        time.sleep(PUBLISH_INTERVAL_SECONDS)