import json
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.models.outbox import OutboxEvent


def create_outbox_event(
    db: Session,
    *,
    event_id: str,
    event_type: str,
    aggregate_type: str,
    aggregate_id: int,
    payload: dict[str, Any],
) -> OutboxEvent:
    """Store an event in the transactional outbox."""
    event = OutboxEvent(
        event_id=event_id,
        event_type=event_type,
        aggregate_type=aggregate_type,
        aggregate_id=str(aggregate_id),
        payload=json.dumps(payload),
    )

    db.add(event)

    return event


def mark_event_published(
    db: Session,
    event: OutboxEvent,
) -> None:
    """Mark an outbox event as successfully published."""
    event.published_at = datetime.utcnow()
    db.commit()