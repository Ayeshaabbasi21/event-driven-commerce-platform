from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4


def build_order_event(
    event_type: str,
    order_id: int,
    user_id: int,
    status: str,
    total_amount: Decimal,
    created_at: datetime,
) -> dict[str, Any]:
    """Build a standardized order event."""
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "order_id": order_id,
        "user_id": user_id,
        "status": status,
        "total_amount": str(total_amount),
        "created_at": created_at.isoformat(),
    }