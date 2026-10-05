import logging
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from app.cache.redis import (
    cache_order,
    delete_cached_order,
    get_cached_order,
)
from app.core.exceptions import (
    InvalidOrderStatusException,
    OrderNotFoundException,
)
from app.messaging.events import build_order_event
from app.models.order import Order
from app.services.outbox_service import create_outbox_event

logger = logging.getLogger(__name__)


def serialize_order(order: Order) -> dict[str, Any]:
    return {
        "id": order.id,
        "user_id": order.user_id,
        "status": order.status,
        "total_amount": str(order.total_amount),
        "created_at": order.created_at.isoformat(),
    }


def create_order(
    db: Session,
    user_id: int,
    total_amount: Decimal,
) -> Order:
    """Create an order and its Kafka event atomically."""
    logger.info("Creating order for user_id=%s", user_id)

    order = Order(
        user_id=user_id,
        total_amount=total_amount,
        status="pending",
    )

    try:
        db.add(order)
        db.flush()

        event = build_order_event(
            event_type="order.created",
            order_id=order.id,
            user_id=order.user_id,
            status=order.status,
            total_amount=order.total_amount,
            created_at=order.created_at,
        )

        create_outbox_event(
            db,
            event_id=event["event_id"],
            event_type=event["event_type"],
            aggregate_type="order",
            aggregate_id=order.id,
            payload=event,
        )

        db.commit()
        db.refresh(order)

    except SQLAlchemyError:
        db.rollback()
        logger.exception(
            "Database transaction failed while creating order "
            "for user_id=%s",
            user_id,
        )
        raise

    logger.info(
        "Order and outbox event created successfully: order_id=%s",
        order.id,
    )

    return order


def get_order(
    db: Session,
    order_id: int,
) -> Order | dict[str, Any]:
    """Retrieve an order using Redis cache before PostgreSQL."""
    logger.info("Fetching order_id=%s", order_id)

    cached_order = get_cached_order(order_id)

    if cached_order is not None:
        logger.info("Cache hit for order_id=%s", order_id)
        return cached_order

    logger.info("Cache miss for order_id=%s", order_id)

    order = db.get(Order, order_id)

    if order is None:
        logger.warning(
            "Order not found: order_id=%s",
            order_id,
        )
        raise OrderNotFoundException(order_id)

    cache_order(
        order_id=order.id,
        order_data=serialize_order(order),
    )

    logger.info(
        "Order cached successfully: order_id=%s",
        order_id,
    )

    return order


def get_orders(
    db: Session,
    page: int,
    page_size: int,
) -> tuple[list[Order], int]:
    """Return active orders using pagination."""
    offset = (page - 1) * page_size

    statement = (
        select(Order)
        .where(Order.status != "cancelled")
        .order_by(Order.id)
        .offset(offset)
        .limit(page_size)
    )

    orders = list(db.scalars(statement).all())

    total_statement = (
        select(func.count())
        .select_from(Order)
        .where(Order.status != "cancelled")
    )

    total = db.scalar(total_statement) or 0

    logger.info(
        "Fetched active orders: page=%s, page_size=%s, total=%s",
        page,
        page_size,
        total,
    )

    return orders, total


def cancel_order(
    db: Session,
    order_id: int,
) -> Order:
    """Cancel an order and store the event transactionally."""
    logger.info("Cancelling order_id=%s", order_id)

    order = db.get(Order, order_id)

    if order is None:
        logger.warning(
            "Cannot cancel missing order: order_id=%s",
            order_id,
        )
        raise OrderNotFoundException(order_id)

    if order.status != "pending":
        logger.warning(
            "Invalid cancellation attempt: order_id=%s, status=%s",
            order_id,
            order.status,
        )
        raise InvalidOrderStatusException(
            order_id=order_id,
            current_status=order.status,
            action="cancelled",
        )

    order.status = "cancelled"

    try:
        event = build_order_event(
            event_type="order.cancelled",
            order_id=order.id,
            user_id=order.user_id,
            status=order.status,
            total_amount=order.total_amount,
            created_at=order.created_at,
        )

        create_outbox_event(
            db,
            event_id=event["event_id"],
            event_type=event["event_type"],
            aggregate_type="order",
            aggregate_id=order.id,
            payload=event,
        )

        db.commit()
        db.refresh(order)

    except SQLAlchemyError:
        db.rollback()
        logger.exception(
            "Database transaction failed while cancelling "
            "order_id=%s",
            order_id,
        )
        raise

    delete_cached_order(order_id)

    logger.info(
        "Order cancellation and outbox event created successfully: "
        "order_id=%s",
        order_id,
    )

    return order