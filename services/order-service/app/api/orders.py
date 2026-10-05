from math import ceil

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.schemas.order import (
    OrderCreate,
    OrderListResponse,
    OrderResponse,
)
from app.services.order_service import (
    cancel_order as cancel_order_service,
    create_order as create_order_service,
    get_order as get_order_service,
    get_orders as get_orders_service,
)


router = APIRouter(
    prefix="/orders",
    tags=["Orders"],
)


@router.post(
    "",
    response_model=OrderResponse,
    status_code=status.HTTP_201_CREATED,
)
def create_order(
    order_data: OrderCreate,
    db: Session = Depends(get_db),
):
    return create_order_service(
        db=db,
        user_id=order_data.user_id,
        total_amount=order_data.total_amount,
    )


@router.get(
    "",
    response_model=OrderListResponse,
)
def list_orders(
    page: int = Query(
        default=1,
        ge=1,
        description="Page number starting from 1.",
    ),
    page_size: int = Query(
        default=20,
        ge=1,
        le=100,
        description="Number of orders per page.",
    ),
    db: Session = Depends(get_db),
):
    orders, total = get_orders_service(
        db=db,
        page=page,
        page_size=page_size,
    )

    pages = ceil(total / page_size) if total else 0

    return {
        "items": orders,
        "page": page,
        "page_size": page_size,
        "total": total,
        "pages": pages,
    }


@router.get(
    "/{order_id}",
    response_model=OrderResponse,
)
def get_order(
    order_id: int,
    db: Session = Depends(get_db),
):
    return get_order_service(
        db=db,
        order_id=order_id,
    )


@router.patch(
    "/{order_id}/cancel",
    response_model=OrderResponse,
)
def cancel_order(
    order_id: int,
    db: Session = Depends(get_db),
):
    return cancel_order_service(
        db=db,
        order_id=order_id,
    )