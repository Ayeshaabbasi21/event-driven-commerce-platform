from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field


class OrderCreate(BaseModel):
    user_id: int = Field(
        gt=0,
        description="ID of the user placing the order.",
    )

    total_amount: Decimal = Field(
        gt=0,
        description="Total monetary value of the order.",
    )


class OrderResponse(BaseModel):
    id: int
    user_id: int
    status: str
    total_amount: Decimal
    created_at: datetime

    model_config = {
        "from_attributes": True
    }


class OrderListResponse(BaseModel):
    items: list[OrderResponse]
    page: int
    page_size: int
    total: int
    pages: int