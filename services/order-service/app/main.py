from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.api.orders import router as orders_router
from app.core.config import settings
from app.core.exceptions import (
    InvalidOrderStatusException,
    OrderNotFoundException,
)
from app.core.logging import configure_logging


configure_logging()


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Order management service for the Event-Driven Commerce Platform.",
)


@app.exception_handler(OrderNotFoundException)
async def order_not_found_handler(
    request: Request,
    exc: OrderNotFoundException,
):
    return JSONResponse(
        status_code=404,
        content={
            "error": "order_not_found",
            "message": str(exc),
            "order_id": exc.order_id,
        },
    )


@app.exception_handler(InvalidOrderStatusException)
async def invalid_order_status_handler(
    request: Request,
    exc: InvalidOrderStatusException,
):
    return JSONResponse(
        status_code=409,
        content={
            "error": "invalid_order_status",
            "message": str(exc),
            "order_id": exc.order_id,
            "current_status": exc.current_status,
        },
    )


app.include_router(orders_router)


@app.get("/")
def root():
    return {
        "service": settings.app_name,
        "status": "running",
        "version": settings.app_version,
        "environment": settings.environment,
    }


@app.get("/health")
def health_check():
    return {"status": "healthy"}