class OrderServiceException(Exception):
    """Base exception for expected order-service errors."""


class OrderNotFoundException(OrderServiceException):
    """Raised when an order does not exist."""

    def __init__(self, order_id: int):
        self.order_id = order_id
        super().__init__(f"Order {order_id} was not found.")


class InvalidOrderStatusException(OrderServiceException):
    """Raised when an order operation is invalid for its current status."""

    def __init__(self, order_id: int, current_status: str, action: str):
        self.order_id = order_id
        self.current_status = current_status
        self.action = action

        super().__init__(
            f"Order {order_id} cannot be {action} "
            f"because its current status is '{current_status}'."
        )