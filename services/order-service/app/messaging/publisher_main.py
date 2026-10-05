import logging

from app.core.logging import configure_logging
from app.messaging.outbox_publisher import run_publisher


configure_logging()

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logger.info("Starting Order Service Outbox Publisher.")
    run_publisher()