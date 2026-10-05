import logging

from app.consumer import run_consumer


logging.basicConfig(
    level=logging.INFO,
    format=(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(name)s | "
        "%(message)s"
    ),
)


if __name__ == "__main__":
    run_consumer()