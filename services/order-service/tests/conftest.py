import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.cache import redis as redis_cache
from app.core.config import settings
from app.db.base import Base
from app.db.session import get_db
from app.main import app
from app.models.order import Order  # noqa: F401


class FakeRedis:
    """Small in-memory Redis replacement for tests."""

    def __init__(self):
        self.data = {}

    def get(self, key):
        return self.data.get(key)

    def setex(self, key, ttl_seconds, value):
        self.data[key] = value
        return True

    def delete(self, key):
        return int(self.data.pop(key, None) is not None)

    def ping(self):
        return True


engine = create_engine(
    settings.test_database_url,
    pool_pre_ping=True,
)

TestingSessionLocal = sessionmaker(
    bind=engine,
    autocommit=False,
    autoflush=False,
)


@pytest.fixture(scope="session", autouse=True)
def setup_database():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def db_session():
    connection = engine.connect()
    transaction = connection.begin()
    session = TestingSessionLocal(bind=connection)

    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()


@pytest.fixture()
def client(db_session, monkeypatch):
    fake_redis = FakeRedis()

    monkeypatch.setattr(
        redis_cache,
        "redis_client",
        fake_redis,
    )

    def override_get_db():
        yield db_session

    app.dependency_overrides[get_db] = override_get_db

    with TestClient(app) as test_client:
        yield test_client

    app.dependency_overrides.clear()