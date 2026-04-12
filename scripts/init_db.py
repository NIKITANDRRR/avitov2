"""Скрипт создания таблиц в PostgreSQL."""

from __future__ import annotations

from app.storage.database import Base, get_engine
from app.storage.models import TrackedSearch, SearchRun, Ad, AdSnapshot, NotificationSent  # noqa: F401


def init_db() -> None:
    """Создаёт все таблицы в базе данных на основе SQLAlchemy-моделей.

    Использует Base.metadata.create_all для создания таблиц,
    которые ещё не существуют в базе.
    """
    engine = get_engine()
    Base.metadata.create_all(engine)
    print("Database tables created successfully.")


if __name__ == "__main__":
    init_db()
