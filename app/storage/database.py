"""Подключение к базе данных PostgreSQL через SQLAlchemy."""

from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.config import get_settings


class Base(DeclarativeBase):
    """Базовый класс для всех SQLAlchemy-моделей."""

    pass


def get_engine():
    """Создаёт и возвращает движок SQLAlchemy.

    Returns:
        Engine: Экземпляр SQLAlchemy Engine для подключения к PostgreSQL.
    """
    settings = get_settings()
    return create_engine(settings.DATABASE_URL, echo=False)


def get_session_factory() -> sessionmaker:
    """Создаёт и возвращает фабрику сессий SQLAlchemy.

    Returns:
        sessionmaker: Фабрика сессий, привязанная к движку.
    """
    engine = get_engine()
    return sessionmaker(bind=engine)


def get_session():
    """Создаёт и возвращает новую сессию базы данных.

    Returns:
        Session: Экземпляр SQLAlchemy-сессии.
    """
    Session = get_session_factory()
    return Session()


def ensure_tables() -> None:
    """Создаёт таблицы, если они не существуют."""
    from app.storage.models import (  # noqa: F401
        Ad, AdSnapshot, NotificationSent, SearchRun, TrackedSearch,
    )
    engine = get_engine()
    Base.metadata.create_all(engine)
