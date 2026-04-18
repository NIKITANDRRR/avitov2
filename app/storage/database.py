"""Подключение к базе данных PostgreSQL через SQLAlchemy."""

from __future__ import annotations

import logging
import threading

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.config import get_settings

logger = logging.getLogger(__name__)

_engine: "Engine | None" = None
_lock = threading.Lock()


class Base(DeclarativeBase):
    """Базовый класс для всех SQLAlchemy-моделей."""

    pass


def get_engine():
    """Создаёт и возвращает singleton-движок SQLAlchemy.

    Использует двойную проверку с блокировкой (double-checked locking)
    для потокобезопасного создания единственного экземпляра Engine.

    Returns:
        Engine: Экземпляр SQLAlchemy Engine для подключения к PostgreSQL.
    """
    global _engine
    if _engine is None:
        with _lock:
            if _engine is None:
                settings = get_settings()
                _engine = create_engine(
                    settings.DATABASE_URL,
                    echo=False,
                    pool_size=settings.DB_POOL_SIZE,
                    max_overflow=settings.DB_MAX_OVERFLOW,
                    pool_pre_ping=True,
                    pool_recycle=settings.DB_POOL_RECYCLE,
                    connect_args={"connect_timeout": settings.DB_CONNECT_TIMEOUT},
                )
                logger.info("database_engine_created", extra={
                    "pool_size": settings.DB_POOL_SIZE,
                    "pool_recycle": settings.DB_POOL_RECYCLE,
                })
    return _engine


def dispose_engine() -> None:
    """Закрывает все подключения в пуле и сбрасывает singleton Engine.

    Используется для graceful shutdown приложения.
    """
    global _engine
    if _engine is not None:
        _engine.dispose()
        _engine = None
        logger.info("database_engine_disposed")


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
