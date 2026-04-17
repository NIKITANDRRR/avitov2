"""Модуль хранения данных (PostgreSQL + сырой HTML)."""

from __future__ import annotations

from app.storage.database import Base, get_engine, get_session, get_session_factory
from app.storage.models import (
    Ad,
    AdSnapshot,
    NotificationSent,
    SearchRun,
    SegmentPriceHistory,
    SegmentStats,
    Seller,
    SoldItem,
    TrackedSearch,
)
from app.storage.repository import Repository

__all__ = [
    "Base",
    "get_engine",
    "get_session",
    "get_session_factory",
    "Ad",
    "AdSnapshot",
    "NotificationSent",
    "SearchRun",
    "SegmentPriceHistory",
    "SegmentStats",
    "Seller",
    "SoldItem",
    "TrackedSearch",
    "Repository",
]
