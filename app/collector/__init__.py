"""Модуль сбора HTML-страниц с Avito."""

from __future__ import annotations

from app.collector.browser import BrowserManager
from app.collector.collector import AvitoCollector

__all__ = [
    "AvitoCollector",
    "BrowserManager",
]
