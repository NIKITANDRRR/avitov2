"""Модуль отправки уведомлений."""

from app.notifier.email_notifier import EmailNotifier
from app.notifier.telegram_notifier import NotificationResult, TelegramNotifier

__all__ = [
    "EmailNotifier",
    "NotificationResult",
    "TelegramNotifier",
]
