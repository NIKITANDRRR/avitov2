"""Email-нотификатор для отправки уведомлений о недооценённых товарах.

Использует aiosmtplib для асинхронной отправки писем через SMTP.
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import aiosmtplib
import structlog

from app.config import get_settings
from app.notifier.telegram_notifier import NotificationResult
from app.utils.exceptions import NotifierError


class EmailNotifier:
    """Отправка уведомлений о недооценённых товарах через email.

    Args:
        smtp_host: SMTP-сервер.
        smtp_port: Порт SMTP.
        smtp_user: Логин SMTP.
        smtp_password: Пароль SMTP.
        smtp_use_tls: Использовать TLS.
        email_from: Адрес отправителя.
        email_to: Список адресов получателей.
    """

    def __init__(
        self,
        smtp_host: str | None = None,
        smtp_port: int | None = None,
        smtp_user: str | None = None,
        smtp_password: str | None = None,
        smtp_use_tls: bool | None = None,
        email_from: str | None = None,
        email_to: list[str] | None = None,
    ) -> None:
        self.logger = structlog.get_logger()
        settings = get_settings()
        self.smtp_host = smtp_host or settings.SMTP_HOST
        self.smtp_port = smtp_port or settings.SMTP_PORT
        self.smtp_user = smtp_user or settings.SMTP_USER
        self.smtp_password = smtp_password or settings.SMTP_PASSWORD
        self.smtp_use_tls = (
            smtp_use_tls if smtp_use_tls is not None else settings.SMTP_USE_TLS
        )
        self.email_from = email_from or settings.EMAIL_FROM
        self.email_to = email_to or settings.EMAIL_TO

    def _format_subject(self, ad: Any) -> str:
        """Сформировать тему письма.

        Args:
            ad: Объект объявления.

        Returns:
            Тема письма.
        """
        title = getattr(ad, "title", None) or "Без названия"
        return f"🔔 Товар ниже рынка: {title}"

    def _format_body(
        self,
        ad: Any,
        market_stats: Any,
        deviation_percent: float,
    ) -> str:
        """Сформировать текст письма (plain text).

        Args:
            ad: Объект объявления.
            market_stats: Статистика рынка.
            deviation_percent: Отклонение от медианы в %.

        Returns:
            Текст письма.
        """
        title = getattr(ad, "title", None) or "Не указано"
        price_val = getattr(ad, "price", None)
        price = f"{price_val:,.0f} ₽" if price_val is not None else "Не указано"
        median_val = getattr(market_stats, "median_price", None)
        median = (
            f"{median_val:,.0f} ₽"
            if median_val is not None
            else "Не указано"
        )
        url = getattr(ad, "url", None) or "Не указано"
        location = getattr(ad, "location", None) or "Не указано"
        detected_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        return (
            "🔔 Товар ниже рынка!\n"
            "\n"
            f"📦 {title}\n"
            f"💰 Цена: {price}\n"
            f"📊 Медиана: {median}\n"
            f"📉 Отклонение: {deviation_percent:+.1f}%\n"
            f"🔗 {url}\n"
            f"📍 {location}\n"
            f"🕐 Обнаружено: {detected_at}"
        )

    async def send_notification(
        self,
        ad: Any,
        market_stats: Any,
        deviation_percent: float,
    ) -> NotificationResult:
        """Отправить email-уведомление.

        Args:
            ad: Объект объявления.
            market_stats: Статистика рынка.
            deviation_percent: Отклонение от медианы в %.

        Returns:
            :class:`NotificationResult` с результатом.
        """
        subject = self._format_subject(ad)
        body = self._format_body(ad, market_stats, deviation_percent)

        self.logger.info(
            "sending_email_notification",
            ad_id=getattr(ad, "ad_id", None),
            recipients=self.email_to,
        )

        try:
            msg = MIMEMultipart()
            msg["From"] = self.email_from
            msg["To"] = ", ".join(self.email_to)
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "plain", "utf-8"))

            await aiosmtplib.send(
                msg,
                hostname=self.smtp_host,
                port=self.smtp_port,
                username=self.smtp_user,
                password=self.smtp_password,
                start_tls=self.smtp_use_tls,
            )

            message_id = f"email:{getattr(ad, 'ad_id', 'unknown')}"
            self.logger.info(
                "email_notification_sent",
                ad_id=getattr(ad, "ad_id", None),
                recipients=self.email_to,
            )
            return NotificationResult(
                success=True,
                message_id=message_id,
            )
        except Exception as exc:
            error_msg = f"Error sending email notification: {exc}"
            self.logger.error(
                "email_notification_send_error",
                ad_id=getattr(ad, "ad_id", None),
                error=error_msg,
            )
            raise NotifierError(error_msg) from exc

    async def send_undervalued_notifications(
        self,
        undervalued_ads: list,
        repository: object,
    ) -> list[NotificationResult]:
        """Отправить уведомления для списка undervalued объявлений.

        Args:
            undervalued_ads: Список :class:`UndervaluedAd`.
            repository: Экземпляр :class:`Repository`.

        Returns:
            Список :class:`NotificationResult`.
        """
        results: list[NotificationResult] = []

        for item in undervalued_ads:
            ad = item.ad

            if repository.is_notification_sent(ad.id):
                self.logger.info(
                    "notification_already_sent",
                    ad_id=ad.ad_id,
                    db_id=ad.id,
                )
                continue

            result = await self.send_notification(
                ad=ad,
                market_stats=item.market_stats,
                deviation_percent=item.deviation_percent,
            )

            if result.success:
                repository.mark_notification_sent(
                    ad_id=ad.id,
                    telegram_message_id=result.message_id,
                )

            results.append(result)

            delay = random.uniform(1.0, 3.0)
            await asyncio.sleep(delay)

        self.logger.info(
            "email_undervalued_notifications_completed",
            total=len(undervalued_ads),
            sent=len(results),
        )
        return results
