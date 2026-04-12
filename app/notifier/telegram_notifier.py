"""Telegram-нотификатор для отправки уведомлений о недооценённых товарах.

Использует Telethon с поддержкой MTProto-прокси через TcpMTProxy.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog
from telethon import TelegramClient, connection

from app.config import get_settings
from app.utils.exceptions import NotifierError


SESSION_PATH = Path("data/bot_session")


@dataclass
class NotificationResult:
    """Результат отправки уведомления.

    Attributes:
        success: Флаг успешной отправки.
        message_id: ID сообщения (при успехе).
        error: Текст ошибки (при неудаче).
    """

    success: bool
    message_id: str | None = None
    error: str | None = None


class TelegramNotifier:
    """Отправка уведомлений о недооценённых товарах в Telegram через Telethon.

    Поддерживает подключение через MTProto-прокси (TcpMTProxy).

    Args:
        bot_token: Токен Telegram бота.
        chat_id: ID чата.
    """

    def __init__(
        self,
        bot_token: str | None = None,
        chat_id: str | None = None,
    ) -> None:
        self.logger = structlog.get_logger()
        settings = get_settings()
        self.bot_token = bot_token or settings.TELEGRAM_BOT_TOKEN
        self.chat_id = int(chat_id or settings.TELEGRAM_CHAT_ID)

        # Создаём директорию для сессии
        SESSION_PATH.parent.mkdir(parents=True, exist_ok=True)

        # Конфигурация MTProto-прокси
        proxy = None
        conn = None
        if settings.MTPROXY_ENABLED:
            proxy = (
                settings.MTPROXY_ADDRESS,
                settings.MTPROXY_PORT,
                settings.MTPROXY_SECRET,
            )
            conn = connection.TcpMTProxy
            self.logger.info(
                "mtproto_proxy_configured",
                address=settings.MTPROXY_ADDRESS,
                port=settings.MTPROXY_PORT,
            )

        # Создаём Telethon-клиента
        client_kwargs: dict[str, Any] = {
            "session": str(SESSION_PATH),
            "api_id": settings.TELEGRAM_API_ID,
            "api_hash": settings.TELEGRAM_API_HASH,
        }
        if conn is not None:
            client_kwargs["connection"] = conn
        if proxy is not None:
            client_kwargs["proxy"] = proxy

        self._client = TelegramClient(**client_kwargs)
        self._connected = False

    async def _ensure_connected(self) -> None:
        """Подключиться к Telegram, если ещё не подключены."""
        if self._connected:
            return
        try:
            await self._client.connect()
            if not await self._client.is_user_authorized():
                await self._client.sign_in(bot_token=self.bot_token)
            self._connected = True
            self.logger.info("telegram_connected")
        except Exception as exc:
            self._connected = False
            error_msg = f"Telegram connection error: {exc}"
            self.logger.error("telegram_connection_failed", error=error_msg)
            raise NotifierError(error_msg) from exc

    async def _disconnect(self) -> None:
        """Отключиться от Telegram."""
        if self._connected:
            await self._client.disconnect()
            self._connected = False
            self.logger.info("telegram_disconnected")

    def _format_message(
        self,
        ad: Any,
        market_stats: Any,
        deviation_percent: float,
    ) -> str:
        """Форматировать HTML-сообщение об undervalued товаре.

        Args:
            ad: Объект объявления.
            market_stats: Статистика рынка.
            deviation_percent: Отклонение от медианы в %.

        Returns:
            Отформатированное HTML-сообщение.
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
            "🔔 <b>Товар ниже рынка!</b>\n"
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
        """Отправить уведомление в Telegram.

        Args:
            ad: Объект объявления.
            market_stats: Статистика рынка.
            deviation_percent: Отклонение от медианы в %.

        Returns:
            :class:`NotificationResult` с результатом.

        Raises:
            NotifierError: При ошибке отправки.
        """
        text = self._format_message(ad, market_stats, deviation_percent)

        self.logger.info(
            "sending_notification",
            ad_id=getattr(ad, "ad_id", None),
            chat_id=self.chat_id,
        )

        try:
            await self._ensure_connected()
            msg = await self._client.send_message(
                self.chat_id,
                text,
                parse_mode="html",
            )
            message_id = str(msg.id)
            self.logger.info(
                "notification_sent",
                ad_id=getattr(ad, "ad_id", None),
                message_id=message_id,
            )
            return NotificationResult(
                success=True,
                message_id=message_id,
            )
        except NotifierError:
            raise
        except Exception as exc:
            self._connected = False  # Сбросить состояние подключения
            error_msg = f"Error sending notification: {exc}"
            self.logger.error(
                "notification_send_error",
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

        try:
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
        finally:
            await self._disconnect()

        self.logger.info(
            "undervalued_notifications_completed",
            total=len(undervalued_ads),
            sent=len(results),
        )
        return results

    async def test_connection(self) -> bool:
        """Проверить подключение к Telegram.

        Returns:
            ``True`` если подключение успешно.
        """
        self.logger.info(
            "notifier_test",
            bot_token_set=bool(self.bot_token),
            chat_id_set=bool(self.chat_id),
        )

        if not self.bot_token:
            self.logger.warning("bot_token_not_set")
            return False

        if not self.chat_id:
            self.logger.warning("chat_id_not_set")
            return False

        try:
            await self._ensure_connected()
            me = await self._client.get_me()
            self.logger.info(
                "notifier_ready",
                bot_username=getattr(me, "username", None),
                bot_id=getattr(me, "id", None),
            )
            return True
        except Exception as exc:
            self.logger.error("notifier_test_failed", error=str(exc))
            return False
        finally:
            await self._disconnect()
