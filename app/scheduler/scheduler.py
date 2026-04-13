"""Циклический планировщик запуска поисков Avito."""

from __future__ import annotations

import asyncio
import structlog

from app.config.settings import Settings
from app.scheduler.pipeline import Pipeline

logger = structlog.get_logger(__name__)


class Scheduler:
    """Циклический планировщик запуска поисков.

    Периодически проверяет, какие поиски пора запускать,
    и передаёт их в :class:`Pipeline` для обработки.

    Args:
        settings: Конфигурация приложения. Если ``None`` —
            создаётся экземпляр по умолчанию.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()
        self.pipeline = Pipeline(self.settings)
        self._running = False

    async def run(self) -> None:
        """Основной цикл: проверяет каждые 5 минут, какие поиски пора запустить.

        Цикл работает до вызова :meth:`stop`. Каждый цикл:
            1. Вызывает :meth:`Pipeline.run_search_cycle` для обработки
               всех просроченных поисков.
            2. Спит 5 минут до следующей проверки.

        Исключения в одном цикле не прерывают работу планировщика.
        """
        self._running = True
        logger.info("scheduler_started")

        while self._running:
            try:
                logger.info("scheduler_cycle_start")
                stats = await self.pipeline.run_search_cycle()
                logger.info("scheduler_cycle_end", **stats)
            except Exception as e:
                logger.error("scheduler_cycle_error: %s", e, exc_info=True)

            if self._running:
                logger.info("scheduler_sleeping_seconds=3000")
                await asyncio.sleep(3000)

        logger.info("scheduler_stopped")

    def stop(self) -> None:
        """Остановить планировщик.

        Текущий цикл завершится, после чего планировщик выйдет из цикла.
        """
        self._running = False
        logger.info("scheduler_stop_requested")
