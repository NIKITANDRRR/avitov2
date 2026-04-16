"""Циклический планировщик запуска поисков Avito."""

from __future__ import annotations

import asyncio
import signal
import structlog

from app.config.settings import Settings
from app.scheduler.pipeline import Pipeline
from app.storage.database import dispose_engine

logger = structlog.get_logger(__name__)


class Scheduler:
    """Циклический планировщик запуска поисков.

    Периодически проверяет, какие поиски пора запускать,
    и передаёт их в :class:`Pipeline` для обработки.

    Поддерживает graceful shutdown по сигналам SIGINT/SIGTERM.

    Args:
        settings: Конфигурация приложения. Если ``None`` —
            создаётся экземпляр по умолчанию.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()
        self.pipeline = Pipeline(self.settings)
        self._running = False
        self._shutdown = False

    def _signal_handler(self, signum: int, frame) -> None:
        """Обработчик сигналов завершения."""
        logger.info("scheduler_signal_received", signal=signum)
        self._shutdown = True
        self.stop()

    async def run(self) -> None:
        """Основной цикл: проверяет каждые 5 минут, какие поиски пора запустить.

        Цикл работает до вызова :meth:`stop` или получения сигнала завершения.
        Каждый цикл:
            1. Вызывает :meth:`Pipeline.run_search_cycle` для обработки
               всех просроченных поисков.
            2. Спит 5 минут до следующей проверки.

        Исключения в одном цикле не прерывают работу планировщика.
        """
        signal.signal(signal.SIGINT, self._signal_handler)
        try:
            signal.signal(signal.SIGTERM, self._signal_handler)
        except (OSError, ValueError):
            pass  # SIGTERM может быть недоступен на Windows

        self._running = True
        logger.info("scheduler_started")

        while self._running and not self._shutdown:
            try:
                logger.info("scheduler_cycle_start")
                stats = await self.pipeline.run_search_cycle()
                logger.info("scheduler_cycle_end", **stats)
            except Exception as e:
                logger.error("scheduler_cycle_error: %s", e, exc_info=True)

            if self._running and not self._shutdown:
                logger.info("scheduler_sleeping_seconds=300")
                # Прерываемый сон: проверяем флаг shutdown каждую секунду
                for _ in range(300):
                    if not self._running or self._shutdown:
                        break
                    await asyncio.sleep(1)

        logger.info("scheduler_stopped")

    def stop(self) -> None:
        """Остановить планировщик.

        Текущий цикл завершится, после чего планировщик выйдет из цикла.
        Также закрывает все подключения к базе данных.
        """
        self._running = False
        logger.info("scheduler_stop_requested")
        dispose_engine()
        logger.info("scheduler_engine_disposed")
