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
                cycle_interval = self.settings.SCHEDULER_CYCLE_INTERVAL_SECONDS
                logger.info("scheduler_sleeping_seconds=%d", cycle_interval)
                # Прерываемый сон: проверяем флаг shutdown каждую секунду
                for _ in range(cycle_interval):
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


class ConstantScheduler:
    """Постоянный 24/7 планировщик.

    Цикл: search → force-pending → sleep → repeat.

    Pipeline пересоздаётся каждый цикл для очистки состояния.
    Ошибки в одном цикле логируются, но НЕ прерывают работу.

    Args:
        settings: Конфигурация приложения. Если ``None`` —
            создаётся экземпляр по умолчанию.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or Settings()
        self._running = False
        self._shutdown = False
        self._cycle_count = 0

    def _signal_handler(self, signum: int, frame) -> None:
        """Обработчик сигналов завершения."""
        logger.info("constant_shutdown_signal", signal=signum)
        self._shutdown = True
        self._running = False

    async def run(self) -> None:
        """Основной бесконечный цикл.

        Каждый цикл:
            1. Создать ``Pipeline(settings)``.
            2. Вызвать ``pipeline.run_constant_cycle()``.
            3. Логировать статистику.
            4. Закрыть pipeline (dispose engine).
            5. Прерываемый сон на ``CONSTANT_CYCLE_INTERVAL_SECONDS``.
            6. При shutdown — выйти из цикла.
        """
        signal.signal(signal.SIGINT, self._signal_handler)
        try:
            signal.signal(signal.SIGTERM, self._signal_handler)
        except (OSError, ValueError):
            pass  # SIGTERM может быть недоступен на Windows

        self._running = True
        logger.info(
            "constant_mode_starting",
            interval=self.settings.CONSTANT_CYCLE_INTERVAL_SECONDS,
            force_pending=self.settings.CONSTANT_FORCE_PENDING_AFTER_SEARCH,
        )

        while self._running and not self._shutdown:
            self._cycle_count += 1
            cycle_num = self._cycle_count

            try:
                logger.info("constant_cycle_start", cycle=cycle_num)

                pipeline = Pipeline(self.settings)
                try:
                    stats = await pipeline.run_constant_cycle()
                    logger.info(
                        "constant_cycle_complete",
                        cycle=cycle_num,
                        **stats,
                    )
                finally:
                    dispose_engine()
                    logger.info(
                        "constant_cycle_engine_disposed",
                        cycle=cycle_num,
                    )

            except Exception as exc:
                logger.error(
                    "constant_cycle_error",
                    cycle=cycle_num,
                    error=str(exc),
                    exc_info=True,
                )

            if self._running and not self._shutdown:
                interval = self.settings.CONSTANT_CYCLE_INTERVAL_SECONDS
                logger.info(
                    "constant_sleeping",
                    cycle=cycle_num,
                    seconds=interval,
                    next_cycle=cycle_num + 1,
                )
                await self._interruptible_sleep(interval)

        logger.info(
            "constant_shutdown",
            total_cycles=self._cycle_count,
        )

    async def _interruptible_sleep(self, seconds: int) -> None:
        """Сон с проверкой shutdown каждую секунду."""
        for _ in range(seconds):
            if not self._running or self._shutdown:
                break
            await asyncio.sleep(1)
