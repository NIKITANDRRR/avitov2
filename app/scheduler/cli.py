"""CLI интерфейс Avito Monitor."""
from __future__ import annotations

import asyncio

import structlog
import typer

app = typer.Typer(
    name="avito-monitor",
    help="Avito Monitor — PoC система мониторинга объявлений",
)


@app.command()
def run() -> None:
    """Запустить один цикл сбора и анализа."""
    asyncio.run(_run_cycle())


async def _run_cycle() -> None:
    """Асинхронная реализация одного цикла сбора."""
    from app.scheduler.pipeline import Pipeline
    from app.utils import setup_logging
    from app.config import get_settings

    settings = get_settings()
    setup_logging(settings.LOG_LEVEL)

    logger = structlog.get_logger("cli")
    logger.info("starting_avito_monitor_cycle")

    pipeline = Pipeline()
    stats = await pipeline.run()

    logger.info("cycle_completed", **stats)

    if stats["errors"] > 0:
        raise typer.Exit(code=1)


@app.command()
def init_db() -> None:
    """Инициализировать таблицы в PostgreSQL."""
    from app.storage.database import Base, get_engine
    from app.storage.models import TrackedSearch, SearchRun, Ad, AdSnapshot, NotificationSent  # noqa: F401

    engine = get_engine()
    Base.metadata.create_all(engine)
    typer.echo("✅ Database tables created successfully.")


@app.command()
def test_telegram() -> None:
    """Проверить подключение к Telegram боту."""
    asyncio.run(_test_telegram())


async def _test_telegram() -> None:
    """Асинхронная проверка подключения к Telegram."""
    from app.notifier import TelegramNotifier

    notifier = TelegramNotifier()
    result = await notifier.test_connection()
    if result:
        typer.echo("OK: Telegram notifier ready (output -> data/notifications.jsonl)")
    else:
        typer.echo("FAIL: Telegram notifier check failed.")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
