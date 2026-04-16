"""CLI интерфейс Avito Monitor."""

from __future__ import annotations

import asyncio
from datetime import datetime

import structlog
import typer

app = typer.Typer(
    name="avito-monitor",
    help="Avito Monitor — PoC система мониторинга объявлений",
)


# ------------------------------------------------------------------
# Команды управления поисками
# ------------------------------------------------------------------

@app.command("add-search")
def add_search(
    query: str = typer.Argument(..., help="Поисковый запрос (например: 'iPhone 15 Pro')"),
    location: str = typer.Option("Москва", "--location", "-l", help="Город/регион поиска"),
    interval: int = typer.Option(2, "--interval", "-i", help="Интервал запуска (часы)"),
    max_ads: int = typer.Option(3, "--max-ads", "-m", help="Карточек на поиск за запуск"),
    priority: int = typer.Option(1, "--priority", "-p", help="Приоритет (1-10, ниже = важнее)"),
) -> None:
    """Добавить поисковый запрос в отслеживание."""
    _ensure_tables()

    from app.storage import get_session
    from app.storage.repository import Repository
    from app.storage.models import TrackedSearch

    # Формируем URL поиска Avito
    from app.utils.helpers import build_avito_url
    search_url = build_avito_url(query, location)

    session = get_session()
    repo = Repository(session)
    try:
        tracked = repo.get_or_create_tracked_search(search_url)

        # Обновляем параметры, если поиск уже существует
        tracked.schedule_interval_hours = interval
        tracked.max_ads_to_parse = max_ads
        tracked.priority = priority
        tracked.search_phrase = query
        tracked.is_active = True
        repo.commit()

        typer.echo(
            f"✅ Поиск добавлен/обновлён (id={tracked.id}):\n"
            f"   URL: {search_url}\n"
            f"   Запрос: {query}\n"
            f"   Локация: {location}\n"
            f"   Интервал: {interval} ч.\n"
            f"   Макс. карточек: {max_ads}\n"
            f"   Приоритет: {priority}"
        )
    except Exception as exc:
        typer.echo(f"❌ Ошибка: {exc}", err=True)
        raise typer.Exit(code=1)
    finally:
        repo.close()


@app.command("remove-search")
def remove_search(
    search_id: int = typer.Argument(..., help="ID поискового запроса для удаления"),
) -> None:
    """Удалить поисковый запрос из отслеживания."""
    _ensure_tables()

    from sqlalchemy import delete
    from app.storage import get_session
    from app.storage.repository import Repository
    from app.storage.models import TrackedSearch

    session = get_session()
    repo = Repository(session)
    try:
        tracked = session.get(TrackedSearch, search_id)
        if tracked is None:
            typer.echo(f"❌ Поиск с id={search_id} не найден.", err=True)
            raise typer.Exit(code=1)

        url = tracked.search_url
        session.delete(tracked)
        repo.commit()

        typer.echo(f"✅ Поиск id={search_id} удалён: {url}")
    except typer.Exit:
        raise
    except Exception as exc:
        typer.echo(f"❌ Ошибка: {exc}", err=True)
        raise typer.Exit(code=1)
    finally:
        repo.close()


@app.command("list-searches")
def list_searches() -> None:
    """Показать список всех отслеживаемых поисков."""
    _ensure_tables()

    from app.storage import get_session
    from app.storage.repository import Repository

    session = get_session()
    repo = Repository(session)
    try:
        searches = repo.get_active_searches()

        if not searches:
            typer.echo("[ ] Нет отслеживаемых поисков.")
            return

        typer.echo(f"[LIST] Отслеживаемые поиски ({len(searches)}):\n")
        typer.echo("-" * 100)
        typer.echo(
            f"{'ID':<5} {'Запрос':<25} {'Интервал':<10} "
            f"{'Макс.объ':<10} {'Приор.':<8} {'Посл.запуск':<20}"
        )
        typer.echo("-" * 100)

        for s in searches:
            last_run = (
                s.last_run_at.strftime("%Y-%m-%d %H:%M")
                if s.last_run_at
                else "никогда"
            )
            phrase = s.search_phrase or s.search_url[:40]
            typer.echo(
                f"{s.id:<5} {phrase:<25} {s.schedule_interval_hours}ч.<10 "
                f"{s.max_ads_to_parse:<10} {s.priority:<8} {last_run:<20}"
            )
            typer.echo(f"      URL: {s.search_url}")

        typer.echo("-" * 100)
    except Exception as exc:
        typer.echo(f"❌ Ошибка: {exc}", err=True)
        raise typer.Exit(code=1)
    finally:
        repo.close()


# ------------------------------------------------------------------
# Команды запуска
# ------------------------------------------------------------------

@app.command("start")
def start() -> None:
    """Полный запуск: init-db + seed (модельные + категорийные) + scheduler."""
    # 1. Создать таблицы
    _ensure_tables()
    typer.echo("✅ Таблицы созданы/проверены")

    # 2. Заполнить модельные поиски
    _seed_searches()
    typer.echo("✅ Модельные поисковые запросы добавлены")

    # 3. Заполнить категорийные поиски
    _seed_category_searches()
    typer.echo("✅ Категорийные поисковые запросы добавлены")

    # 4. Запустить scheduler
    typer.echo(">> Запуск планировщика...")
    asyncio.run(_run_scheduler())


@app.command("run")
def run() -> None:
    """Запустить один цикл сбора и анализа (legacy-режим)."""
    asyncio.run(_run_cycle())


@app.command("run-scheduler")
def run_scheduler() -> None:
    """Запустить циклический планировщик."""
    asyncio.run(_run_scheduler())


@app.command("run-once")
def run_once(
    force: bool = typer.Option(
        True, "--force/--no-force", help="Принудительно запустить ВСЕ поиски (по умолчанию). "
        "С --no-force — только просроченные по расписанию.",
    ),
) -> None:
    """Однократный запуск поисков. По умолчанию — все принудительно."""
    asyncio.run(_run_once(force=force))


# ------------------------------------------------------------------
# Служебные команды
# ------------------------------------------------------------------

@app.command("init-db")
def init_db() -> None:
    """Инициализировать таблицы в PostgreSQL."""
    from app.storage.database import Base, get_engine
    from app.storage.models import (
        TrackedSearch, SearchRun, Ad, AdSnapshot, NotificationSent,
    )

    engine = get_engine()
    Base.metadata.create_all(engine)
    typer.echo("✅ Database tables created successfully.")


@app.command("test-telegram")
def test_telegram() -> None:
    """Проверить подключение к Telegram боту."""
    asyncio.run(_test_telegram())


# ------------------------------------------------------------------
# Асинхронные реализации
# ------------------------------------------------------------------

async def _run_cycle() -> None:
    """Асинхронная реализация одного цикла сбора (legacy)."""
    from app.scheduler.pipeline import Pipeline
    from app.utils import setup_logging
    from app.config import get_settings

    settings = get_settings()
    setup_logging(settings.LOG_LEVEL)

    logger = structlog.get_logger("cli")
    logger.info("starting_avito_monitor_cycle")

    pipeline = Pipeline(settings)
    stats = await pipeline.run()

    logger.info("cycle_completed", **stats)

    if stats["errors"] > 0:
        raise typer.Exit(code=1)


async def _run_scheduler() -> None:
    """Асинхронная реализация циклического планировщика."""
    from app.scheduler.scheduler import Scheduler
    from app.utils import setup_logging
    from app.config import get_settings

    settings = get_settings()
    setup_logging(settings.LOG_LEVEL)

    logger = structlog.get_logger("cli")
    logger.info("starting_scheduler")

    scheduler = Scheduler(settings)
    await scheduler.run()


async def _run_once(force: bool = True) -> None:
    """Асинхронная реализация однократного запуска поисков.

    Args:
        force: Если True (по умолчанию) — запустить ВСЕ активные поиски
            принудительно. Если False — только просроченные по расписанию.
    """
    from app.scheduler.pipeline import Pipeline
    from app.storage import get_session
    from app.storage.repository import Repository
    from app.utils import setup_logging
    from app.config import get_settings

    settings = get_settings()
    setup_logging(settings.LOG_LEVEL)

    logger = structlog.get_logger("cli")
    logger.info("starting_run_once", force=force)

    if force:
        typer.echo("[..] Запуск однократного цикла (run-once, принудительно — ВСЕ поиски)...")
    else:
        typer.echo("[..] Запуск однократного цикла (run-once, только просроченные)...")

    # Получаем поиски в зависимости от флага force
    forced_searches = None
    if force:
        from app.storage.database import ensure_tables
        ensure_tables()

        session = get_session()
        repo = Repository(session)
        try:
            forced_searches = repo.get_active_searches()
        finally:
            repo.close()

        if not forced_searches:
            typer.echo("[INFO] Нет активных поисков для обработки.")
            typer.echo("       Используйте 'add-search' для добавления поисков.")
            return

        typer.echo(f"[..] Найдено {len(forced_searches)} активных поисков. Запуск...")

    pipeline = Pipeline(settings)
    stats = await pipeline.run_search_cycle(searches=forced_searches)

    logger.info("run_once_completed", **stats)

    if stats["searches_processed"] == 0:
        if force:
            typer.echo("[INFO] Нет активных поисков для обработки.")
        else:
            typer.echo("[INFO] Нет просроченных поисков для обработки.")
        typer.echo("       Используйте 'add-search' для добавления или подождите.")
    else:
        typer.echo(
            f"[OK] Цикл завершён: "
            f"поисков={stats['searches_processed']}, "
            f"найдено={stats['ads_found']}, "
            f"новых={stats['ads_new']}, "
            f"недооценённых={stats['ads_undervalued']}, "
            f"уведомлений={stats['notifications_sent']}, "
            f"ошибок={stats['errors']}"
        )

    if stats["errors"] > 0:
        raise typer.Exit(code=1)


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


# ------------------------------------------------------------------
# Вспомогательные функции
# ------------------------------------------------------------------

def _ensure_tables() -> None:
    """Создать таблицы в БД при необходимости."""
    from app.storage.database import Base, get_engine
    from app.storage.models import (
        TrackedSearch, SearchRun, Ad, AdSnapshot, NotificationSent,
    )

    engine = get_engine()
    Base.metadata.create_all(engine)


def _seed_searches() -> None:
    """Добавить 14 поисковых запросов по всей России (если их нет)."""
    from app.storage import get_session
    from app.storage.repository import Repository
    from app.utils.helpers import build_avito_url

    SEARCHES = [
        # iPhone (6)
        "iPhone 15 Pro 128GB",
        "iPhone 15 Pro 256GB",
        "iPhone 15 Pro Max 256GB",
        "iPhone 15 128GB",
        "iPhone 14 Pro 128GB",
        "iPhone 14 Pro Max 256GB",
        # MacBook (4)
        "MacBook Air M2",
        "MacBook Air M3",
        "MacBook Pro M2",
        "MacBook Pro M3",
        # iPad (4)
        "iPad Pro 11 M4",
        "iPad Pro 13 M4",
        "iPad Air M2",
        "iPad mini 6",
    ]

    session = get_session()
    repo = Repository(session)
    try:
        added = 0
        updated = 0
        for query in SEARCHES:
            search_url = build_avito_url(query, "Россия")
            tracked = repo.get_or_create_tracked_search(search_url)

            is_new = tracked.search_phrase is None
            tracked.schedule_interval_hours = 2
            tracked.max_ads_to_parse = 3
            tracked.search_phrase = query
            tracked.is_active = True
            tracked.priority = 1

            if is_new:
                added += 1
            else:
                updated += 1

        repo.commit()
        typer.echo(
            f"[STATS] Поисковые запросы: {added} добавлено, {updated} обновлено "
            f"(всего {len(SEARCHES)})"
        )
    except Exception as exc:
        typer.echo(f"❌ Ошибка при заполнении поисков: {exc}", err=True)
        raise typer.Exit(code=1)
    finally:
        repo.close()


def _seed_category_searches() -> None:
    """Добавить категорийные поиски (телефоны, ноутбуки, велосипеды, шины)."""
    from app.storage import get_session
    from app.storage.repository import Repository

    CATEGORY_SEARCHES = [
        {
            "search_url": "https://www.avito.ru/rossiya/telefony",
            "search_phrase": "Телефоны (широкая лента)",
            "category": "телефоны",
            "schedule_interval_hours": 2,
            "max_ads_to_parse": 20,
            "priority": 10,
        },
        {
            "search_url": "https://www.avito.ru/rossiya/noutbuki",
            "search_phrase": "Ноутбуки (широкая лента)",
            "category": "ноутбуки",
            "schedule_interval_hours": 3,
            "max_ads_to_parse": 15,
            "priority": 8,
        },
        {
            "search_url": "https://www.avito.ru/rossiya/velosipedy",
            "search_phrase": "Велосипеды (широкая лента)",
            "category": "велосипеды",
            "schedule_interval_hours": 4,
            "max_ads_to_parse": 20,
            "priority": 7,
        },
        {
            "search_url": "https://www.avito.ru/rossiya/shiny",
            "search_phrase": "Шины (широкая лента)",
            "category": "шины",
            "schedule_interval_hours": 3,
            "max_ads_to_parse": 20,
            "priority": 7,
        },
    ]

    session = get_session()
    repo = Repository(session)
    try:
        created = 0
        skipped = 0
        for data in CATEGORY_SEARCHES:
            existing = repo.get_or_create_tracked_search(data["search_url"])

            if existing.search_phrase is None or getattr(existing, "search_type", None) == "model":
                existing.search_type = "category"  # type: ignore[attr-defined]
                existing.search_phrase = data["search_phrase"]
                existing.category = data["category"]  # type: ignore[attr-defined]
                existing.schedule_interval_hours = data["schedule_interval_hours"]
                existing.max_ads_to_parse = data["max_ads_to_parse"]
                existing.priority = data["priority"]
                existing.is_active = True
                created += 1
            else:
                skipped += 1

        repo.commit()
        typer.echo(
            f"[STATS] Категорийные поиски: {created} создано, {skipped} пропущено "
            f"(всего {len(CATEGORY_SEARCHES)})"
        )
    except Exception as exc:
        typer.echo(f"❌ Ошибка при заполнении категорийных поисков: {exc}", err=True)
        raise typer.Exit(code=1)
    finally:
        repo.close()


if __name__ == "__main__":
    app()
