"""Скрипт заполнения БД модельными поисковыми запросами из конфигурации.

Данные читаются из config/products.json.

Запуск:
    python -m scripts.seed_searches
    # или
    python scripts/seed_searches.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Добавляем корень проекта в sys.path для корректных импортов
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from app.storage.database import Base, get_engine
from app.storage.models import (  # noqa: F401
    Ad,
    AdSnapshot,
    NotificationSent,
    SearchRun,
    TrackedSearch,
)
from app.utils.helpers import build_avito_url

# Путь к конфигурационному файлу
from app.config.settings import get_settings
_CONFIG_PATH = Path(get_settings().PRODUCTS_CONFIG_PATH)


def _load_products() -> list[dict]:
    """Загрузить список модельных поисков из config/products.json."""
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
    return config["model_searches"]


def seed_searches() -> None:
    """Создать таблицы и добавить модельные поисковые запросы из конфигурации.

    - Если таблиц нет — создаёт их.
    - Если поиск с таким URL уже есть — обновляет параметры.
    - Не дублирует поиски при повторном запуске.
    """
    # 1. Инициализация таблиц
    engine = get_engine()
    Base.metadata.create_all(engine)
    print("✅ Таблицы созданы/проверены")

    # 2. Загрузка данных из конфигурации
    products = _load_products()

    # 3. Добавление поисковых запросов
    from app.storage import get_session
    from app.storage.repository import Repository

    session = get_session()
    repo = Repository(session)
    try:
        added = 0
        updated = 0
        for item in products:
            query = item["search_phrase"]
            location = item.get("location", "россия")
            search_url = build_avito_url(query, location)
            tracked = repo.get_or_create_tracked_search(search_url)

            is_new = tracked.search_phrase is None
            tracked.schedule_interval_hours = item.get("schedule_interval_hours", 0.5)
            tracked.max_ads_to_parse = item.get("max_ads_to_parse", 3)
            tracked.search_phrase = query
            tracked.is_active = True
            tracked.is_category_search = False
            tracked.priority = item.get("priority", 1)

            if is_new:
                added += 1
            else:
                updated += 1

        repo.commit()
        print(
            f"📊 Модельные поиски: {added} добавлено, {updated} обновлено "
            f"(всего {len(products)})"
        )
    except Exception as exc:
        print(f"❌ Ошибка при заполнении поисков: {exc}")
        raise
    finally:
        repo.close()


if __name__ == "__main__":
    seed_searches()
