"""Добавление category-поисков в БД из конфигурационного файла.

Данные читаются из config/categories.json.

Запуск:
    python -m scripts.seed_category_searches
"""

from __future__ import annotations

import json
from pathlib import Path

from app.storage.database import ensure_tables
from app.storage.models import TrackedSearch
from app.storage import get_session, Repository

# Путь к конфигурационному файлу
_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "categories.json"


def _load_categories() -> list[dict]:
    """Загрузить список категорийных поисков из config/categories.json."""
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = json.load(f)
    return config["category_searches"]


def seed_category_searches() -> None:
    """Добавить category-поиски в БД из конфигурации."""
    ensure_tables()
    session = get_session()
    repo = Repository(session)

    categories = _load_categories()
    created_count = 0
    skipped_count = 0

    for data in categories:
        existing = repo.get_or_create_tracked_search(data["search_url"])

        # Проверяем, был ли только что создан или уже существует
        if existing.search_phrase is None or not existing.is_category_search:
            # Обновляем поля для category-поиска
            existing.is_category_search = True
            existing.search_phrase = data["search_phrase"]
            existing.category = data["category"]
            existing.schedule_interval_hours = data["schedule_interval_hours"]
            existing.max_ads_to_parse = data["max_ads_to_parse"]
            existing.priority = data["priority"]
            existing.is_active = True
            # Дополнительные поля из конфигурации
            if "location" in data:
                existing.location = data["location"]
            if "owner_type" in data:
                existing.owner_type = data["owner_type"]
            if "min_price" in data:
                existing.min_price = data["min_price"]
            created_count += 1
            print(f"  [+] Создан category-поиск: {data['search_phrase']}")
        else:
            skipped_count += 1
            print(f"  - Уже существует: {data['search_phrase']}")

    repo.commit()
    repo.close()

    print(f"\nИтого: создано {created_count}, пропущено {skipped_count}")


if __name__ == "__main__":
    seed_category_searches()
