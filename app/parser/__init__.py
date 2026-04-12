"""Модуль парсинга HTML-страниц Avito."""

from __future__ import annotations

from app.parser.ad_parser import AdData, parse_ad_page
from app.parser.search_parser import SearchResultItem, parse_search_page

__all__ = [
    "AdData",
    "SearchResultItem",
    "parse_ad_page",
    "parse_search_page",
]
