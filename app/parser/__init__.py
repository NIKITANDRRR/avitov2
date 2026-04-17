"""Модуль парсинга HTML-страниц Avito."""

from __future__ import annotations

from app.parser.ad_parser import AdData, parse_ad_page
from app.parser.search_parser import SearchResultItem, parse_search_page
from app.parser.seller_parser import SellerProfileData, SoldItemData, parse_seller_profile

__all__ = [
    "AdData",
    "SearchResultItem",
    "SellerProfileData",
    "SoldItemData",
    "parse_ad_page",
    "parse_search_page",
    "parse_seller_profile",
]
