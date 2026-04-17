"""Сборщик данных с Avito через Playwright."""

from __future__ import annotations

import asyncio
import random
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import structlog

from playwright.async_api import Error as PlaywrightError

from app.collector.browser import BrowserManager
from app.config.settings import Settings
from app.utils.exceptions import CollectorError
from app.utils.helpers import random_delay, save_html, RateLimiter

if TYPE_CHECKING:
    from playwright.async_api import BrowserContext


class AvitoCollector:
    """Сборщик данных с Avito через Playwright.

    Открывает страницы Avito (поисковые и карточки объявлений),
    дожидается загрузки контента и сохраняет HTML для последующего парсинга.

    Attributes:
        browser: Менеджер браузера Playwright.
        settings: Конфигурация приложения.
    """

    # Селекторы ожидания для поисковой страницы
    _SEARCH_SELECTORS: list[str] = [
        '[data-marker="item"]',
        "div[class*='items-items-']",
    ]

    # Селекторы ожидания для карточки объявления
    _AD_SELECTORS: list[str] = [
        '[data-marker="item-view/title-info"]',
        '[data-marker="item-view/item-title"]',
        'h1[itemprop="name"]',
        "h1",
    ]

    def __init__(
        self,
        browser_manager: BrowserManager,
        settings: Settings,
        rate_limiter: RateLimiter | None = None,
        search_rate_limiter: RateLimiter | None = None,
        ad_rate_limiter: RateLimiter | None = None,
    ) -> None:
        self.browser = browser_manager
        self.settings = settings
        self._rate_limiter = rate_limiter
        self._search_rate_limiter = search_rate_limiter
        self._ad_rate_limiter = ad_rate_limiter
        self.logger = structlog.get_logger()

    async def _navigate_with_retry(
        self,
        page: "Page",  # noqa: F821
        url: str,
        max_attempts: int | None = None,
    ) -> None:
        """Навигация с retry и exponential backoff.

        При ошибке загрузки страницы выполняет повторные попытки
        с нарастающей задержкой (exponential backoff + jitter).

        Args:
            page: Страница Playwright.
            url: URL для навигации.
            max_attempts: Максимум попыток (по умолчанию из настроек).

        Raises:
            Exception: Последняя ошибка после исчерпания всех попыток.
        """
        max_attempts = max_attempts or self.settings.RETRY_MAX_ATTEMPTS

        for attempt in range(1, max_attempts + 1):
            try:
                await page.goto(
                    url, wait_until="domcontentloaded", timeout=30000,
                )
                return
            except Exception as e:
                if attempt == max_attempts:
                    raise
                backoff = min(
                    self.settings.RETRY_BACKOFF_BASE * (2 ** (attempt - 1)),
                    self.settings.RETRY_BACKOFF_MAX,
                )
                jitter = random.uniform(0.5, 1.5)
                wait_time = backoff * jitter
                self.logger.warning(
                    "navigate_retry",
                    url=url,
                    attempt=attempt,
                    max_attempts=max_attempts,
                    wait_sec=round(wait_time, 2),
                    error=str(e),
                )
                await asyncio.sleep(wait_time)

    async def collect_search_page(
        self,
        url: str,
        context: "BrowserContext | None" = None,
    ) -> tuple[str, str]:
        """Открыть поисковую страницу и вернуть ``(html, saved_path)``.

        Выполняет случайную задержку перед открытием, загружает страницу,
        ожидает появления списка объявлений, имитирует скролл и сохраняет HTML.

        Args:
            url: URL поисковой страницы Avito.
            context: Опциональный изолированный контекст браузера.
                Если передан — страница создаётся из него.

        Returns:
            tuple[str, str]: Кортеж ``(html_content, path_to_saved_file)``.

        Raises:
            CollectorError: Если не удалось загрузить страницу.
        """
        if context is not None:
            page = await context.new_page()
        else:
            page = await self.browser.new_page()
        try:
            await random_delay(
                self.settings.MIN_DELAY_SECONDS,
                self.settings.MAX_DELAY_SECONDS,
            )

            self.logger.info("collecting_search_page", url=url)

            # Rate limiter: приоритет раздельному, fallback на общий
            if self._search_rate_limiter is not None:
                await self._search_rate_limiter.acquire()
            elif self._rate_limiter is not None:
                await self._rate_limiter.acquire()

            await self._navigate_with_retry(page, url)

            # Ожидание загрузки списка объявлений
            selector_found = await self._wait_for_selectors(
                page, self._SEARCH_SELECTORS, timeout=15000,
            )
            if not selector_found:
                self.logger.warning(
                    "search_selectors_not_found",
                    url=url,
                    selectors=self._SEARCH_SELECTORS,
                )

            # Имитация скролла
            await page.mouse.wheel(0, random.randint(300, 800))
            await random_delay(1.0, 3.0)

            html = await page.content()

            # Генерация имени файла
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename = f"search_{timestamp}"
            directory = f"{self.settings.RAW_HTML_PATH}/search"
            saved_path = await save_html(html, directory, filename)

            self.logger.info(
                "search_page_collected",
                url=url,
                saved_path=saved_path,
                html_length=len(html),
            )

            return html, saved_path

        except PlaywrightError as exc:
            # TargetClosedError и другие ошибки Playwright — страница закрыта
            self.logger.warning(
                "search_page_browser_error",
                url=url,
                error=str(exc),
            )
            raise CollectorError(
                f"Failed to collect search page {url}: {exc}"
            ) from exc
        except Exception as exc:
            # Пытаемся сохранить HTML для отладки даже при ошибке
            try:
                html = await page.content()
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                directory = f"{self.settings.RAW_HTML_PATH}/search"
                await save_html(html, directory, f"search_error_{timestamp}")
            except Exception as save_exc:
                self.logger.debug("save_error_html_failed", error=str(save_exc))

            raise CollectorError(
                f"Failed to collect search page {url}: {exc}"
            ) from exc
        finally:
            try:
                await page.close()
            except Exception as exc:
                self.logger.debug("page_close_failed", error=str(exc))

    async def collect_ad_page(
        self,
        url: str,
        context: "BrowserContext | None" = None,
    ) -> tuple[str, str]:
        """Открыть карточку объявления и вернуть ``(html, saved_path)``.

        Выполняет случайную задержку перед открытием, загружает страницу,
        ожидает появления карточки объявления, имитирует скролл и сохраняет HTML.

        Args:
            url: URL карточки объявления Avito.
            context: Опциональный изолированный контекст браузера.
                Если передан — страница создаётся из него.

        Returns:
            tuple[str, str]: Кортеж ``(html_content, path_to_saved_file)``.

        Raises:
            CollectorError: Если не удалось загрузить страницу.
        """
        if context is not None:
            page = await context.new_page()
        else:
            page = await self.browser.new_page()
        try:
            await random_delay(
                self.settings.MIN_DELAY_SECONDS,
                self.settings.MAX_DELAY_SECONDS,
            )

            self.logger.info("collecting_ad_page", url=url)

            # Rate limiter: приоритет раздельному, fallback на общий
            if self._ad_rate_limiter is not None:
                await self._ad_rate_limiter.acquire()
            elif self._rate_limiter is not None:
                await self._rate_limiter.acquire()

            await self._navigate_with_retry(page, url)

            # Ожидание загрузки карточки объявления
            selector_found = await self._wait_for_selectors(
                page, self._AD_SELECTORS, timeout=15000,
            )
            if not selector_found:
                self.logger.warning(
                    "ad_selectors_not_found",
                    url=url,
                    selectors=self._AD_SELECTORS,
                )

            # Имитация скролла
            await page.mouse.wheel(0, random.randint(300, 800))
            await random_delay(1.0, 3.0)

            html = await page.content()

            # Генерация имени файла
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            filename = f"ad_{timestamp}"
            directory = f"{self.settings.RAW_HTML_PATH}/ad"
            saved_path = await save_html(html, directory, filename)

            self.logger.info(
                "ad_page_collected",
                url=url,
                saved_path=saved_path,
                html_length=len(html),
            )

            return html, saved_path

        except PlaywrightError as exc:
            # TargetClosedError и другие ошибки Playwright — страница закрыта
            self.logger.warning(
                "ad_page_browser_error",
                url=url,
                error=str(exc),
            )
            raise CollectorError(
                f"Failed to collect ad page {url}: {exc}"
            ) from exc
        except Exception as exc:
            # Пытаемся сохранить HTML для отладки даже при ошибке
            try:
                html = await page.content()
                timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                directory = f"{self.settings.RAW_HTML_PATH}/ad"
                await save_html(html, directory, f"ad_error_{timestamp}")
            except Exception as save_exc:
                self.logger.debug("save_error_html_failed", error=str(save_exc))

            raise CollectorError(
                f"Failed to collect ad page {url}: {exc}"
            ) from exc
        finally:
            try:
                await page.close()
            except Exception as exc:
                self.logger.debug("page_close_failed", error=str(exc))

    async def _wait_for_selectors(
        self,
        page: "Page",  # noqa: F821
        selectors: list[str],
        timeout: int = 15000,
    ) -> bool:
        """Ожидать хотя бы один из переданных селекторов.

        Пытается найти каждый селектор по очереди. Возвращает ``True``,
        если хотя бы один селектор был найден в течение таймаута.

        Args:
            page: Страница Playwright.
            selectors: Список CSS-селекторов для ожидания.
            timeout: Таймаут ожидания в миллисекундах.

        Returns:
            bool: ``True``, если хотя бы один селектор найден.
        """
        from playwright.async_api import TimeoutError as PlaywrightTimeout

        for selector in selectors:
            try:
                await page.wait_for_selector(selector, timeout=timeout)
                self.logger.debug(
                    "selector_found", selector=selector,
                )
                return True
            except PlaywrightTimeout:
                continue
            except PlaywrightError:
                # TargetClosedError — страница/контекст/браузер закрыты
                self.logger.warning(
                    "selector_wait_page_closed",
                    selector=selector,
                )
                return False
            except Exception as exc:
                self.logger.debug("selector_wait_failed", error=str(exc))
                continue

        return False

    async def close(self) -> None:
        """Закрыть ресурсы сборщика.

        Делегирует закрытие браузера :class:`BrowserManager`.
        """
        await self.browser.close()
        self.logger.info("collector_closed")
