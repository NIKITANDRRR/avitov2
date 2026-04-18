"""Управление жизненным циклом браузера Playwright."""

from __future__ import annotations

import random
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from playwright.async_api import Browser, BrowserContext, Page, Playwright

def _get_user_agents() -> list[str]:
    """Возвращает список User-Agent строк из настроек."""
    from app.config.settings import get_settings
    return get_settings().USER_AGENTS


def _get_viewports() -> list[dict[str, int]]:
    """Возвращает список viewport размеров из настроек."""
    from app.config.settings import get_settings
    return get_settings().BROWSER_VIEWPORTS


def _get_locale() -> str:
    """Возвращает locale браузера из настроек."""
    from app.config.settings import get_settings
    return get_settings().BROWSER_LOCALE


def _get_timezone() -> str:
    """Возвращает timezone браузера из настроек."""
    from app.config.settings import get_settings
    return get_settings().BROWSER_TIMEZONE


class BrowserManager:
    """Управление жизненным циклом браузера Playwright.

    Отвечает за запуск браузера, создание контекстов с anti-detection
    настройками и корректное закрытие ресурсов.

    Attributes:
        headless: Запуск браузера в headless-режиме.
        use_proxy: Использовать прокси-сервер.
        proxy_url: URL прокси-сервера.
    """

    def __init__(
        self,
        headless: bool = False,
        use_proxy: bool = False,
        proxy_url: str | None = None,
    ) -> None:
        self.headless = headless
        self.use_proxy = use_proxy
        self.proxy_url = proxy_url
        self.logger = structlog.get_logger()

        self._playwright: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None

    async def start(self) -> None:
        """Запустить Playwright и создать браузер.

        Создаёт экземпляр Playwright, запускает Chromium с anti-detection
        аргументами и создаёт контекст браузера с рандомным user-agent.

        Raises:
            CollectorError: Если не удалось запустить браузер.
        """
        from playwright.async_api import async_playwright

        from app.utils.exceptions import CollectorError

        try:
            self._playwright = await async_playwright().start()

            launch_args = [
                "--disable-blink-features=AutomationControlled",
            ]

            self._browser = await self._playwright.chromium.launch(
                headless=self.headless,
                args=launch_args,
            )

            context_kwargs = self._build_context_kwargs()
            self._context = await self._browser.new_context(**context_kwargs)

            self.logger.info(
                "browser_started",
                headless=self.headless,
                use_proxy=self.use_proxy,
            )
        except Exception as exc:
            raise CollectorError(f"Failed to start browser: {exc}") from exc

    def _build_context_kwargs(self) -> dict:
        """Сформировать параметры для создания контекста браузера.

        Returns:
            dict: Параметры для ``browser.new_context()``.
        """
        kwargs: dict = {
            "user_agent": random.choice(_get_user_agents()),
            "viewport": random.choice(_get_viewports()),
            "locale": _get_locale(),
            "timezone_id": _get_timezone(),
        }

        if self.use_proxy and self.proxy_url:
            kwargs["proxy"] = {"server": self.proxy_url}

        return kwargs

    async def create_context(self) -> "BrowserContext":
        """Создать изолированный контекст браузера с уникальным fingerprint.

        Каждый вызов создаёт новый ``BrowserContext`` с рандомизированным
        viewport и user-agent, а также добавляет anti-detection скрипт.
        Используется для изоляции поисковых запросов друг от друга.

        Returns:
            BrowserContext: Новый изолированный контекст браузера.

        Raises:
            CollectorError: Если не удалось создать контекст.
        """
        from app.utils.exceptions import CollectorError

        try:
            if self._browser is None:
                await self.start()

            assert self._browser is not None  # для type checker

            viewport = {
                "width": random.randint(1280, 1920),
                "height": random.randint(720, 1080),
            }
            user_agent = random.choice(_get_user_agents())
            context = await self._browser.new_context(
                viewport=viewport,
                user_agent=user_agent,
                locale=_get_locale(),
                timezone_id=_get_timezone(),
            )

            # Anti-detection: скрыть navigator.webdriver
            await context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', "
                "{get: () => undefined});"
            )

            self.logger.debug(
                "isolated_context_created",
                viewport=viewport,
                user_agent=user_agent[:50],
            )
            return context
        except Exception as exc:
            raise CollectorError(
                f"Failed to create isolated context: {exc}"
            ) from exc

    async def close_context(self, context: "BrowserContext") -> None:
        """Закрыть контекст браузера.

        Args:
            context: Контекст браузера для закрытия.
        """
        if context is not None:
            try:
                await context.close()
                self.logger.debug("isolated_context_closed")
            except Exception as exc:
                self.logger.warning(
                    "context_close_error", error=str(exc),
                )

    async def new_page(self) -> Page:
        """Создать новую страницу с anti-detection настройками.

        Создаёт страницу в текущем контексте браузера. Если контекст
        ещё не создан, автоматически вызывает :meth:`start`.

        Returns:
            Page: Новая страница Playwright.

        Raises:
            CollectorError: Если не удалось создать страницу.
        """
        from app.utils.exceptions import CollectorError

        try:
            if self._context is None:
                await self.start()

            assert self._context is not None  # для type checker

            page = await self._context.new_page()

            # Anti-detection: скрыть navigator.webdriver
            await page.add_init_script(
                """
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                """
            )

            self.logger.debug("new_page_created")
            return page
        except Exception as exc:
            raise CollectorError(f"Failed to create new page: {exc}") from exc

    async def close(self) -> None:
        """Закрыть браузер и Playwright.

        Корректно освобождает все ресурсы: сначала закрывается браузер,
        затем останавливается Playwright.
        """
        try:
            if self._browser is not None:
                await self._browser.close()
                self._browser = None
                self.logger.info("browser_closed")
        except Exception as exc:
            self.logger.warning("browser_close_error", error=str(exc))

        try:
            if self._playwright is not None:
                await self._playwright.stop()
                self._playwright = None
        except Exception as exc:
            self.logger.warning("playwright_stop_error", error=str(exc))

        self._context = None
