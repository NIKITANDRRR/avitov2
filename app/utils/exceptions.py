"""Кастомные исключения Avito Monitor."""


class AvitoMonitorError(Exception):
    """Базовое исключение для всех ошибок Avito Monitor."""

    pass


class CollectorError(AvitoMonitorError):
    """Ошибка при сборе HTML-страниц."""

    pass


class ParserError(AvitoMonitorError):
    """Ошибка при парсинге HTML-контента."""

    pass


class StorageError(AvitoMonitorError):
    """Ошибка при сохранении данных."""

    pass


class NotifierError(AvitoMonitorError):
    """Ошибка при отправке уведомлений."""

    pass


class ConfigurationError(AvitoMonitorError):
    """Ошибка конфигурации приложения."""

    pass
