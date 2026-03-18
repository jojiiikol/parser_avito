import json
import time
from pathlib import Path
from typing import Optional

from loguru import logger
from playwright.async_api import async_playwright

from parser.cookies.base import CookiesProvider


class PlaywrightCookies(CookiesProvider):
    def __init__(
            self,
            storage_path: str | Path = "storage/own_cookies.json",
            save_on_update: bool = True,  # Сохранять при каждом обновлении
            save_on_exit: bool = True,  # Сохранять при выходе (через atexit)
    ):
        self.storage_path = Path(storage_path)

        self.last_id: str | None = None  # Может пригодиться для совместимости
        self.last_cookies: dict | None = None
        self.last_headers: dict = {}

        self.unblock_started_at: float | None = None
        self.UNBLOCK_TIMEOUT = 10  # секунд

        self._load_from_disk()

        self.browser = None
        self.context = None

        # Регистрируем сохранение при выходе
        if save_on_exit:
            import atexit
            atexit.register(self._save_on_exit)

    def get(self) -> dict:
        if self.last_cookies:
            return self.last_cookies

        # Получение куков браузером
        raise Exception("Нет собственных cookies")

    def update(self, response):
        """Обновляем куки, сохраняя существующие"""
        if not response:
            return

        # response.cookies содержит ТОЛЬКО новые/измененные куки
        response_cookies = dict(response.cookies)

        if not response_cookies:
            logger.debug("Нет новых cookies в ответе")
            return

        # Если нет текущих cookies, инициализируем пустым словарем
        if self.last_cookies is None:
            self.last_cookies = {}

        # Проверяем, были ли реальные изменения
        changes = {}
        for key, val in response_cookies.items():
            if self.last_cookies.get(key) != val.value:
                changes[key] = val.value

        if not changes:
            logger.debug("Значения cookies не изменились")
            return

        # Обновляем только изменившиеся куки, остальные оставляем как есть
        self.last_cookies.update(changes)
        logger.info(f"🔄 Обновлены cookies: {list(changes.keys())}")

        # Сохраняем на диск
        self._save_to_disk()

    async def handle_block(self):
        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp("http://127.0.0.1:9222")
            print("Connected to CDP")
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto("https://www.avito.ru", wait_until="domcontentloaded", timeout=30000)
            cookies = await context.cookies()
            await page.close()
            cookies = self._extract_cookies_from_response(cookies)
            return cookies

    @staticmethod
    def _extract_cookies_from_response(response) -> Optional[dict]:
        """
        Извлекает cookies из response в зависимости от его типа
        """
        try:
            # Если response от requests
            if hasattr(response, 'cookies'):
                # Преобразуем RequestsCookieJar в dict
                return dict(response.cookies)

            # Если response от selenium webdriver
            elif hasattr(response, 'get_cookies'):
                cookies_list = response.get_cookies()
                return {cookie['name']: cookie['value'] for cookie in cookies_list}

            # Если response - это уже dict с cookies
            elif isinstance(response, dict):
                # Проверяем, похоже ли это на cookies
                if all(isinstance(k, str) for k in response.keys()):
                    return response

            # Если response - это строка (например, Set-Cookie header)
            elif isinstance(response, str):
                # Простой парсинг cookie string
                cookies = {}
                for item in response.split(';'):
                    if '=' in item:
                        key, value = item.strip().split('=', 1)
                        cookies[key] = value
                return cookies if cookies else None

            logger.debug(f"Неизвестный тип response для извлечения cookies: {type(response)}")
            return None

        except Exception as e:
            logger.warning(f"Ошибка при извлечении cookies из response: {e}")
            return None

    def _load_from_disk(self):
        if not self.storage_path.exists():
            logger.info("Нет сохраненных собственных cookies")
            return

        try:
            data = json.loads(self.storage_path.read_text(encoding="utf-8"))
            self.last_cookies = data.get("cookies")

            if self.last_cookies:
                logger.info(f"📂 Загружены собственные cookies с диска")
            else:
                logger.warning("Cookies файл есть, но нет cookies в нем")

        except Exception as err:
            logger.warning(f"Не удалось загрузить собственные cookies локально: {err}")

    def _save_to_disk(self):
        """Сохраняет cookies на диск"""
        if not self.last_cookies:
            logger.debug("Нет cookies для сохранения")
            return

        try:
            self.storage_path.parent.mkdir(parents=True, exist_ok=True)

            payload = {
                "cookies": self.last_cookies,
                "saved_at": time.time(),
                "cookie_count": len(self.last_cookies),
            }

            # Используем временный файл для атомарности записи
            temp_path = self.storage_path.with_suffix('.tmp')
            temp_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            temp_path.replace(self.storage_path)  # Атомарная операция

            logger.info(f"💾 Собственные cookies сохранены ({len(self.last_cookies)} шт.)")

        except Exception as err:
            logger.warning(f"Не удалось сохранить собственные cookies локально: {err}")

    def _save_on_exit(self):
        """Сохраняет cookies при выходе из программы"""
        if self.last_cookies:
            logger.info("📦 Сохраняем cookies перед выходом...")
            self._save_to_disk()

    def force_save(self):
        """Принудительное сохранение (можно вызывать вручную)"""
        self._save_to_disk()

    def clear(self):
        """Очищает текущие cookies"""
        logger.warning("🧹 Очищаем собственные cookies")
        self.last_cookies = None
        if self.storage_path.exists():
            self.storage_path.unlink()  # Удаляем файл