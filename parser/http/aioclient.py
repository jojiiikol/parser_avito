"""
Клиент для запросов парсера
"""
import asyncio
import os
import random
import time
from http.cookies import SimpleCookie

import aiohttp
import httpx
from aiohttp import ClientTimeout
from loguru import logger

from get_cookies import get_cookies
from parser.cookies.base import CookiesProvider
from parser.cookies.playwright_cookies import PlaywrightCookies
from parser.proxies.proxy import Proxy


class ResponseObj:
    url: str
    status_code: int
    cookies: dict
    text: str



HEADERS = [
    {
        'sec-ch-ua-platform': '"Windows"',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        'sec-ch-ua-mobile': '?0',
    },
    {
        'sec-ch-ua-platform': '"macOS"',
        'referer': 'https://www.avito.ru/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Not.A/Brand";v="99", "Chromium";v="136"',
        'sec-ch-ua-mobile': '?0'
    }
]

class AioHttpClient:
    def __init__(
        self,
        proxy: Proxy,
        cookies: CookiesProvider | None = None,
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: int = 2,  # задержка после блокировки
        block_threshold: int = 20,  # ← сколько блоков подряд терпим
    ):
        self.proxy = proxy
        self.cookies = cookies
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.block_threshold = block_threshold
        self.headers = HEADERS
        self._block_attempts = 0
        self._global_retries = 0
        self.traffic = 0

    def _build_client(self) -> aiohttp.ClientSession:

        return aiohttp.ClientSession(
            timeout=ClientTimeout(total=self.timeout),
        )

    def extract_cookies(self, cookies: SimpleCookie):
        cookies_dict = {}
        for key, val in cookies.items():
            cookies_dict[key] = val.value
        return cookies_dict

    async def request(self, method: str, url: str, **kwargs) -> httpx.Response:
        last_exc = None

        for attempt in range(1, self.max_retries + 1):
            try:
                async with self._build_client() as client:
                    if self.cookies:
                        cookies = await self.cookies.get()
                        kwargs.setdefault("cookies", cookies)

                    proxy_rotate = random.choice([os.getenv("PROXY_URL1")])
                    headers_rotate = random.choice(HEADERS)
                    logger.debug(f"Использую прокси {proxy_rotate}")

                    response_raw = await client.request(proxy=proxy_rotate, method=method, url=url, headers=headers_rotate, **kwargs)
                    self.traffic += len(await response_raw.read()) / 1024 / 1024
                    logger.info(f"Траффик: {self.traffic}")
                    response = ResponseObj()
                    response.url = url
                    response.status_code = response_raw.status
                    response.cookies = self.extract_cookies(response_raw.cookies)
                    response.text = await response_raw.text()



                # === обновление cookies (если нужно) ===

                if self.cookies:
                    self.cookies.update(response)

                # === обработка блокировок ===
                if response.status_code in (401, 403, 429):
                    self._block_attempts += 1
                    logger.warning(
                        f"Blocked request ({response.status_code}), "
                        f"attempt {self._block_attempts}"
                    )

                    if self._block_attempts >= self.block_threshold or self._global_retries >= 20:
                        print(self._block_attempts)
                        print(self._global_retries)
                        logger.warning("Block threshold reached, handling block")

                        if self.cookies:
                            # cookies, headers, user_agent = await self.cookies.handle_block()
                            # self.cookies.force_update(cookies)
                            # self.headers = headers
                            delay = 40 * 60 + random.randint(0, 10 * 60)
                            logger.warning(f"БЛОКИРОВКА на {delay} секунд")
                            await asyncio.sleep(delay)
                        self.proxy.handle_block()
                        self._block_attempts = 0
                        self._global_retries = 0

                    await asyncio.sleep(random.uniform(1, self.retry_delay))
                    continue

                # === успех ===
                self._global_retries = 0
                self._block_attempts = 0
                return response

            except Exception as e:
                last_exc = e
                logger.warning(f"Request error (attempt {attempt}): {e}")
                self._global_retries += 1

        raise RuntimeError("HTTP request failed after retries") from last_exc
