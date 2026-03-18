import asyncio
import json
import random

import aiohttp
import requests
from playwright.async_api import async_playwright

from get_cookies import get_cookies


def clean_headers(headers):
    """Очищает заголовки от символов новой строки"""
    cleaned = {}
    for key, value in headers.items():
        # Очищаем ключ
        clean_key = key.replace('\n', '').replace('\r', '').strip()
        if not clean_key:
            continue

        # Очищаем значение
        if isinstance(value, str):
            clean_value = value.replace('\n', '').replace('\r', '').strip()
            if clean_value:  # Не добавляем пустые заголовки
                cleaned[clean_key] = clean_value
        else:
            cleaned[clean_key] = value

    return cleaned


class Playwright:
    def __init__(self):
        self.headers = {}

    def get_cdp_url(self):
        playwright_cdp_address = f"http://127.0.0.1:9222"
        version_info = requests.get(f"{playwright_cdp_address}/json/version",
                                    headers={"Host": "localhost"})
        try:
            version_json = version_info.json()
        except json.decoder.JSONDecodeError as e:
            raise RuntimeError(
                f"Playwright CDP server is not running at {playwright_cdp_address}")

        url = version_json["webSocketDebuggerUrl"].replace(
            "localhost",
            f"127.0.0.1:9222",
        )
        return url

    def set_headers(self, request):
        self.headers = clean_headers(request.headers)

    async def issue(self):
        async with async_playwright() as p:
            url = "https://www.avito.ru/all/mebel_i_interer/myagkaya-mebel/divany-ASgBAgICAkRaqgKMvg2ArjU?p="
            browser = await p.chromium.launch(
                headless=False,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-automation",
                    "--disable-infobars",
                    "--disable-dev-shm-usage",
                    "--disable-browser-side-navigation",
                ]
            )
            print("Connected to CDP")
            context = await browser.new_context()


            for i in range(0, 100):
                page = await context.new_page()
                page.on("request", lambda request: self.set_headers(request))
                try:
                    response = await page.goto(
                        f"https://www.avito.ru/",
                        wait_until="domcontentloaded",
                        timeout=100000
                    )
                    await page.wait_for_selector('[data-marker="search-form/logo"]', timeout=5000)
                    cookies = await context.cookies()

                    print(response.status)

                    if response.status in [200]:
                        print(f"Куки: {cookies}\nЗаголовки:{self.headers}")
                        await page.close()
                        await browser.close()
                        return cookies, self.headers
                except Exception as e:
                    print(response.status)
                    if response.status in [429, 302]:
                        print("Спаслили...")
                        await asyncio.sleep(60)
                    await page.close()


    async def get(self):
        cookies, headers = await self.issue()

        cookies_dict = {}
        for cookie in cookies:
            cookies_dict[cookie['name']] = cookie['value']

        for i in range(0, 100):
            async with aiohttp.ClientSession() as session:
                async with session.get("https://www.avito.ru", cookies=cookies_dict,
                                       headers=clean_headers(headers)) as resp:
                    print(resp.status)
                    if resp.status == 429:
                        print("Емае")

                        cookies, headers = await self.issue()

                        cookies_dict = {}
                        for cookie in cookies:
                            cookies_dict[cookie['name']] = cookie['value']
                    await asyncio.sleep(random.uniform(1, 2))


async def test(self):
    cookies = await get_cookies(headless=False)
    print(cookies)


if __name__ == "__main__":
    playwright = Playwright()
    asyncio.run(playwright.get())
