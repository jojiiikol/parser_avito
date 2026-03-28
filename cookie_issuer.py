import asyncio
import json
import random

import aiohttp
import dateparser
import requests
from playwright.async_api import async_playwright, ProxySettings


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
        self.headers = {'sec-ch-ua-platform': '"macOS"', 'referer': 'https://www.avito.ru/', 'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36', 'sec-ch-ua': '"Not.A/Brand";v="99", "Chromium";v="136"', 'sec-ch-ua-mobile': '?0'}


    def parse_cookie_string(self, cookie_str: list[dict]) -> dict:
        cookies_dict = {}
        for cookie in cookie_str:
            cookies_dict[cookie['name']] = cookie['value']
        return cookies_dict

    async def issue(self):
        async with async_playwright() as p:
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
            # browser = await p.chromium.connect_over_cdp("http://202.148.55.193:9222")
            print("Connected to CDP")



            for i in range(0, 10):
                context = await browser.new_context(extra_http_headers=self.headers)
                await context.clear_cookies()
                page = await context.new_page()
                try:
                    response = await page.goto(
                        f"https://www.avito.ru/",
                        wait_until="domcontentloaded",
                        timeout=100000
                    )
                    await page.wait_for_selector('[data-marker="search-form/logo"]', timeout=5000)
                    cookies = await context.cookies()
                    if response.status in [200]:
                        print(f"Куки: {cookies}\nЗаголовки:{self.headers}")
                        await page.close()
                        await context.close()
                        return self.parse_cookie_string(cookies), self.headers
                except Exception as e:
                    print(response.status)
                    if response.status in [429, 302, 403]:
                        if i != 0:
                            await asyncio.sleep(random.randint(60, 180))
                        print("БЛОК")
                    await page.close()
                    await context.close()


    async def get(self):
        cookies, headers = await self.issue()
        print(cookies)

        for i in range(0, 500):
            print(len(cookies))
            async with aiohttp.ClientSession() as session:
                async with session.get("https://www.avito.ru", cookies=cookies,
                                       headers=clean_headers(headers)) as resp:
                    print(resp.status)
                    if resp.status == 429:
                        print("Емае")

                        cookies, headers = await self.issue()
                        await asyncio.sleep(2)




if __name__ == "__main__":
    playwright = Playwright()
    asyncio.run(playwright.get())

