import asyncio
import json

import requests
from playwright.async_api import async_playwright


class Playwright:

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

    async def issue(self):
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            print("Connected to CDP")
            await asyncio.sleep(2)
            context = await browser.new_context()
            page = await context.new_page()
            await page.goto("https://www.avito.ru")
            print("Context page loaded, waiting for cookies to be issued...")
            await asyncio.sleep(5)
            cookies = await context.cookies()
            print(cookies)

if __name__ == "__main__":
    playwright = Playwright()
    asyncio.run(playwright.issue())