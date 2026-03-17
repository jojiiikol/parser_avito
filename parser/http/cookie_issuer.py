import typing as tp

from playwright.async_api import Playwright, Browser, BrowserContext, Page


class PlaywrightCore:
    def __init__(
            self,
            context_kwargs: tp.Optional[dict[str, tp.Any]] = None,
            browser_kwargs: tp.Optional[dict[str, tp.Any]] = None,
    ) -> None:
        """
        Initialize the Playwright core resources.
        """
        self.context_kwargs = context_kwargs or {}
        self.browser_kwargs = browser_kwargs or {}
        self.playwright: tp.Optional[Playwright] = None
        self.browser: tp.Optional[Browser] = None
        self.context: tp.Optional[BrowserContext] = None
        self.page: tp.Optional[Page] = None

    async def start(
            self,
            headless: bool = True,
    ) -> None:
        """
        Open Playwright browser with the specified options.

        :param proxy_type: The type of proxy to use (EU, RU, etc.).
        :param headless: Whether to run in headless mode.
        """
        await self._init_playwright()
        await self._init_browser(headless, proxy_type)
        await self._init_context()
        await self._init_page()

    async def _init_playwright(self) -> None:
        """Initialize Playwright."""
        self.playwright = await async_playwright().start()

    async def _init_browser(
            self, headless: bool = True, proxy_type: ProxyType = ProxyType.NONE
    ) -> None:
        """Initialize the browser with given options."""
        launch_options = await self._get_launch_options(headless, proxy_type)
        self.browser = await self.playwright.firefox.launch(**launch_options)

    async def _get_launch_options(
            self,
            headless: bool = True,
            proxy_type: ProxyType = ProxyType.NONE,
    ) -> dict[str, tp.Any]:
        """
        Generate browser launch options.

        :param headless: Whether to run in headless mode.
        :param proxy_type: The type of proxy to use.
        :return: A dictionary of launch options.
        """
        launch_options: dict[str, tp.Any] = {"headless": headless}
        proxy: tp.Optional[dict[str, tp.Optional[str]]] = await self._proxy_config_plw(
            proxy_type
        )
        launch_options["proxy"] = proxy
        launch_options.update(self.browser_kwargs)
        return launch_options

    @staticmethod
    async def _proxy_config_plw(
            proxy_type: ProxyType = ProxyType.NONE,
    ) -> tp.Optional[dict[str, tp.Optional[str]]]:
        proxy = await proxy_handle(proxy_type)
        if proxy is None:
            return proxy
        else:
            parsed_url = urlparse(proxy)
            proxy_dict = {
                "server": f"http://{parsed_url.hostname}:{parsed_url.port}",
                "username": parsed_url.username,
                "password": parsed_url.password,
            }

            filtered_proxy_dict = {k: v for k, v in proxy_dict.items() if v is not None}
            return filtered_proxy_dict

    async def _init_context(self) -> None:
        """Initialize the browser context."""
        context_options = await self._get_context_options()
        self.context = await self.browser.new_context(**context_options)

    async def _get_context_options(self) -> dict[str, tp.Any]:
        """
        Generate browser context options.

        :return: A dictionary of context options.
        """
        user_agent = await choice_master("ua")
        context_options = {"user_agent": user_agent, "ignore_https_errors": True}
        context_options.update(self.context_kwargs)
        return context_options

    async def _init_page(self) -> None:
        """Initialize a new page in the browser context."""
        self.page = await self.context.new_page()
        await custom_sleep(start_value=1)

    async def stop(self) -> None:
        """Close all Playwright resources."""
        if self.page:
            await self.page.close()
        if self.context:
            await self.context.close()
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def load_page(
            self,
            url: str,
            timeout: int = 30000,
    ) -> tp.Optional[Response]:
        """
        Goes to the specified URL.

        :param url: The URL to navigate to.
        :param timeout: Maximum time in milliseconds to wait for the navigation (default: 30 seconds).
        :return: Response object from Playwright
        """
        if not self.page:
            raise ValueError("Page is not initialized. Call 'start' method first.")
        response = await self.page.goto(url, wait_until="domcontentloaded", timeout=timeout)
        await custom_sleep(start_value=2)
        return response

    async def get_soup(
            self,
            url: str,
            proxy_type: ProxyType = ProxyType.NONE,
            parser_type: str = "html.parser",
            wait_time: tp.Optional[int] = None,
            scroll: bool = False,
            headless: bool = True,
    ) -> BeautifulSoup:
        _, _, content = await self.get_content(
            url=url,
            proxy_type=proxy_type,
            wait_time=wait_time,
            scroll=scroll,
            headless=headless,
        )
        return BeautifulSoup(markup=content, features=parser_type)

    @staticmethod
    async def get_soup_from_page(page: Page):
        if not page:
            raise ValueError("No page to get soup from")
        page_content: str = await page.content()
        soup: BeautifulSoup = BeautifulSoup(page_content, "html.parser")
        return soup

    async def scroll_page(self) -> None:
        _x = await custom_sleep(start_value=10, end_value=500, no_sleep=True)
        _y = await custom_sleep(start_value=900, end_value=1100, no_sleep=True)
        for _ in range(0, 5):
            if not self.page:
                raise ValueError("Page is not initialized. Call 'start' method first.")
            await self.page.mouse.wheel(_x, _y)
            await custom_sleep()

    async def get_content(
            self,
            url: str,
            proxy_type: ProxyType = ProxyType.NONE,
            wait_time: tp.Optional[int] = None,
            scroll: bool = False,
            headless: bool = True,
    ) -> tuple[int, tp.Optional[dict[str, str]], str]:
        try:
            await self.start(proxy_type=proxy_type, headless=headless)
            response = await self.load_page(url, timeout=0)
            if wait_time:
                await custom_sleep(time=wait_time)
            if scroll:
                await self.scroll_page()
            content = await self.page.content()

            # Response is None if you:
            # 1. Navigated to "about:blank"
            #    which is impossible, because of wait_until domcontentloaded
            # 2. Navigated to the same URL with different hash (fragment)
            #    which is impossible here, because of making single request per browser instance
            # Otherwise it throws an exception or returns Response object
            if response is None:
                raise ValueError("Unexpected response value: None")
            return response.status, dict(response.headers), content
        finally:
            await self.stop()