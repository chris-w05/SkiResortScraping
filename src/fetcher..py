import asyncio
from playwright.async_api import async_playwright, Browser, Page
from urllib.parse import urlparse
from logger_conf import setup_logger
from utils import sleep_random, domain_from_url, allowed_by_robots
import time

logger = setup_logger("fetcher")

class PageFetcher:
    def __init__(self, user_agent, concurrency=4, per_domain_delay=(1.0,3.0)):
        self.user_agent = user_agent
        self.per_domain_delay = per_domain_delay
        self.playwright = None
        self.browser = None
        self.lock = asyncio.Semaphore(concurrency)
        self.domain_last_access = {}

    async def start(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(headless=True, args=["--no-sandbox"])

    async def stop(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def _enforce_delay(self, domain):
        last = self.domain_last_access.get(domain)
        if last:
            wait = max(0, random.uniform(*self.per_domain_delay) - (time.time() - last))
            if wait > 0:
                await asyncio.sleep(wait)
        self.domain_last_access[domain] = time.time()

    async def fetch(self, url, render_js=True, timeout=25000):
        if not allowed_by_robots(url, self.user_agent):
            logger.info("Blocked by robots.txt: %s", url)
            return None, None
        domain = domain_from_url(url)
        await self._enforce_delay(domain)
        async with self.lock:
            try:
                if render_js:
                    context = await self.browser.new_context(user_agent=self.user_agent, viewport={"width":1280,"height":800})
                    page = await context.new_page()
                    await page.goto(url, timeout=timeout)
                    # wait for network idle
                    await page.wait_for_load_state("networkidle", timeout=timeout)
                    html = await page.content()
                    status = page.response.status if page.response else None
                    await page.close()
                    await context.close()
                else:
                    # fallback, not rendering JS - still use playwright to avoid extra deps
                    context = await self.browser.new_context(user_agent=self.user_agent)
                    page = await context.new_page()
                    r = await page.goto(url, timeout=timeout)
                    html = await page.content()
                    status = r.status if r else None
                    await page.close()
                    await context.close()
                return status, html
            except Exception as e:
                logger.exception("Error fetching %s: %s", url, e)
                return None, None
