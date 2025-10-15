import asyncio
import urllib
from playwright.async_api import async_playwright, Browser, Page
from urllib.parse import urlparse
from logger_conf import setup_logger
from utils import sleep_random, domain_from_url, allowed_by_robots
import time
import numpy as np

logger = setup_logger("fetcher")

class PageFetcher:
    
    __robots_cache = {}
    
    def __init__(self, user_agent, concurrency=4, per_domain_delay=(1.0,3.0)):
        self.user_agent = user_agent
        self.per_domain_delay = per_domain_delay
        self.playwright = None
        self.browser = None
        self.lock = asyncio.Semaphore(concurrency)
        self.domain_last_access = {}

    async def start(self):
        logger.info("Starting Playwright...")
        self.playwright = await async_playwright().start()
        logger.info("Launching browser...")
        self.browser = await self.playwright.chromium.launch(headless=True, args=["--no-sandbox"])
        logger.info("Browser launched.")

    async def stop(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
            
    async def allowed_by_robots(self, url):
        domain = urlparse(url).netloc
        if domain in self.__robots_cache:
            return self.__robots_cache[domain]
        try:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(f"https://{domain}/robots.txt")
            rp.read()
            allowed = rp.can_fetch(self.user_agent, url)
        except:
            allowed = True  # fail open if robots.txt missing
        self.__robots_cache[domain] = allowed
        return allowed

    async def _enforce_delay(self, domain):
        last = self.domain_last_access.get(domain)
        if last:
            wait = max(0, np.random.uniform(*self.per_domain_delay) - (time.time() - last))
            if wait > 0:
                logger.info(f"Enforcing delay of {wait:.2f}s for domain {domain}")
                await asyncio.sleep(wait)
        self.domain_last_access[domain] = time.time()

    async def fetch(self, url, render_js=False, timeout=10000):  # Increased timeout to 6s
        domain = domain_from_url(url)
        logger.info(f"Checking robots.txt for {url}")
        if not await self.allowed_by_robots(url):
            logger.warning(f"Blocked by robots.txt: {url}")
            return None, None, True

        logger.info(f"Enforcing delay for {domain}")
        await self._enforce_delay(domain)
        async with self.lock:
            try:
                logger.info(f"Creating new context for {url}")
                context = await self.browser.new_context(user_agent=self.user_agent, viewport={"width":1280,"height":800}) if render_js else await self.browser.new_context(user_agent=self.user_agent)
                logger.info(f"Creating new page for {url}")
                page = await context.new_page()
                logger.info(f"Navigating to {url} with timeout {timeout}ms")
                response = await page.goto(url, timeout=timeout)
                logger.info(f"Navigation complete, status: {response.status if response else 'None'}")
                try:
                    logger.info(f"Waiting for load state (networkidle) for {url}")
                    await page.wait_for_load_state("networkidle", timeout=timeout)
                except Exception as e:
                    logger.warning(f"Networkidle timeout for {url}: {e}. Falling back to domcontentloaded.")
                    await page.wait_for_load_state("domcontentloaded", timeout=timeout)
                logger.info(f"Getting page content for {url}")
                html = await page.content()
                status = response.status if response else None
                await page.close()
                await context.close()
                logger.info(f"Fetch successful for {url}, status: {status}")
                return status, html, False
            except Exception as e:
                logger.exception(f"Error fetching {url}: {e}")
                return None, None, False