import asyncio, time, random
from fetcher import PageFetcher
from extractor import Extractor
from db import SessionLocal
from models import Resort, RawPage, ExtractionLog
from logger_conf import setup_logger
from utils import domain_from_url, sleep_random
import requests
from bs4 import BeautifulSoup

logger = setup_logger("crawler")

class Crawler:
    def __init__(self, config):
        self.config = config
        self.fetcher = PageFetcher(user_agent=config['user_agent'], concurrency=config['concurrency'], per_domain_delay=tuple(config['per_domain_delay_seconds']))
        self.session = SessionLocal()
        self.extractor = Extractor(self.session)

    async def start(self):
        await self.fetcher.start()

    async def stop(self):
        await self.fetcher.stop()
        self.session.close()

    async def discover_urls(self):
        """
        Multi-mode discovery:
        1. DuckDuckGo search with region/safesearch
        2. Optional Bing API (if api_key in config)
        3. Seed URLs from config
        Returns a deduplicated list of URLs up to max_discovered_urls
        """
        urls = set()

        # --- 1. Seed URLs first ---
        seed_urls = self.config.get("seed_urls", [])
        for u in seed_urls:
            urls.add(u)

        # --- 2. DuckDuckGo search ---
        from duckduckgo_search import DDGS
        queries = self.config.get("seed_search_queries", [])
        with DDGS() as ddgs:
            for q in queries:
                logger.info("Discovering for query: %s", q)
                try:
                    results = ddgs.text(q, region="wt-wt", safesearch="off", max_results=50)
                    for r in results:
                        href = r.get("href")
                        if href:
                            urls.add(href)
                except Exception as e:
                    logger.warning("DuckDuckGo search failed for '%s': %s", q, e)
                await sleep_random(1.0, 2.0)
                if len(urls) >= self.config["max_discovered_urls"]:
                    break

        # --- 3. Optional Bing API fallback ---
        if "bing_api_key" in self.config:
            import requests
            headers = {"Ocp-Apim-Subscription-Key": self.config["bing_api_key"]}
            for q in queries:
                try:
                    resp = requests.get(
                        "https://api.bing.microsoft.com/v7.0/search",
                        headers=headers,
                        params={"q": q, "count": 50},
                        timeout=10
                    )
                    for r in resp.json().get("webPages", {}).get("value", []):
                        urls.add(r.get("url"))
                except Exception as e:
                    logger.warning("Bing search failed for '%s': %s", q, e)
                await sleep_random(1.0, 2.0)
                if len(urls) >= self.config["max_discovered_urls"]:
                    break

        # --- 4. Clean and limit results ---
        # simple filter: keep only http/https URLs
        urls = [u for u in urls if u.startswith("http")]
        urls = urls[:self.config["max_discovered_urls"]]
        logger.info("Discovered %d URLs", len(urls))
        return urls

    async def process_url(self, url):
        for attempt in range(self.config['max_retries']):
            status, html, blocked = await self.fetcher.fetch(url, render_js=True)
            if blocked:
                logger.warning("Skipped due to robots.txt: %s", url)
                return
            if html:
                # store raw page
                rp = RawPage(url=url, domain=domain_from_url(url), status_code=status, html=html)
                self.session.add(rp)
                self.session.commit()
                # extract
                extracted = self.extractor.extract_all(html)
                # build normalized resort record
                resort = self.normalize_to_resort(url, extracted)
                if resort:
                    # upsert by URL
                    existing = self.session.query(Resort).filter(Resort.url==url).first()
                    if existing:
                        # update fields if present
                        for k,v in resort.items():
                            if v is not None:
                                setattr(existing, k, v)
                        self.session.commit()
                    else:
                        r = Resort(**resort)
                        self.session.add(r)
                        self.session.commit()
                # log extraction outcomes
                for fld, val in extracted.items():
                    if val:
                        elog = ExtractionLog(url=url, field=fld, value=str(val.get('value')), method="hybrid", confidence=val.get('confidence',0.5))
                        self.session.add(elog)
                self.session.commit()
                return
            else:
                await asyncio.sleep(2 ** attempt)
        logger.warning("Failed to fetch after retries: %s", url)

    def normalize_to_resort(self, url, extracted):
        # map extracted dict to Resort fields with lightweight normalization
        if not extracted:
            return None
        def safe(v): return v['value'] if v and 'value' in v else None
        runs = safe(extracted.get('runs_breakdown'))
        return {
            "name": None,
            "url": url,
            "country": None,
            "continent": None,
            "lat": None, "lon": None,
            "snowfall_inches": safe(extracted.get('snowfall')),
            "opening_date": safe(extracted.get('opening_date')),
            "closing_date": safe(extracted.get('closing_date')),
            "num_lifts": safe(extracted.get('num_lifts')),
            "runs_easy": runs.get('easy') if runs else None,
            "runs_intermediate": runs.get('intermediate') if runs else None,
            "runs_advanced": runs.get('advanced') if runs else None,
            "day_pass_usd": safe(extracted.get('day_pass_price')),
            "season_pass_usd": safe(extracted.get('season_pass_price')),
            "raw": {k: v for k,v in extracted.items()}
        }

    async def run(self):
        urls = await self.discover_urls()
        # for efficient concurrency, use asyncio.gather with chunks
        sem = asyncio.Semaphore(self.config['concurrency'])
        async def sem_proc(u):
            async with sem:
                await self.process_url(u)
                await sleep_random(*self.config['per_domain_delay_seconds'])
        tasks = [asyncio.create_task(sem_proc(u)) for u in urls]
        for t in tasks:
            await t
