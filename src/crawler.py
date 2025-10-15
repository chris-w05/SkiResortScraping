import datetime
import re
import asyncio, time, random
from fetcher import PageFetcher
from extractor import Extractor
from db import SessionLocal
from models import Resort, RawPage, ExtractionLog
from logger_conf import setup_logger
from utils import domain_from_url, sleep_random
import requests
from bs4 import BeautifulSoup
from duckduckgo_search import DDGS
from urllib.parse import urljoin

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
        urls = set()
        
        # Hardcoded seed list pages for autonomy (based on reliable sources; can be config['seed_list_urls'])
        seed_list_pages = self.config.get("seed_search_queries", [])
        # Handle pagination for skiresort.info (up to 5 pages for ~250 resorts; adjust as needed)
        # for page_num in range(1, 6):  # Limits to initial pages to avoid overload
        #     paginated_url = f"https://www.skiresort.info/ski-resorts/page/{page_num}/"
        #     seed_list_pages.append(paginated_url)
        
        # Fetch and parse seed list pages for resort links
        for list_url in seed_list_pages:
            logger.info("Crawling list page for resorts: %s", list_url)
            status, html, blocked = await self.fetcher.fetch(list_url, render_js=False)
            if blocked or not html:
                continue
            soup = BeautifulSoup(html, "lxml")
            # Extract links likely to be resorts (improved patterns)
            for a in soup.find_all("a", href=True):
                logger.info("Checking link: %s", a)
                href = a['href']
                text_lower = a.text.lower()
                if any(term in href.lower() or term in text_lower for term in ["ski-resort", "resort", "ski-area", "skiing", "powderhounds.com/", "snowmagazine.com/ski-resort-guide"]):
                    full_url = urljoin(list_url, href)
                    if full_url.startswith("http") and "wikipedia.org" not in full_url or "en.wikipedia.org/wiki/" in full_url:  # Include wiki resort pages
                        urls.add(full_url)
                    # If aggregator/review (e.g., skiresort.info, powderhounds, snowmagazine), fetch and extract official homepage
                    if any(domain in full_url for domain in ["skiresort.info", "powderhounds.com", "snowmagazine.com", "onthesnow.com"]):
                        off_status, off_html, _ = await self.fetcher.fetch(full_url)
                        if off_html:
                            off_soup = BeautifulSoup(off_html, "lxml")
                            # Improved selector: look for "official", "homepage", "website", or class/id patterns
                            official_link = off_soup.find("a", string=re.compile(r"(official|homepage|website|visit site)", re.I)) or \
                                            off_soup.find("a", attrs={"class": re.compile(r"(external|link|official)", re.I)}) or \
                                            off_soup.find("a", href=re.compile(r"(ski|resort|official)\.(com|net|org|at|ch|fr|it|ca|jp)"))
                            if official_link and 'href' in official_link.attrs:
                                off_href = urljoin(full_url, official_link['href'])
                                urls.add(off_href)
        queries = self.config.get("additional_queries", [])
        with DDGS() as ddgs:
            for q in queries:
                logger.info("Discovering for query: %s", q)
                try:
                    results = ddgs.text(q, region="wt-wt", safesearch="off", max_results=500)
                    for r in results:
                        href = r.get("href")
                        if href and any(term in href.lower() for term in ["resort", "ski", "snow", "mountain"]):
                            urls.add(href)
                except Exception as e:
                    logger.warning("DuckDuckGo search failed for '%s': %s", q, e)
        
        # Filter out duplicates and limit
        urls = list(urls)[:self.config['max_discovered_urls']]
        logger.info("Discovered %d unique URLs", len(urls))
        return urls

    async def process_url(self, url):
        for attempt in range(self.config['max_retries']):
            status, html, blocked = await self.fetcher.fetch(url, render_js=False)
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
        if not extracted:
            return None

        def safe(v, for_json=False):
            if v is None:
                return None
            val = v.get('value') if isinstance(v, dict) else v
            if isinstance(val, (datetime.date, datetime.datetime)):
                if for_json:
                    return val.isoformat()   # for JSON storage
                return val.date() if isinstance(val, datetime.datetime) else val  # for Date columns
            return val

        runs = safe(extracted.get('runs_breakdown'))
        return {
            "name": safe(extracted.get('name')),
            "url": url,
            "country": safe(extracted.get('country')),
            "continent": safe(extracted.get('continent')),
            "lat": safe(extracted.get('lat')),
            "lon": safe(extracted.get('lon')),
            "snowfall_inches": safe(extracted.get('snowfall')),
            "opening_date": safe(extracted.get('opening_date')),   # keep as date
            "closing_date": safe(extracted.get('closing_date')),   # keep as date
            "num_lifts": safe(extracted.get('num_lifts')),
            "runs_easy": runs.get('easy') if runs else None,
            "runs_intermediate": runs.get('intermediate') if runs else None,
            "runs_advanced": runs.get('advanced') if runs else None,
            "day_pass_usd": safe(extracted.get('day_pass_price')),
            "season_pass_usd": safe(extracted.get('season_pass_price')),
            # convert values to JSON-safe for the raw column
            "raw": {k: safe(v, for_json=True) for k, v in extracted.items()}
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
