import asyncio, random, time
from urllib.parse import urlparse
import aiohttp
import logging
from logger_conf import setup_logger
logger = setup_logger("utils")

async def sleep_random(a=1.0, b=3.0):
    await asyncio.sleep(random.uniform(a, b))

def domain_from_url(url):
    try:
        return urlparse(url).netloc
    except:
        return None

# Simple robots.txt checker (synchronous)
import urllib.robotparser
def allowed_by_robots(url, user_agent):
    rp = urllib.robotparser.RobotFileParser()
    parts = urlparse(url)
    robots_url = f"{parts.scheme}://{parts.netloc}/robots.txt"
    try:
        rp.set_url(robots_url)
        rp.read()
        return rp.can_fetch(user_agent, url)
    except Exception as e:
        logger.warning("Robots.txt check failed for %s: %s", robots_url, e)
        return True
