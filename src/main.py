import asyncio, yaml, os
from db import init_db
from crawler import Crawler
from logger_conf import setup_logger

logger = setup_logger("main")

def load_config():
    with open(os.path.join(os.path.dirname(__file__), "..", "config.yaml"), "r") as f:
        cfg = yaml.safe_load(f)
    # ensure numeric types
    cfg.setdefault("concurrency", 4)
    cfg.setdefault("per_domain_delay_seconds", [1.0,3.0])
    cfg.setdefault("max_retries", 3)
    return cfg

async def main():
    cfg = load_config()
    init_db()
    crawler = Crawler(cfg)
    await crawler.start()
    try:
        await crawler.run()
    finally:
        await crawler.stop()

if __name__ == "__main__":
    asyncio.run(main())
