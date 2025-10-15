import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base
from logger_conf import setup_logger

logger = setup_logger("db")

DATABASE_URL = os.environ.get("DATABASE_URL") or "sqlite:///./ski_crawler.db"

engine = create_engine(DATABASE_URL, future=True, echo=False)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)

def init_db():
    Base.metadata.create_all(bind=engine)
    logger.info("Database initialized: %s", DATABASE_URL)
