from sqlalchemy import Column, Integer, String, Float, Date, DateTime, JSON, Text, Boolean, ForeignKey, func
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import expression

Base = declarative_base()

class Resort(Base):
    __tablename__ = "resorts"
    id = Column(Integer, primary_key=True)
    name = Column(String, index=True)
    url = Column(String, unique=True, index=True)
    country = Column(String)
    continent = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    snowfall_inches = Column(Float)
    opening_date = Column(Date)
    closing_date = Column(Date)
    num_lifts = Column(Integer)
    runs_easy = Column(Integer)
    runs_intermediate = Column(Integer)
    runs_advanced = Column(Integer)
    day_pass_usd = Column(Float)
    season_pass_usd = Column(Float)
    raw = Column(JSON)  # store raw extracted values and provenance
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())

class RawPage(Base):
    __tablename__ = "raw_pages"
    id = Column(Integer, primary_key=True)
    url = Column(String, index=True)
    domain = Column(String)
    status_code = Column(Integer)
    html = Column(Text)
    discovered_at = Column(DateTime, server_default=func.now())
    processed = Column(Boolean, server_default=expression.false())

class ExtractionPattern(Base):
    __tablename__ = "extraction_patterns"
    id = Column(Integer, primary_key=True)
    field = Column(String, index=True)  # e.g. snowfall, opening_date
    pattern_text = Column(String)       # regex or spaCy pattern JSON
    source = Column(String)             # auto or human
    confidence = Column(Float, default=0.5)
    created_at = Column(DateTime, server_default=func.now())

class ExtractionLog(Base):
    __tablename__ = "extraction_logs"
    id = Column(Integer, primary_key=True)
    url = Column(String, index=True)
    field = Column(String)
    value = Column(String)
    method = Column(String)  # regex/spacy/pattern_bank
    confidence = Column(Float)
    timestamp = Column(DateTime, server_default=func.now())
