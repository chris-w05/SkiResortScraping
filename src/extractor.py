import re
from bs4 import BeautifulSoup
from dateparser import parse as parse_date
from logger_conf import setup_logger
from pattern_learning import PatternBank
from fuzzywuzzy import fuzz
import spacy


logger = setup_logger("extractor")
nlp = spacy.load("en_core_web_sm")


def textify(html):
    soup = BeautifulSoup(html, "lxml")
    # remove scripts/styles
    for s in soup(["script", "style", "noscript"]):
        s.decompose()
    return soup.get_text(separator=" ", strip=True)

def to_inches(value, unit):
    unit = (unit or "").lower()
    try:
        v = float(value)
    except:
        return None
    if "cm" in unit:
        return v / 2.54
    return v  # assuming inches

# Expanded regex patterns
DEFAULT_REGEXES = {
    "name": [r"<title>(.*?)</title>", r"<h1.*?>(.*?)</h1>"],  # HTML-based
    "snowfall": [
        r"([0-9]{1,3}(?:\.[0-9])?)\s*(inches|inch|in|cm)\s*(?:of)?\s*(snow|annual snow|average|season|snowfall)",
        r"Average snowfall[:\s]*([0-9]{1,3}(?:\.[0-9])?)\s*(cm|inches|in)",
        r"Annual average snowfall:\s*([0-9]{1,3})\s*in",  # New
        r"Snowfall\s*:\s*([0-9]{1,3})\s*inches\s*per year"  # New
    ],
    "opening_date": [
        r"(?:season\s*(?:starts|opens|opening)[\s:]*)([A-Za-z]+\s*\d{1,2},?\s*\d{2,4})",
        r"(?:opens\s*on)\s*([A-Za-z]+\s*\d{1,2})",
        r"Opening date:\s*([A-Za-z]+\s*\d{1,2})"  # New
    ],
    "closing_date": [
        r"(?:season\s*(?:ends|closes|closing)[\s:]*)([A-Za-z]+\s*\d{1,2},?\s*\d{2,4})",
        r"(?:closes\s*on)\s*([A-Za-z]+\s*\d{1,2})",
        r"Closing date:\s*([A-Za-z]+\s*\d{1,2})"  # New
    ],
    "num_lifts": [
        r"(\d{1,3})\s*(?:lifts|chairlifts|drag lifts|surface lifts|t-bar)s?",
        r"Total lifts:\s*(\d{1,3})",  # New
        r"Number of lifts:\s*(\d{1,3})"  # New
    ],
    "runs_breakdown": [
        r"(\d+)\s*(?:beginner|easy|green)\b.*?(\d+)\s*(?:intermediate|blue)\b.*?(\d+)\s*(?:advanced|black|expert)",
        r"beginner[:\s]*(\d+).+intermediate[:\s]*(\d+).+advanced[:\s]*(\d+)",
        r"Easy runs:\s*(\d+).*Intermediate:\s*(\d+).*Advanced:\s*(\d+)",  # New
        r"Green:\s*(\d+)%.*Blue:\s*(\d+)%.*Black:\s*(\d+)%",  # New (percentages; can normalize later if needed)
    ],
    "day_pass_price": [
        r"\$\s*(\d{1,4}(?:\.\d{1,2})?)\s*(?:per day|day pass|lift ticket|day ticket)",
        r"Day pass:\s*\$(\d{1,4})",  # New
    ],
    "season_pass_price": [
        r"\$\s*(\d{1,5}(?:\.\d{1,2})?)\s*(?:season pass|season-ticket|season pass price)",
        r"Season pass:\s*\$(\d{1,5})",  # New
    ],
    "country": [r"([A-Z][a-z]+)\s*(?:country|location|resort in)", r"\.([a-z]{2})$"],  # TLD or text
    "continent": [r"(Europe|North America|Asia|South America|Australia|Africa|Antarctica)"],  # Simple
    "lat": [r"lat\s*:\s*([-]?[0-9]{1,3}\.[0-9]{4,})", r'data-lat="([-]?[0-9]{1,3}\.[0-9]{4,})"'],  # Maps/meta
    "lon": [r"lon\s*:\s*([-]?[0-9]{1,3}\.[0-9]{4,})", r'data-lng="([-]?[0-9]{1,3}\.[0-9]{4,})"'],
}

# ... (textify, to_inches remain the same)

class Extractor:
    def __init__(self, session):
        # session is DB session for pattern bank queries
        self.pattern_bank = PatternBank(session)

    def extract_field_regex(self, text, field, soup=None):
        
        patterns = self.pattern_bank.get_patterns(field) or DEFAULT_REGEXES.get(field, [])
        for pat in patterns:
            try:
                m = re.search(pat, text, re.IGNORECASE | re.DOTALL)
                if m:
                    # Expanded parsing logic
                    if field == "snowfall":
                        num = m.group(1)
                        unit = m.group(2) if len(m.groups()) >= 2 else "in"
                        return {"value": to_inches(num, unit), "raw": m.group(0), "confidence": 0.8}
                    if field in ("opening_date", "closing_date"):
                        dt = parse_date(m.group(1))
                        return {"value": dt.date() if dt else None, "raw": m.group(0), "confidence": 0.8}
                    if field == "num_lifts":
                        return {"value": int(m.group(1)), "raw": m.group(0), "confidence": 0.75}
                    if field in ("day_pass_price", "season_pass_price"):
                        return {"value": float(m.group(1)), "raw": m.group(0), "confidence": 0.8}
                    if field == "runs_breakdown":
                        g = m.groups()
                        if len(g) >= 3:
                            return {"value": {"easy": int(g[0]) if g[0].isdigit() else int(g[0].rstrip('%')), "intermediate": int(g[1]) if g[1].isdigit() else int(g[1].rstrip('%')), "advanced": int(g[2]) if g[2].isdigit() else int(g[2].rstrip('%'))}, "raw": m.group(0), "confidence": 0.8}
                    if field == "name" and soup:
                        title = soup.find("title")
                        return {"value": title.text.strip() if title else None, "raw": title.text if title else "", "confidence": 0.9}
                    if field == "country":
                        return {"value": m.group(1), "raw": m.group(0), "confidence": 0.7}
                    if field == "continent":
                        return {"value": m.group(1), "raw": m.group(0), "confidence": 0.7}
                    if field in ("lat", "lon"):
                        return {"value": float(m.group(1)), "raw": m.group(0), "confidence": 0.8}
                    return {"value": m.group(1), "raw": m.group(0), "confidence": 0.6}
            except Exception as e:
                logger.exception("Regex error: %s", e)
        return None

    def extract_spacy(self, text, field):
        doc = nlp(text[:20000])
        if field in ("opening_date", "closing_date"):
            for ent in doc.ents:
                if ent.label_ == "DATE":
                    dt = parse_date(ent.text)
                    if dt:
                        return {"value": dt.date(), "raw": ent.text, "confidence": 0.6}
        if field in ("day_pass_price", "season_pass_price"):
            for ent in doc.ents:
                if ent.label_ == "MONEY":
                    v = re.sub(r"[^\d\.]", "", ent.text)
                    try:
                        return {"value": float(v), "raw": ent.text, "confidence": 0.6}
                    except:
                        continue
        if field == "country" or field == "continent":
            for ent in doc.ents:
                if ent.label_ == "GPE" or ent.label_ == "LOC":
                    return {"value": ent.text, "raw": ent.text, "confidence": 0.6}
        return None

    def extract_all(self, html):
        soup = BeautifulSoup(html, "lxml")
        text = textify(html)
        result = {}
        fields = ["name", "country", "continent", "lat", "lon", "snowfall", "opening_date", "closing_date", "num_lifts", "runs_breakdown", "day_pass_price", "season_pass_price"]
        for f in fields:
            out = self.extract_field_regex(html if f in ["lat", "lon", "name"] else text, f, soup=soup)  # Use raw HTML for some
            if not out:
                out = self.extract_spacy(text, f)
            if not out:
                candidate = self.find_candidate_and_save_pattern(text, f)
                if candidate:
                    out = candidate
            result[f] = out
        return result

    def find_candidate_and_save_pattern(self, text, field):
        # Improved heuristic: higher fuzzy threshold, more keywords
        keywords = {
            "name": ["resort name", "welcome to"],
            "snowfall": ["snowfall", "annual snow", "average snowfall", "avg snowfall", "annual snowfall", "snow depth"],
            "opening_date": ["season opens","opens on","season starts","opening day", "open from"],
            "closing_date": ["season ends","closes on","closing day","season closes", "close on"],
            "num_lifts": ["lifts","chairlifts","total lifts","number of lifts", "lift count"],
            "day_pass_price": ["day pass","day ticket","lift ticket","daily rate"],
            "season_pass_price": ["season pass","season-ticket","season price", "annual pass"],
            "runs_breakdown": ["beginner","intermediate","advanced","runs","trails", "green blue black"],
            "country": ["located in", "country", "address"],
            "continent": ["continent", "region"],
            "lat": ["latitude", "lat", "gps"],
            "lon": ["longitude", "lon", "gps"],
        }
        kws = keywords.get(field, [])
        for kw in kws:
            # Use fuzzy matching for better recall
            matches = [ (m.start(), m.group(0)) for m in re.finditer(re.escape(kw), text.lower()) if fuzz.ratio(kw, m.group(0)) > 80 ]
            for idx, _ in matches:
                start = max(0, idx - 100)
                snippet = text[start: idx + len(kw) + 150]
                # Improved number/unit capture
                m = re.search(r"([0-9]{1,5}(?:\.[0-9]{1,})?)\s*(cm|in|inches|\$|%|km|miles|lifts|runs)?", snippet) or re.search(r"([A-Za-z]+\s*\d{1,2}(?:,\s*\d{4})?)", snippet)  # Dates
                if m:
                    num_or_val = m.group(1)
                    unit = m.group(2) if len(m.groups()) > 1 else ""
                    # Loose pattern: kw + context + capture group
                    pattern = rf"{re.escape(kw)}[\s\:\-\,\w\(\)]{{0,50}}([0-9]{{1,5}}(?:\.[0-9]{{1,}})?|[A-Za-z]+\s*\d{{1,2}}(?:,\s*\d{{4}})?)\s*(cm|in|inches|\$|%|km|miles|lifts|runs)?"
                    self.pattern_bank.add_pattern(field, pattern, source="auto", confidence=0.6)  # Higher confidence
                    # Parse tentative value
                    if field == "snowfall":
                        return {"value": to_inches(num_or_val, unit), "raw": snippet.strip(), "confidence": 0.6}
                    if field in ("day_pass_price", "season_pass_price"):
                        try:
                            return {"value": float(num_or_val), "raw": snippet.strip(), "confidence": 0.6}
                        except:
                            pass
                    if field == "num_lifts":
                        try:
                            return {"value": int(num_or_val), "raw": snippet.strip(), "confidence": 0.6}
                        except:
                            pass
                    if field in ("opening_date", "closing_date"):
                        dt = parse_date(num_or_val)
                        if dt:
                            return {"value": dt.date(), "raw": snippet.strip(), "confidence": 0.6}
                    if field in ("lat", "lon"):
                        try:
                            return {"value": float(num_or_val), "raw": snippet.strip(), "confidence": 0.6}
                        except:
                            pass
                    if field == "country" or field == "continent":
                        return {"value": num_or_val, "raw": snippet.strip(), "confidence": 0.6}
        return None