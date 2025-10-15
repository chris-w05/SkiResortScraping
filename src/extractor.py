import re
from bs4 import BeautifulSoup
from dateparser import parse as parse_date
from logger_conf import setup_logger
from pattern_learning import PatternBank
from fuzzywuzzy import fuzz
import spacy

logger = setup_logger("extractor")
nlp = spacy.load("en_core_web_sm")

# Basic regex patterns (default bank)
DEFAULT_REGEXES = {
    "snowfall": [
        r"([0-9]{1,3}(?:\.[0-9])?)\s*(inches|inch|in|cm)\s*(?:of)?\s*(snow|annual snow|average|season)",
        r"Average snowfall[:\s]*([0-9]{1,3}(?:\.[0-9])?)\s*(cm|inches|in)"
    ],
    "opening_date": [
        r"(?:season\s*(?:starts|opens|opening)[\s:]*)([A-Za-z]+\s*\d{1,2},?\s*\d{2,4})",
        r"(?:opens\s*on)\s*([A-Za-z]+\s*\d{1,2})"
    ],
    "closing_date": [
        r"(?:season\s*(?:ends|closes|closing)[\s:]*)([A-Za-z]+\s*\d{1,2},?\s*\d{2,4})",
        r"(?:closes\s*on)\s*([A-Za-z]+\s*\d{1,2})"
    ],
    "num_lifts": [ r"(\d{1,3})\s*(?:lifts|chairlifts|drag lifts|surface lifts|t-bar)s?" ],
    "day_pass_price": [ r"\$\s*(\d{1,4}(?:\.\d{1,2})?)\s*(?:per day|day pass|lift ticket|day ticket)" ],
    "season_pass_price": [ r"\$\s*(\d{1,5}(?:\.\d{1,2})?)\s*(?:season pass|season-ticket|season pass price)" ],
    "runs_breakdown": [ r"(\d+)\s*(?:beginner|easy|green)\b.*?(\d+)\s*(?:intermediate|blue)\b.*?(\d+)\s*(?:advanced|black|expert)", r"beginner[:\s]*(\d+).+intermediate[:\s]*(\d+).+advanced[:\s]*(\d+)" ]
}

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

class Extractor:
    def __init__(self, session):
        # session is DB session for pattern bank queries
        self.pattern_bank = PatternBank(session)

    def extract_field_regex(self, text, field):
        patterns = self.pattern_bank.get_patterns(field) or DEFAULT_REGEXES.get(field, [])
        for pat in patterns:
            try:
                m = re.search(pat, text, re.IGNORECASE|re.DOTALL)
                if m:
                    # different fields are interpreted differently
                    if field == "snowfall":
                        num = m.group(1)
                        unit = m.group(2) if len(m.groups())>=2 else "in"
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
                            return {"value": {"easy":int(g[0]), "intermediate":int(g[1]), "advanced":int(g[2])}, "raw":m.group(0), "confidence":0.8}
                        continue
                    # fallback
                    return {"value": m.group(1), "raw": m.group(0), "confidence": 0.6}
            except Exception as e:
                logger.exception("Regex error: %s", e)
        return None

    def extract_spacy(self, text, field):
        # use spaCy for dates and MONEY recognition as a fallback
        doc = nlp(text[:20000])  # limit size
        if field in ("opening_date", "closing_date"):
            for ent in doc.ents:
                if ent.label_ in ("DATE",):
                    dt = parse_date(ent.text)
                    if dt:
                        return {"value": dt.date(), "raw": ent.text, "confidence": 0.6}
        if field in ("day_pass_price","season_pass_price"):
            for ent in doc.ents:
                if ent.label_ == "MONEY":
                    v = re.sub(r"[^\d\.]", "", ent.text)
                    try:
                        return {"value": float(v), "raw": ent.text, "confidence": 0.6}
                    except:
                        continue
        return None

    def extract_all(self, html):
        text = textify(html)
        result = {}
        fields = ["snowfall","opening_date","closing_date","num_lifts","runs_breakdown","day_pass_price","season_pass_price"]
        for f in fields:
            out = self.extract_field_regex(text, f)
            if not out:
                out = self.extract_spacy(text, f)
            # if still not found, attempt weak label search and create candidate pattern
            if not out:
                candidate = self.find_candidate_and_save_pattern(text, f)
                if candidate:
                    out = candidate
            result[f] = out
        return result

    def find_candidate_and_save_pattern(self, text, field):
        # heuristic: search for anchor keywords near numeric tokens
        keywords = {
            "snowfall": ["snowfall", "annual snow", "average snowfall", "avg snowfall", "annual snowfall"],
            "opening_date": ["season opens","opens on","season starts","opening day"],
            "closing_date": ["season ends","closes on","closing day","season closes"],
            "num_lifts": ["lifts","chairlifts","total lifts","number of lifts"],
            "day_pass_price": ["day pass","day ticket","lift ticket","day ticket"],
            "season_pass_price": ["season pass","season-ticket","season pass price"],
            "runs_breakdown": ["beginner","intermediate","advanced","runs","trails"]
        }
        kws = keywords.get(field, [])
        # simple sliding window:
        for kw in kws:
            idx = text.lower().find(kw)
            if idx >= 0:
                # capture 150 chars around it
                start = max(0, idx-80)
                snippet = text[start: idx+len(kw)+100]
                # look for nearest numeric + unit pattern
                m = re.search(r"([0-9]{1,3}(?:\.[0-9])?)\s*(cm|in|inches|\$)?", snippet)
                if m:
                    # create a regex from snippet context: take up to 10 chars before & after the number
                    num = m.group(1)
                    # create a loose pattern anchored to kw
                    pattern = rf"{kw}[\s\:\-\,\w\(\)]{{0,40}}([0-9]{{1,4}}(?:\.[0-9])?)\s*(cm|in|inches|\$)?"
                    # store in pattern bank
                    self.pattern_bank.add_pattern(field, pattern, source="auto", confidence=0.5)
                    # return tentative parsed value
                    unit = m.group(2) or ""
                    if field=="snowfall":
                        return {"value": to_inches(num, unit), "raw": snippet.strip(), "confidence":0.5}
                    if field in ("day_pass_price","season_pass_price"):
                        try:
                            return {"value": float(num), "raw": snippet.strip(), "confidence":0.5}
                        except:
                            pass
                    if field == "num_lifts":
                        try:
                            return {"value": int(num), "raw": snippet.strip(), "confidence":0.5}
                        except:
                            pass
        return None
