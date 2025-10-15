from models import ExtractionPattern
from logger_conf import setup_logger
logger = setup_logger("pattern_bank")

class PatternBank:
    def __init__(self, session):
        self.session = session

    def get_patterns(self, field):
        rows = self.session.query(ExtractionPattern).filter(ExtractionPattern.field==field).order_by(ExtractionPattern.confidence.desc()).all()
        return [r.pattern_text for r in rows]

    def add_pattern(self, field, pattern_text, source="auto", confidence=0.5):
        # avoid duplicates
        exists = self.session.query(ExtractionPattern).filter(ExtractionPattern.field==field, ExtractionPattern.pattern_text==pattern_text).first()
        if exists:
            return exists
        p = ExtractionPattern(field=field, pattern_text=pattern_text, source=source, confidence=confidence)
        self.session.add(p)
        self.session.commit()
        logger.info("Added pattern for %s: %s", field, pattern_text)
        return p
