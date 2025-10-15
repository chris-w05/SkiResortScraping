import logging, sys

def setup_logger(name="ski_crawler", level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(level)
        fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
        ch.setFormatter(fmt)
        logger.addHandler(ch)
    return logger
