import logging
import os

def get_enote_link_logger() -> logging.Logger:
    logger = logging.getLogger("enote_link")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    os.makedirs("logs", exist_ok=True)
    fh = logging.FileHandler("logs/enote_link.log", encoding="utf-8")
    fmt = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger
