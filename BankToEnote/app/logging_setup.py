# logging_setup.py
import logging, sys
from pathlib import Path
def setup_logging(level: str = "INFO", log_file: str | None = None):
    lvl = getattr(logging, level.upper(), logging.INFO)
    logging.root.handlers.clear()
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(level=lvl, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s", handlers=handlers)
