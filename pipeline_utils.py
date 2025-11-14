from pathlib import Path

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
DB_PATH = BASE_DIR / "ecom.db"


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(exist_ok=True)


def ensure_reports_dir() -> None:
    REPORTS_DIR.mkdir(exist_ok=True)

