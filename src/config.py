from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = BASE_DIR / "data"

LANDING_DIR = DATA_DIR / "landing" / "yellow_taxi"
CONSUMPTION_DIR = DATA_DIR / "consumption" / "yellow_taxi_trips"

YEAR = 2023
MONTHS = ["01", "02", "03", "04", "05"]

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"