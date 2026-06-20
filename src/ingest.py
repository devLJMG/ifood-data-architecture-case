from pathlib import Path

import certifi
import requests

from config import BASE_URL, LANDING_DIR, YEAR, MONTHS


def download_file(month: str) -> None:
    filename = f"yellow_tripdata_{YEAR}-{month}.parquet"
    url = f"{BASE_URL}/{filename}"

    output_dir = LANDING_DIR / f"year={YEAR}" / f"month={month}"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / filename

    if output_file.exists():
        print(f"Arquivo já existe: {output_file}")
        return

    print(f"Baixando: {url}")

    response = requests.get(url, stream=True, timeout=120, verify=certifi.where())
    response.raise_for_status()

    with open(output_file, "wb") as file:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                file.write(chunk)

    print(f"Salvo em: {output_file}")


def main() -> None:
    for month in MONTHS:
        download_file(month)


if __name__ == "__main__":
    main()