import os
from datetime import datetime


def discover_album_data(base_path: str, market: str) -> str:
    """
    find latest base_dir for extract album details from csv file
    """

    market_path = os.path.join(base_path, "data", "raw", market)

    # folder (format YYYY-MM-DD) using Python

    date_folders = [
        folder
        for folder in os.listdir(market_path)
        if os.path.isdir(os.path.join(market_path, folder))
    ]
    dates = []
    for f in date_folders:

        try:
            dates.append(
                datetime.strptime(f, "%Y-%m-%d")
            )  # into date format with  strptime
        except ValueError:
            continue

    latest_date = max(dates)
    return os.path.join(market_path, latest_date.strftime("%Y-%m-%d"))
