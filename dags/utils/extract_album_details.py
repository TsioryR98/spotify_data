import logging
from typing import List
import os
import pandas as pd

import requests
from airflow.decorators import task
from utils.discover_album_data import discover_album_data


@task
def extract_album_details(
    token: str,
    markets: List[str],
    output_dir: str,
) -> bool:
    """
    Extract data for each album and ids from spotify
    """
    try:
        for market in markets:
            subdir = os.path.join(output_dir, "data", "raw")
            market_path = discover_album_data(subdir, market)
            df = pd.read_csv(
                os.path.join(market_path, "search_album.csv")
            )  # read csv file with pandas

            album_list_id = df["album_id"].tolist()

            for id in album_list_id:
                url = f"https://api.spotify.com/v1/albums/{id}"
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                }
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()

                data = response.json()

    except requests.exceptions.RequestException as e:
        logging.error(f"error for extracting : {str(e)}")

        return False
