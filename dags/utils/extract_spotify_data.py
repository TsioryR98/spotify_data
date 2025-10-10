import logging
import os
from datetime import datetime
from typing import List

import pandas as pd
import requests
from airflow.decorators import task


@task
def extract_spotify_data(
    token: str,
    time_range: str,
    types: List[str],
    markets: List[str],
    output_dir: str,
) -> bool:
    """
    Extract data from spotify with time range and types
    """
    try:
        for search_type in types:
            for market in markets:
                url = f"https://api.spotify.com/v1/search?q=year:{time_range}&type={search_type}&market={market}&limit=50"
                headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                }
                response = requests.get(url, headers=headers, timeout=10)

                response.raise_for_status()

                data = response.json()

                items_key = f"{search_type}s"

                if items_key in data and "items" in data[items_key]:
                    items = data[items_key]["items"]
                    if items:
                        df = pd.DataFrame(items)

                        df["market"] = market
                        df["search_type"] = search_type
                        df["time_range"] = time_range
                        df["extraction_time"] = pd.Timestamp.now().floor("h")

                        # create directory for each types and markets
                        subdir = os.path.join(
                            output_dir, "data", f"{search_type}s", market
                        )
                        os.makedirs(subdir, exist_ok=True)

                        current_date = datetime.now().strftime("%Y-%m-%d")
                        filename = os.path.join(subdir, f"{current_date}.csv")

                        # CSV output
                        df.to_csv(filename, index=False)

                        # JSON output
                        json_path = os.path.join(subdir, f"{current_date}.json")
                        df.to_json(
                            json_path,
                            orient="records",
                            lines=False,
                            indent=2,
                            force_ascii=False,
                        )

                        logging.info(f"{search_type.upper()} data for {market}")

                    else:
                        logging.warning(f"{search_type} data for {market} not found")
                else:
                    logging.warning(f" Invalid response for {search_type} in {market}")
        return True

    except Exception as e:
        logging.error(f"error for extracting : {str(e)}")

        return False
