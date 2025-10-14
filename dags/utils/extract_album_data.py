import logging
import os
from datetime import datetime
from typing import List

import pandas as pd
import requests
from airflow.decorators import task


@task
def extract_album_data(
    token: str,
    time_range: str,
    types: str,
    markets: List[str],
    output_dir: str,
) -> bool:
    """
    Extract data from spotify with time range and types
    """
    try:
        for market in markets:
            url = f"https://api.spotify.com/v1/search?q=year:{time_range}&type={types}&market={market}&limit=50"
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
            response = requests.get(url, headers=headers, timeout=10)

            response.raise_for_status()

            data = response.json()

            items_key = f"{types}s"

            if items_key in data and "items" in data[items_key]:
                items = data[items_key]["items"]
                if items:
                    album_datas = []  # store data in a list
                    for artist in items["artists"]:
                        album_data = {
                            "album_id": items["id"],
                            "album_type": items["album_type"],
                            "total_tracks": items["total_tracks"],
                            "name": items["name"],
                            "release_date": items["release_date"],
                            "market": items["market"],
                            "artist_id": artist["id"],
                            "artist_name": artist["name"],
                            "extraction_time": datetime.fromtimestamp(
                                items["extraction_time"]
                            ).strftime("%Y-%m-%d %H:%M:%S"),
                        }
                        album_datas.append(album_data)

                    df = pd.DataFrame(album_datas, index=[0])

                    # create directory for each types and markets
                    current_date = datetime.now().strftime("%Y-%m-%d")

                    subdir = os.path.join(
                        output_dir, "data", "raw", market, current_date
                    )
                    os.makedirs(subdir, exist_ok=True)

                    filename = os.path.join(subdir, f"search_{types}.csv")

                    # CSV output
                    df.to_csv(filename, index=False)

                    # JSON output
                    json_path = os.path.join(subdir, f"search_{types}.json")
                    df.to_json(
                        json_path,
                        orient="records",
                        lines=False,
                        indent=2,
                        force_ascii=False,
                    )

                    logging.info(f"{types.upper()} data for {market}")

                else:
                    logging.warning(f"{types} data for {market} not found")
            else:
                logging.warning(f" Invalid response for {types} in {market}")
        return True

    except Exception as e:
        logging.error(f"error for extracting : {str(e)}")

        return False
