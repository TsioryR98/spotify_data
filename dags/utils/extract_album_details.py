import logging
from typing import List
import os
import pandas as pd
from datetime import datetime
import requests
from airflow.decorators import task
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.discover_album_data import discover_album_data


def fetch_album_data(album_id: str, token: str) -> dict:
    """Fetch album data from Spotify API."""
    url = f"https://api.spotify.com/v1/albums/{album_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


@task
def extract_album_details(
    token: str, markets: List[str], output_dir: str, max_workers: int = 10
) -> bool:
    """
    Extract data for each album and ids from Spotify in parallel.
    Saves album details, tracks, and artists CSV per market/date.
    """
    try:
        for market in markets:
            logging.info(f"Processing market: {market}")

            # Read album IDs
            csv_path = os.path.join(
                discover_album_data(output_dir, market), "search_album.csv"
            )
            df = pd.read_csv(csv_path)
            album_list_id = df["album_id"].tolist()

            # Accumulate results
            album_details, album_tracks, album_artists = [], [], []

            # Fetch album data in parallel
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(fetch_album_data, album_id, token)
                    for album_id in album_list_id
                ]
                for future in as_completed(futures):
                    data = future.result()

                    # Album details
                    album_details.append(
                        {
                            "id": data["id"],
                            "album_type": data["album_type"],
                            "total_tracks": data["total_tracks"],
                            "name": data["name"],
                            "release_date": data["release_date"],
                            "popularity": data["popularity"],
                        }
                    )

                    # Tracks
                    total_tracks = data["tracks"]["total"]
                    for track in data["tracks"]["items"]:
                        album_tracks.append(
                            {
                                "id": data["id"],
                                "track_id": track["id"],
                                "duration_ms": track["duration_ms"],
                                "name": track["name"],
                                "disc_number": track["disc_number"],
                                "total": total_tracks,
                            }
                        )

                    # Artists
                    for artist in data["artists"]:
                        album_artists.append(
                            {
                                "id": data["id"],
                                "artist_id": artist["id"],
                                "artist_name": artist["name"],
                            }
                        )

            # Save CSVs per market/date
            current_date = datetime.now().strftime("%Y-%m-%d")
            subdir = os.path.join(output_dir, "data", "raw", market, current_date)
            os.makedirs(subdir, exist_ok=True)

            pd.DataFrame(album_details).to_csv(
                os.path.join(subdir, "albums_details.csv"), index=False
            )
            pd.DataFrame(album_tracks).to_csv(
                os.path.join(subdir, "albums_tracks.csv"), index=False
            )
            pd.DataFrame(album_artists).to_csv(
                os.path.join(subdir, "albums_artists.csv"), index=False
            )

            logging.info(f"Market {market} done. Data saved in {subdir}")

        return True

    except requests.exceptions.RequestException as e:
        logging.error(f"Error extracting album data: {str(e)}")
        return False
