import logging
from typing import List
import os
import pandas as pd
from datetime import datetime


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
                album_details = []
                album_tracks = []
                album_artists = []

                # album detail format
                album_detail = {
                    "id": data["id"],
                    "album_type": data["album_type"],
                    "total_tracks": data["total_tracks"],
                    "name": data["name"],
                    "release_date": data["release_date"],
                    "popularity": data["popularity"],
                }
                album_details.append(album_detail)
                # album tracks format
                album_id = data["id"]
                total_track = data["tracks"]["total"]
                for track in data["tracks"]["items"]:
                    album_track = {
                        "id": album_id,
                        "track_id": track["id"],
                        "duration_ms": track["duration_ms"],
                        "name": track["name"],
                        "disc_number": track["disc_number"],
                        "total": total_track,
                    }
                    album_tracks.append(album_track)
                # album tracks format
                for artist in data["artists"]:
                    album_artist = {
                        "id": album_id,
                        "artist_id": artist["id"],
                        "artist_name": artist["name"],
                    }
                    album_artists.append(album_artist)

        df_album_details = pd.DataFrame(album_details)
        df_album_tracks = pd.DataFrame(album_tracks)
        df_album_artists = pd.DataFrame(album_artists)

        current_date = datetime.now().strftime("%Y-%m-%d")

        subdir = os.path.join(output_dir, "data", "raw", market, current_date)
        os.makedirs(subdir, exist_ok=True)

        df_album_details.to_csv(subdir, "albums_details.csv", index=False)
        df_album_tracks.to_csv(subdir, "df_album_tracks.csv", index=False)
        df_album_artists.to_csv(subdir, "df_album_artists.csv", index=False)

        logging.info(f"data for {market} done")

    except requests.exceptions.RequestException as e:
        logging.error(f"error for extracting : {str(e)}")

        return False
