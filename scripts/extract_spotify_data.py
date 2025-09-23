import requests

time_range = "2015-2025"
type = ["track", "artist", "album"]


def extract_spotify_data(token: str, time_range: str):
    for search_type in type:
        url = f"https://api.spotify.com/v1/search?q={time_range}&type={search_type}"
        headers = {
            "Authorization": f"Bearer {token}",
            "content-Type": "application/json",
        }
        response = requests.get(url, headers=headers)
        data = response.json()
