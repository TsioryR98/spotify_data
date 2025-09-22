import base64
import os

import requests
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")


def request_auth_api(client_id: str, client_secret: str) -> str:

    # token as need base 64 encoded string
    auth_str = f"{client_id}:{client_secret}".encode("utf-8")
    auth_base64 = base64.b64encode(auth_str).decode("utf-8")

    url = "https://accounts.spotify.com/api/token"

    headers = {
        "Authorization": f"Basic  {auth_base64}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {"grant_type": "client_credentials"}

    response = requests.post(url, headers=headers, data=data)

    token = response.json()["access_token"]

    return token
