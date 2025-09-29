import base64
import os
from airflow.decorators import task

import requests
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("client_id")
CLIENT_SECRET = os.getenv("client_secret")


@task
def request_auth_api(CLIENT_ID: str, CLIENT_SECRET: str) -> str:

    # token as need base 64 encoded string
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}".encode("utf-8")
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
