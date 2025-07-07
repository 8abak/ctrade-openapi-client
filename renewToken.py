#!/usr/bin/env python3

import json
import requests
import os

creds_path = os.path.expanduser("~/cTrade/creds.json")

# Load existing credentials
with open(creds_path, "r") as f:
    creds = json.load(f)

refresh_token = creds["refreshToken"]
client_id = creds["clientId"]
client_secret = creds["clientSecret"]
use_live = creds.get("connectionType", "live").lower() == "live"

# Choose correct token URL
token_url = "https://live-api.ctrader.com/apps/token" if use_live else "https://sandbox-api.ctrader.com/apps/token"

# Request new token
response = requests.post(token_url, data={
    "grant_type": "refresh_token",
    "refresh_token": refresh_token,
    "client_id": client_id,
    "client_secret": client_secret
})

# Handle response
if response.status_code == 200:
    data = response.json()
    creds["accessToken"] = data["access_token"]
    creds["refreshToken"] = data.get("refresh_token", refresh_token)

    with open(creds_path, "w") as f:
        json.dump(creds, f, indent=2)

    print("✅ Token refreshed successfully.")
    print("🔐 New access token:", creds["accessToken"])
else:
    print("❌ Failed to refresh token.")
    print("Status code:", response.status_code)
    print("Response:", response.text)
