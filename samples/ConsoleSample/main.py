import json
import os
from twisted.internet import reactor
from ctrader_open_api.client import Client
from ctrader_open_api.factory import Factory
from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAAccountAuthReq

# Load credentials
with open(os.path.expanduser("~/cTrade/creds.json"), "r") as f:
    creds = json.load(f)

clientId = creds["clientId"]
clientSecret = creds["clientSecret"]
accountId = creds["accountId"]
connectionType = creds.get("connectionType", "Live").lower()

client = Client(clientId=clientId, clientSecret=clientSecret, environment=connectionType)

def on_ready():
    print("✅ Connected to cTrader. Sending account auth request...")
    msg = Factory.build_payload(ProtoOAAccountAuthReq, accountId=accountId)
    client.send(msg)

def on_auth_response(message):
    print("🎉 Authentication Success!")
    print(message)
    reactor.stop()

def on_error(e):
    print("❌ Error occurred:", e)
    reactor.stop()

client.on("connected", on_ready)
client.on("ProtoOAAccountAuthRes", on_auth_response)
client.on("error", on_error)

client.connect()
reactor.run()
