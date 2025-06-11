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

host = "live.ctraderapi.com" if connectionType == "live" else "demo.ctraderapi.com"
port = 5035
protocol = "protobuf"

# Create client
client = Client(host=host, port=port, protocol=protocol)

# Set callbacks
def on_connected(_):
    print("✅ Connected to cTrader.")
    authMsg = Factory.build_payload(ProtoOAAccountAuthReq, accountId=accountId)
    client.send(authMsg).addCallback(on_auth_response).addErrback(on_error)

def on_auth_response(message):
    print("🎉 Authenticated successfully!")
    print(message)
    reactor.stop()

def on_error(error):
    print("❌ Error during connection or authentication:")
    print(error)
    reactor.stop()

client.setConnectedCallback(on_connected)
client.setDisconnectedCallback(lambda _, reason: print(f"🔌 Disconnected: {reason}"))
client.setMessageReceivedCallback(lambda _, msg: print(f"📩 Message: {msg.payloadType}"))

# Start the client service and reactor
client.startService()
reactor.run()
