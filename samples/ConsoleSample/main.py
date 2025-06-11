#!/usr/bin/env python

import json
import os
from twisted.internet import reactor
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAApplicationAuthReq, ProtoOAAccountAuthReq

# Load credentials
with open(os.path.expanduser("~/cTrade/creds.json"), "r") as f:
    creds = json.load(f)

clientId = creds["clientId"]
clientSecret = creds["clientSecret"]
accountId = creds["accountId"]
accessToken = creds["accessToken"]
connectionType = creds.get("connectionType", "live").lower()

host = EndPoints.PROTOBUF_LIVE_HOST if connectionType == "live" else EndPoints.PROTOBUF_DEMO_HOST
port = EndPoints.PROTOBUF_PORT

client = Client(host=host, port=port, protocol=TcpProtocol)

def connected(_):
    print("\n✅ Connected")
    # Step 1: Authenticate app
    authMsg = ProtoOAApplicationAuthReq()
    authMsg.clientId = clientId
    authMsg.clientSecret = clientSecret
    deferred = client.send(authMsg)

    def afterAppAuth(_):
        print("\n🎉 API Application authorized\n")
        # Step 2: Authenticate account
        accountAuth = ProtoOAAccountAuthReq()
        accountAuth.ctidTraderAccountId = accountId
        accountAuth.accessToken = accessToken
        return client.send(accountAuth)

    deferred.addCallback(afterAppAuth)
    deferred.addCallback(lambda _: print(f"\n🔐 Account {accountId} authorized.\n"))
    deferred.addErrback(onError)

def disconnected(_, reason):
    print(f"\n🔌 Disconnected: {reason}")

def onError(err):
    print("❌ Error during connection or authentication:")
    print(err)
    reactor.stop()

client.setConnectedCallback(connected)
client.setDisconnectedCallback(disconnected)
client.setMessageReceivedCallback(lambda _, m: None)  # Silence all extra messages

client.startService()
reactor.run()
