#!/usr/bin/env python

import json
import os
from twisted.internet import reactor
from ctrader_open_api import Client, Protobuf, TcpProtocol, EndPoints
from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOAApplicationAuthReq, ProtoOAAccountAuthReq
from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOASubscribeSpotsReq, ProtoOAUnsubscribeSpotsReq, ProtoOASpotEvent


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
    deferred.addCallback(lambda _: subscribeToSpot(creds["symbolId"]))
    deferred.addErrback(onError)

def onMessage(clientRef, message):
    from ctrader_open_api.messages.OpenApiMessages_pb2 import ProtoOASpotEvent
    if message.payloadType == ProtoOASpotEvent().payloadType:
        try:
            spot = Protobuf.extract(message)
            if hasattr(spot, "quote"):
                for quote in spot.quote:
                    print(f"💰 Spot Update - symbolId {quote.symbolId}: bid={quote.bid}, ask={quote.ask}")
            else:
                print("⚠️ Spot message received, but no quotes inside.")
        except Exception as e:
            print(f"❌ Failed to parse spot event: {e}")



def disconnected(_, reason):
    print(f"\n🔌 Disconnected: {reason}")

def onError(err):
    print("❌ Error during connection or authentication:")
    print(err)
    reactor.stop()

def subscribeToSpot(symbolId, timeoutInSeconds=15):
    print(f"📡 Subscribing to spot data for symbolId {symbolId} for {timeoutInSeconds} seconds...")

    req = ProtoOASubscribeSpotsReq()
    req.ctidTraderAccountId = accountId
    req.symbolId.append(symbolId)
    req.subscribeToSpotTimestamp = True

    client.send(req)

    # Schedule unsubscribe
    reactor.callLater(timeoutInSeconds, unsubscribeFromSpot, symbolId)

def unsubscribeFromSpot(symbolId):
    print(f"🛑 Unsubscribing from spot data for symbolId {symbolId}...")
    req = ProtoOAUnsubscribeSpotsReq()
    req.ctidTraderAccountId = accountId
    req.symbolId.append(symbolId)
    client.send(req)




client.setConnectedCallback(connected)
client.setDisconnectedCallback(disconnected)
client.setMessageReceivedCallback(onMessage)

client.startService()
reactor.run()
