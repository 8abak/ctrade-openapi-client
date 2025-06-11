#!/usr/bin/env python

import json
import os
import webbrowser
import datetime
import calendar
from twisted.internet import reactor
from inputimeout import inputimeout, TimeoutOccurred
from ctrader_open_api import Client, Protobuf, TcpProtocol, Auth, EndPoints
from ctrader_open_api.messages.OpenApiCommonMessages_pb2 import *
from ctrader_open_api.messages.OpenApiMessages_pb2 import *
from ctrader_open_api.messages.OpenApiModelMessages_pb2 import *

# Load credentials
with open(os.path.expanduser("~/cTrade/creds.json")) as f:
    creds = json.load(f)

clientId = creds["clientId"]
clientSecret = creds["clientSecret"]
accessToken = creds.get("accessToken")
redirection = creds.get("redirection")
accountId = creds.get("accountId")
connectionType = creds.get("connectionType", "live").lower()

host = EndPoints.PROTOBUF_LIVE_HOST if connectionType == "live" else EndPoints.PROTOBUF_DEMO_HOST
port = EndPoints.PROTOBUF_PORT
currentAccountId = accountId

client = Client(host, port, TcpProtocol)

def connected(client):  # Callback for client connection
    print("\n✅ Connected")
    request = ProtoOAApplicationAuthReq()
    request.clientId = clientId
    request.clientSecret = clientSecret
    deferred = client.send(request)
    deferred.addErrback(onError)

def disconnected(client, reason):  # Callback for client disconnection
    print("\n🔌 Disconnected:", reason)

def onMessageReceived(client, message):  # Callback for receiving all messages
    if message.payloadType in [ProtoOASubscribeSpotsRes().payloadType,
                               ProtoOAAccountLogoutRes().payloadType,
                               ProtoHeartbeatEvent().payloadType]:
        return
    elif message.payloadType == ProtoOAApplicationAuthRes().payloadType:
        print("🎉 API Application authorized\n")
        print("You can now run authenticated commands.\n")
        if currentAccountId is not None:
            sendProtoOAAccountAuthReq()
    elif message.payloadType == ProtoOAAccountAuthRes().payloadType:
        protoOAAccountAuthRes = Protobuf.extract(message)
        print(f"🔐 Account {protoOAAccountAuthRes.ctidTraderAccountId} authorized.")
    else:
        print("📩 Message received:\n", Protobuf.extract(message))
    reactor.callLater(3, executeUserCommand)

def onError(failure):
    print("❌ Message Error:", failure)
    reactor.callLater(3, executeUserCommand)

# ---- Commands ----
def setAccount(accountId):
    global currentAccountId
    if currentAccountId:
        sendProtoOAAccountLogoutReq()
    currentAccountId = int(accountId)
    sendProtoOAAccountAuthReq()

def sendProtoOAAccountAuthReq(clientMsgId=None):
    request = ProtoOAAccountAuthReq()
    request.ctidTraderAccountId = currentAccountId
    request.accessToken = accessToken
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOAVersionReq(clientMsgId=None):
    request = ProtoOAVersionReq()
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOAGetAccountListByAccessTokenReq(clientMsgId=None):
    request = ProtoOAGetAccountListByAccessTokenReq()
    request.accessToken = accessToken
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

def sendProtoOAAccountLogoutReq(clientMsgId=None):
    request = ProtoOAAccountLogoutReq()
    request.ctidTraderAccountId = currentAccountId
    deferred = client.send(request, clientMsgId=clientMsgId)
    deferred.addErrback(onError)

# You can add more command functions here as needed...

commands = {
    "help": lambda: print("Available commands: setAccount, ProtoOAVersionReq, ProtoOAGetAccountListByAccessTokenReq"),
    "setAccount": setAccount,
    "ProtoOAVersionReq": sendProtoOAVersionReq,
    "ProtoOAGetAccountListByAccessTokenReq": sendProtoOAGetAccountListByAccessTokenReq,
}

def executeUserCommand():
    try:
        print("\n")
        userInput = inputimeout("Command (ex: help): ", timeout=18)
    except TimeoutOccurred:
        print("⌛ Command Input Timeout")
        reactor.callLater(3, executeUserCommand)
        return
    parts = userInput.strip().split()
    if not parts:
        print("⚠️ Invalid input")
        reactor.callLater(3, executeUserCommand)
        return
    command, *args = parts
    try:
        if command in commands:
            commands[command](*args)
        else:
            print("❓ Unknown command:", command)
    except Exception as e:
        print("⚠️ Command error:", e)
    reactor.callLater(3, executeUserCommand)

# ---- Setup client ----
client.setConnectedCallback(connected)
client.setDisconnectedCallback(disconnected)
client.setMessageReceivedCallback(onMessageReceived)
client.startService()
reactor.run()
