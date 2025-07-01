#!/usr/bin/env python

from twisted.internet.protocol import ClientFactory
from ctrader_open_api.tcpProtocol import TcpProtocol

class Factory(ClientFactory):
    def __init__(self, *args, **kwargs):
        super().__init__()
        self.client = kwargs['client']
        self.numberOfMessagesToSendPerSecond = self.client.numberOfMessagesToSendPerSecond

    def buildProtocol(self, addr):
        print("✅ buildProtocol CALLED — returning TcpProtocol")
        from ctrader_open_api.tcpProtocol import TcpProtocol
        proto = TcpProtocol(self)
        print("✅ TcpProtocol instance created:", proto)
        return proto


    def connected(self, protocol):
        self.client._connected(protocol)

    def disconnected(self, reason):
        self.client._disconnected(reason)

    def received(self, message):
        self.client._received(message)

    @staticmethod
    def build_payload(message_cls, **params):
        """Create and populate a protobuf message instance."""
        msg = message_cls()
        if 'accountId' in params and hasattr(msg, 'ctidTraderAccountId'):
            setattr(msg, 'ctidTraderAccountId', params.pop('accountId'))
        for field, value in params.items():
            setattr(msg, field, value)
        return msg
