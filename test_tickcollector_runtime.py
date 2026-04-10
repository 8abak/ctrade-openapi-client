import threading
import unittest
from unittest.mock import patch

from ctrader_open_api.messages.OpenApiMessages_pb2 import (
    ProtoOAAccountAuthRes,
    ProtoOAApplicationAuthRes,
    ProtoOAErrorRes,
    ProtoOASpotEvent,
    ProtoOASubscribeSpotsRes,
)

from datavis.tickcollector_runtime import TickCollectorController


class RawMessage:
    def __init__(self, payload, client_msg_id="req-1"):
        self.payload = payload
        self.payloadType = payload.payloadType
        self.clientMsgId = client_msg_id


class ImmediateDeferred:
    def __init__(self, result=None, failure=None):
        self._result = result
        self._failure = failure

    def addCallback(self, callback):
        if self._failure is None:
            self._result = callback(self._result)
        return self

    def addErrback(self, callback):
        if self._failure is not None:
            callback(self._failure)
        return self


class FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.sent = []

    def send(self, payload, responseTimeoutInSeconds=12):
        self.sent.append(type(payload).__name__)
        if not self.responses:
            raise AssertionError("No queued response for send()")
        response = self.responses.pop(0)
        return ImmediateDeferred(result=response)


class FakeDelayedCall:
    def __init__(self, reactor, due, fn, args):
        self.reactor = reactor
        self.due = due
        self.fn = fn
        self.args = args
        self.cancelled = False
        self.called = False

    def active(self):
        return not self.cancelled and not self.called

    def cancel(self):
        self.cancelled = True

    def run(self):
        if not self.active():
            return
        self.called = True
        self.fn(*self.args)


class FakeReactor:
    def __init__(self):
        self.now = 0.0
        self.calls = []

    def callLater(self, delay, fn, *args):
        call = FakeDelayedCall(self, self.now + max(0.0, float(delay)), fn, args)
        self.calls.append(call)
        return call

    def run_ready(self):
        progressed = True
        while progressed:
            progressed = False
            for call in list(self.calls):
                if call.active() and call.due <= self.now:
                    call.run()
                    progressed = True

    def advance(self, seconds):
        self.now += float(seconds)
        self.run_ready()


class FakeReconnectPolicy:
    def __init__(self):
        self.cooldowns = []
        self.disconnects = []

    def on_connected(self):
        return None

    def note_disconnect(self, reason):
        self.disconnects.append(reason)

    def enforce_cooldown(self, *, delay, reason):
        self.cooldowns.append((reason, delay))


class FakeTokenManager:
    def __init__(self, *, access_token="access-1", refresh_result=(True, None), sync_updates=None):
        self.client_id = "client-id"
        self.client_secret = "client-secret"
        self.access_token = access_token
        self.refresh_calls = 0
        self.refresh_result = refresh_result
        self.sync_calls = []
        self.sync_updates = list(sync_updates or [])

    def sync_from_disk(self, *, context, log=None, log_if_unchanged=False):
        self.sync_calls.append(context)
        if self.sync_updates:
            updated, next_token = self.sync_updates.pop(0)
            if updated and next_token:
                self.access_token = next_token
            return updated
        return False

    def refresh_access_token(self, *, context, log=None):
        self.refresh_calls += 1
        return self.refresh_result


def application_auth_ok():
    return RawMessage(ProtoOAApplicationAuthRes())


def account_auth_ok(account_id):
    payload = ProtoOAAccountAuthRes()
    payload.ctidTraderAccountId = int(account_id)
    return RawMessage(payload)


def subscribe_ok(account_id):
    payload = ProtoOASubscribeSpotsRes()
    payload.ctidTraderAccountId = int(account_id)
    return RawMessage(payload)


def error_response(code, description):
    payload = ProtoOAErrorRes()
    payload.errorCode = code
    payload.description = description
    return RawMessage(payload)


class TickCollectorControllerTests(unittest.TestCase):
    def make_controller(self, responses, *, token_manager=None):
        logs = []
        spots = []
        reactor = FakeReactor()
        client = FakeClient(responses)
        reconnect_policy = FakeReconnectPolicy()
        manager = token_manager or FakeTokenManager()
        controller = TickCollectorController(
            client=client,
            reactor_api=reactor,
            stop_event=threading.Event(),
            token_manager=manager,
            account_id=123456,
            symbol_id=99,
            connection_type="live",
            on_tick=lambda ts, bid, ask: spots.append((ts, bid, ask)),
            log=logs.append,
            reconnect_policy=reconnect_policy,
        )
        return controller, client, reactor, manager, reconnect_policy, logs, spots

    @patch("datavis.tickcollector_runtime.Protobuf.extract", side_effect=lambda raw: raw.payload)
    def test_reconnect_resumes_after_already_logged_in(self, _extract):
        controller, client, reactor, manager, _retry, logs, spots = self.make_controller(
            [
                application_auth_ok(),
                account_auth_ok(123456),
                subscribe_ok(123456),
                error_response("ALREADY_LOGGED_IN", "Open API application is already authorized"),
                account_auth_ok(123456),
                subscribe_ok(123456),
            ]
        )

        controller.connected(None)
        reactor.run_ready()

        spot = ProtoOASpotEvent()
        spot.timestamp = 1000
        spot.bid = 200000
        spot.ask = 200500
        controller.on_message(None, RawMessage(spot, client_msg_id=""))

        controller.disconnected(None, "socket lost")
        controller.connected(None)
        reactor.run_ready()

        spot2 = ProtoOASpotEvent()
        spot2.timestamp = 2000
        spot2.bid = 201000
        spot2.ask = 201500
        controller.on_message(None, RawMessage(spot2, client_msg_id=""))

        self.assertEqual(
            client.sent,
            [
                "ProtoOAApplicationAuthReq",
                "ProtoOAAccountAuthReq",
                "ProtoOASubscribeSpotsReq",
                "ProtoOAApplicationAuthReq",
                "ProtoOAAccountAuthReq",
                "ProtoOASubscribeSpotsReq",
            ],
        )
        self.assertEqual(spots, [(1000, 200000, 200500), (2000, 201000, 201500)])
        self.assertEqual(manager.refresh_calls, 0)
        self.assertTrue(any("app auth success state=already_logged_in" in line for line in logs))
        self.assertTrue(any("subscription success symbolId=99" in line for line in logs))

    @patch("datavis.tickcollector_runtime.Protobuf.extract", side_effect=lambda raw: raw.payload)
    def test_account_not_authorized_does_not_refresh_or_subscribe(self, _extract):
        controller, client, reactor, manager, _retry, logs, _spots = self.make_controller(
            [
                application_auth_ok(),
                error_response("INVALID_REQUEST", "Trading account is not authorized"),
            ]
        )

        controller.connected(None)
        reactor.run_ready()

        self.assertEqual(client.sent, ["ProtoOAApplicationAuthReq", "ProtoOAAccountAuthReq"])
        self.assertEqual(manager.refresh_calls, 0)
        self.assertIsNotNone(controller._auth_retry_call)
        self.assertTrue(controller._auth_retry_call.active())
        self.assertEqual(controller._auth_retry_call.due, 3.0)
        self.assertTrue(any("account auth failed code=INVALID_REQUEST" in line for line in logs))

    @patch("datavis.tickcollector_runtime.Protobuf.extract", side_effect=lambda raw: raw.payload)
    def test_refresh_rate_limit_adds_cooldown(self, _extract):
        controller, client, reactor, manager, retry_policy, logs, _spots = self.make_controller(
            [
                application_auth_ok(),
                error_response("CH_ACCESS_TOKEN_INVALID", "Invalid access token"),
            ],
            token_manager=FakeTokenManager(refresh_result=(False, "HTTP 429 Too Many Requests")),
        )

        controller.connected(None)
        reactor.run_ready()

        self.assertEqual(client.sent, ["ProtoOAApplicationAuthReq", "ProtoOAAccountAuthReq"])
        self.assertEqual(manager.refresh_calls, 1)
        self.assertIsNotNone(controller._auth_retry_call)
        self.assertTrue(controller._auth_retry_call.active())
        self.assertEqual(controller._auth_retry_call.due, 10.0)
        self.assertEqual(retry_policy.cooldowns, [("refresh_rate_limited", 10.0)])
        self.assertTrue(any("rate-limit cooldown stage=refresh delay=10.0s" in line for line in logs))


if __name__ == "__main__":
    unittest.main()
