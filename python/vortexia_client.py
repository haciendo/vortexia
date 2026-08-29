"""
vortexia_client.py — thin Python client for the vortexia MQTT broker.

Mirrors the JS client's API (register/send) and adds a poll-and-collect
helper for one-shot CLI invocations that can't stay subscribed forever.

Requires `paho-mqtt` (optional dependency — not installed by default):
    pip install paho-mqtt

Topic schema (see PROTOCOL.md):
    las/agent/<name>/inbox      - direct message to one agent
    las/broadcast               - message to all agents
    las/agent/<name>/presence   - retained LWT presence ("online"/"offline")

Message envelope (JSON): {from, to, source, text, ts}
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Callable, Optional

try:
    import paho.mqtt.client as mqtt
except ImportError as exc:  # pragma: no cover
    raise ImportError(
        "vortexia_client requires paho-mqtt. Install it with: pip install paho-mqtt"
    ) from exc


DEFAULT_HOST = "localhost"
DEFAULT_PORT = 1883


def inbox_topic(name: str) -> str:
    return f"las/agent/{name}/inbox"


def presence_topic(name: str) -> str:
    return f"las/agent/{name}/presence"


BROADCAST_TOPIC = "las/broadcast"


def build_envelope(from_: str, to: str, text: str, source: str = "agent") -> dict:
    return {
        "from": from_,
        "to": to,
        "source": source,
        "text": text,
        "ts": int(time.time() * 1000),
    }


class VortexiaClient:
    """
    A small paho-mqtt wrapper mirroring the JS VortexiaClient API.

    Usage:
        client = VortexiaClient(host="localhost", port=1883)
        client.register("MyAgent")
        client.send("OtherAgent", "hello")
        client.close()
    """

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT):
        self.host = host
        self.port = port
        self.name: Optional[str] = None
        self._client: Optional[mqtt.Client] = None
        self._on_message: Optional[Callable[[dict, str], None]] = None

    def register(self, name: str, on_message: Optional[Callable[[dict, str], None]] = None) -> "VortexiaClient":
        """Connect, set LWT presence, subscribe to own inbox + broadcast."""
        self.name = name
        self._on_message = on_message

        client_id = f"vortexia-{name}-{uuid.uuid4().hex[:8]}"
        client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)
        client.will_set(presence_topic(name), payload="offline", qos=1, retain=True)

        def _on_connect(c, userdata, flags, rc):
            c.publish(presence_topic(name), "online", qos=1, retain=True)
            c.subscribe([(inbox_topic(name), 1), (BROADCAST_TOPIC, 1)])

        def _on_message(c, userdata, msg):
            try:
                envelope = json.loads(msg.payload.decode("utf-8"))
            except (ValueError, UnicodeDecodeError):
                return
            if self._on_message:
                self._on_message(envelope, msg.topic)

        client.on_connect = _on_connect
        client.on_message = _on_message

        client.connect(self.host, self.port, keepalive=30)
        client.loop_start()
        self._client = client
        return self

    def send(self, to: str, text: str, from_: Optional[str] = None, source: str = "agent") -> dict:
        """Publish a direct message (or 'broadcast' to reach everyone).

        Direct inbox messages are published RETAINED — see the JS client's
        send() for the full rationale (plain MQTT delivery only reaches
        subscribers connected at that instant, which loses any message sent
        while nobody happened to be listening; poll_inbox below is what
        clears the retained flag once a message is actually consumed).
        Broadcast is not retained.
        """
        if not self._client:
            raise RuntimeError("client not registered — call register(name) first")
        envelope = build_envelope(from_ or self.name, to, text, source)
        is_broadcast = to == "broadcast"
        topic = BROADCAST_TOPIC if is_broadcast else inbox_topic(to)
        self._client.publish(topic, json.dumps(envelope), qos=1, retain=not is_broadcast)
        return envelope

    def close(self) -> None:
        if not self._client:
            return
        if self.name:
            self._client.publish(presence_topic(self.name), "offline", qos=1, retain=True)
            time.sleep(0.1)  # give the publish a moment to flush before disconnecting
        self._client.loop_stop()
        self._client.disconnect()
        self._client = None


def poll_inbox(name: str, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT, timeout: float = 2.0) -> list[dict]:
    """
    Connect just long enough to drain any pending/retained messages for
    `name`'s inbox, then disconnect. Useful for a CLI tool invoked fresh
    each time, which can't stay subscribed indefinitely.

    This is the "real" consumer of a retained inbox message (see
    VortexiaClient.send): once collected here, the retained flag on the
    inbox topic is cleared (an empty retained publish, the standard MQTT way
    to remove a retained message) so the SAME message isn't handed to every
    future poll forever. A live viewer subscribed via VortexiaClient.register
    (e.g. the Electron widget) does NOT do this — it only displays messages
    as they arrive, leaving the retained copy for this function to actually
    consume later.

    Returns a list of message envelopes received within `timeout` seconds.
    """
    collected: list[dict] = []

    client_id = f"vortexia-poll-{name}-{uuid.uuid4().hex[:8]}"
    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)

    def _on_connect(c, userdata, flags, rc):
        c.subscribe(inbox_topic(name), qos=1)

    def _on_message(c, userdata, msg):
        try:
            collected.append(json.loads(msg.payload.decode("utf-8")))
        except (ValueError, UnicodeDecodeError):
            pass

    client.on_connect = _on_connect
    client.on_message = _on_message

    client.connect(host, port, keepalive=int(timeout) + 5)
    client.loop_start()
    time.sleep(timeout)
    if collected:
        # Clear the retained message now that it's been read — an empty
        # retained publish is the standard MQTT idiom for "delete the
        # retained value on this topic".
        client.publish(inbox_topic(name), payload=None, qos=1, retain=True)
        time.sleep(0.1)  # let the clear publish flush before disconnecting
    client.loop_stop()
    client.disconnect()

    return collected
