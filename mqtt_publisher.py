"""Thin, asyncio-friendly wrapper around paho-mqtt (v2 API)."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from config import MqttConfig
from const import AVAILABILITY_OFFLINE, AVAILABILITY_ONLINE, LOGGER

# paho-mqtt's default reconnect behavior on unexpected disconnects:
# - reconnect_on_failure=True (the client default) makes loop_start()/
#   loop_forever() reconnect automatically instead of giving up.
# - Backoff starts at _RECONNECT_MIN_DELAY and doubles up to
#   _RECONNECT_MAX_DELAY on each further failed attempt (see paho's
#   `_reconnect_wait`). These are paho's own built-in defaults; they are set
#   explicitly here only so the behavior doesn't silently change if a future
#   paho release changes its defaults.
_RECONNECT_MIN_DELAY = 1
_RECONNECT_MAX_DELAY = 120


class MqttPublisher:
    """Manages the MQTT broker connection and publishes events.

    paho-mqtt runs its network I/O in its own background thread
    (`loop_start`). `client.publish` is thread-safe and can therefore be
    called directly from coroutines without blocking the event loop.

    Reconnect behavior on connection loss is left at paho-mqtt's own,
    well-tested default: `loop_start()` runs `loop_forever(
    retry_first_connection=True)` in the background thread, which keeps
    retrying both the initial connection and any later reconnect with
    exponential backoff (see `_RECONNECT_MIN_DELAY`/`_RECONNECT_MAX_DELAY`
    above) for as long as the bridge is running - no custom reconnect logic
    is added on top, to avoid diverging from paho's original, well-known
    behavior.

    Also publishes an overall bridge status ("online"/"offline") to
    `<base_topic>/bridge/status`, retained, using MQTT's Last Will and
    Testament (LWT): the broker publishes "offline" itself if this process
    disappears without a clean disconnect (crash, killed, network drop),
    which no amount of application-level reconnect logic can cover.
    """

    def __init__(self, config: MqttConfig) -> None:
        self._config = config
        self._status_topic = f"{config.base_topic}/bridge/status"
        self._client = mqtt.Client(
            CallbackAPIVersion.VERSION2,
            client_id=config.client_id,
            protocol=mqtt.MQTTv311,
            reconnect_on_failure=True,
        )
        self._client.reconnect_delay_set(
            min_delay=_RECONNECT_MIN_DELAY, max_delay=_RECONNECT_MAX_DELAY
        )
        if config.username:
            self._client.username_pw_set(config.username, config.password)
        if config.use_tls:
            self._client.tls_set(ca_certs=config.ca_certs)
            if config.tls_insecure:
                self._client.tls_insecure_set(True)
        self._client.will_set(
            self._status_topic, AVAILABILITY_OFFLINE, qos=1, retain=True
        )

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_connect_fail = self._on_connect_fail
        self._connected = asyncio.Event()
        self._loop: asyncio.AbstractEventLoop | None = None

    def _on_connect(self, client, userdata, flags, reason_code, properties) -> None:
        if reason_code == 0:
            LOGGER.info(
                "MQTT: connected to %s:%s", self._config.host, self._config.port
            )
            self.publish_raw(self._status_topic, AVAILABILITY_ONLINE, retain=True)
        else:
            LOGGER.error("MQTT: connection failed: %s", reason_code)
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._connected.set)

    def _on_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        LOGGER.warning(
            "MQTT: connection lost (%s), automatic reconnect is running in "
            "the background",
            reason_code,
        )
        if self._loop is not None:
            self._loop.call_soon_threadsafe(self._connected.clear)

    def _on_connect_fail(self, client, userdata) -> None:
        LOGGER.warning(
            "MQTT: connection attempt to %s:%s failed, retrying automatically",
            self._config.host,
            self._config.port,
        )

    async def async_start(self, timeout: float = 10.0) -> None:
        """Connect to the broker and start the background network loop."""
        self._loop = asyncio.get_running_loop()
        LOGGER.debug(
            "MQTT: connecting to %s:%s ...", self._config.host, self._config.port
        )
        self._client.connect_async(self._config.host, self._config.port, keepalive=60)
        self._client.loop_start()
        try:
            await asyncio.wait_for(self._connected.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            LOGGER.warning(
                "MQTT: connection not confirmed within %ss, continuing "
                "anyway (reconnect keeps running in the background)",
                timeout,
            )

    async def async_stop(self) -> None:
        """Publish a clean "offline", disconnect, and stop the background thread.

        A clean `disconnect()` does NOT trigger the LWT (that only fires on
        an unclean disconnect), so an intentional shutdown is reported
        explicitly here instead of leaving the last "online" retained.
        """
        self.publish_raw(self._status_topic, AVAILABILITY_OFFLINE, retain=True)
        self._client.disconnect()
        self._client.loop_stop()

    def publish(self, topic: str, payload: dict[str, Any]) -> None:
        """Publish an event dict as a JSON message."""
        body = json.dumps(payload, ensure_ascii=False, default=str)
        result = self._client.publish(
            topic, body, qos=self._config.qos, retain=self._config.retain
        )
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.warning("MQTT: publish to %s failed (rc=%s)", topic, result.rc)
        else:
            LOGGER.debug("MQTT: -> %s: %s", topic, body)

    def publish_raw(self, topic: str, payload: str, retain: bool = False) -> None:
        """Publish a plain-text payload (e.g. an availability status)."""
        result = self._client.publish(topic, payload, qos=1, retain=retain)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            LOGGER.warning("MQTT: publish to %s failed (rc=%s)", topic, result.rc)
        else:
            LOGGER.debug("MQTT: -> %s: %s", topic, payload)
