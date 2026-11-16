"""UDP doorbell-button listener.

Some camera/doorbell combos additionally have a physical doorbell button
that isn't exposed as an ONVIF event at all: pressing it makes the doorbell
unit send a UDP broadcast packet from its own IP to a fixed destination
port - observed entirely independently of, and asynchronously to, the
ONVIF PullPoint/webhook event stream this bridge otherwise relies on for
everything else. Ported from a user-supplied packet-sniffing script
(`mqttsniffer.py`) that listened for this with a blocking socket and
published straight to MQTT itself.

This module reimplements that same "packet from this IP -> pulse on then
off" behavior with asyncio's UDP transport instead of a blocking socket,
and dispatches through `EventManager.handle_doorbell_event`
(event_manager.py) rather than talking to MQTT directly, so a doorbell
press is published exactly like any ONVIF event: same payload shape, same
per-camera `topic_style` handling (bridge.py), and it runs through the same
dedup + stuck-on watchdog pipeline as any other binary_sensor event.

One UDP socket is opened per distinct destination port referenced by any
configured doorbell (typically just one, shared by every doorbell using
that port - e.g. every unit from the same manufacturer), and incoming
packets are routed to the right camera by matching the packet's source IP
against each doorbell's configured IP - generalizing the reference
script's own `if addr[0] == DOORBELL_IP` check to more than one
doorbell/camera sharing a bridge.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from const import LOGGER


class _DoorbellProtocol(asyncio.DatagramProtocol):
    """asyncio UDP protocol for one listening port.

    Holds the source-IP -> on_press callback mapping for every doorbell
    sharing this port and dispatches each received packet to the matching
    one; packets from any other source on this port are silently ignored,
    exactly like the reference script's own IP check.
    """

    def __init__(self) -> None:
        self.handlers: dict[str, Callable[[], None]] = {}

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        handler = self.handlers.get(addr[0])
        if handler is not None:
            handler()

    def error_received(self, exc: Exception) -> None:
        LOGGER.warning("Doorbell: UDP error: %s", exc)


class DoorbellManager:
    """Owns one shared UDP socket per distinct port across all cameras."""

    def __init__(self) -> None:
        self._protocols: dict[int, _DoorbellProtocol] = {}
        self._transports: list[asyncio.DatagramTransport] = []

    async def async_register(
        self, name: str, ip: str, port: int, on_press: Callable[[], None]
    ) -> bool:
        """Register `on_press` for packets from `ip` arriving on `port`.

        Returns False (logging why) if the port couldn't be bound - e.g.
        already in use by something else on this host - rather than
        raising, so one camera's misconfigured doorbell can't take the
        whole bridge down.
        """
        protocol = self._protocols.get(port)
        if protocol is None:
            loop = asyncio.get_running_loop()
            try:
                transport, protocol = await loop.create_datagram_endpoint(
                    _DoorbellProtocol,
                    local_addr=("0.0.0.0", port),
                    allow_broadcast=True,
                )
            except OSError as err:
                LOGGER.error(
                    "%s: could not listen for doorbell UDP broadcasts on "
                    "port %s: %s",
                    name,
                    port,
                    err,
                )
                return False
            self._protocols[port] = protocol
            self._transports.append(transport)
            LOGGER.info("Doorbell: listening for UDP broadcasts on port %s", port)
        if ip in protocol.handlers:
            LOGGER.warning(
                "%s: doorbell IP %s is already registered on port %s for "
                "another camera; only the most recently registered one "
                "will receive presses from this IP",
                name,
                ip,
                port,
            )
        protocol.handlers[ip] = on_press
        return True

    async def async_close(self) -> None:
        for transport in self._transports:
            transport.close()
        self._transports.clear()
        self._protocols.clear()


class DoorbellListener:
    """Per-camera doorbell: pulses "on" then "off" on every button press.

    Mirrors the reference script's own pulse behavior (publish "on", wait
    `pulse_seconds`, publish "off") exactly, just routed through
    `on_state` (`EventManager.handle_doorbell_event`) instead of an MQTT
    client directly.
    """

    def __init__(
        self,
        name: str,
        ip: str,
        port: int,
        pulse_seconds: float,
        on_state: Callable[[bool], None],
    ) -> None:
        self._name = name
        self._ip = ip
        self._port = port
        self._pulse_seconds = pulse_seconds
        self._on_state = on_state
        self._pulse_task: asyncio.Task[None] | None = None

    async def async_start(self, manager: DoorbellManager) -> bool:
        return await manager.async_register(
            self._name, self._ip, self._port, self._on_press
        )

    def _on_press(self) -> None:
        LOGGER.info("%s: doorbell pressed (UDP from %s)", self._name, self._ip)
        if self._pulse_task and not self._pulse_task.done():
            # A press is already being pulsed; let it finish rather than
            # overlapping two pulses - the reference script's blocking
            # loop couldn't overlap them either.
            LOGGER.debug(
                "%s: doorbell pulse already in progress, ignoring this "
                "press",
                self._name,
            )
            return
        self._pulse_task = asyncio.get_event_loop().create_task(self._async_pulse())

    async def _async_pulse(self) -> None:
        """Publish "on", wait, then unconditionally publish "off" again.

        Both halves are entirely our own responsibility here - unlike an
        ONVIF event, where the closing "off" depends on the camera
        actually sending it and can get lost (see fix #1's stuck-on
        watchdog). The try/finally below already makes that watchdog
        largely redundant for doorbell presses specifically: it covers
        both a cancelled pulse (e.g. bridge shutdown mid-pulse) and even a
        failure while publishing "on" itself, so "off" reliably follows in
        every case except the process dying outright - at which point the
        watchdog wouldn't be running to help either. It's still routed
        through the same dedup/watchdog pipeline as any other event
        (`EventManager.handle_doorbell_event`) for defense-in-depth and
        code-path consistency, not because it's load-bearing here.
        """
        try:
            try:
                self._on_state(True)
                await asyncio.sleep(self._pulse_seconds)
            finally:
                self._on_state(False)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - log rather than an unretrieved task exception
            LOGGER.exception("%s: error handling doorbell press", self._name)

    async def async_stop(self) -> None:
        if self._pulse_task:
            self._pulse_task.cancel()
            try:
                await self._pulse_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._pulse_task = None
