"""Orchestration: ONVIF cameras <-> MQTT broker.

Replaces the Home Assistant integration's `onvif/__init__.py`
(ONVIFDevice setup) and wires the HA-free EventManager to the MQTT
publisher: every ONVIF event is published as an MQTT message.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path

import onvif
from aiohttp import web
from onvif import ONVIFCamera

from config import BridgeConfig, CameraConfig, slugify
from const import (
    AVAILABILITY_OFFLINE,
    AVAILABILITY_ONLINE,
    CONNECT_RETRY_INTERVAL,
    CONNECT_RETRY_LOG_EVERY,
    CONNECT_RETRY_MAX_INTERVAL,
    LOGGER,
    SUPERVISE_DOWN_GRACE,
    SUPERVISE_POLL_INTERVAL,
)
from doorbell import DoorbellListener, DoorbellManager
from event_manager import EventManager
from event_parser import ParsedEvent
from mqtt_publisher import MqttPublisher

# The WSDL files are shipped with python-onvif-zeep-async.
_WSDL_DIR = str(Path(onvif.__file__).resolve().parent / "wsdl")


class CameraBridge:
    """Connects a single camera to the EventManager."""

    def __init__(
        self,
        config: CameraConfig,
        mqtt: MqttPublisher,
        base_topic: str,
        topic_style: str,
        webhook_base_url: str | None,
        register_webhook_handler,
    ) -> None:
        self.config = config
        self._mqtt = mqtt
        self._mqtt_topic_prefix = f"{base_topic}/{config.slug}"
        self._topic_style = topic_style
        self._availability_topic = f"{self._mqtt_topic_prefix}/status"

        self.device = ONVIFCamera(
            config.host,
            config.port,
            config.username,
            config.password,
            wsdl_dir=_WSDL_DIR,
            nat_override=config.nat_override,
        )

        webhook_url = None
        if webhook_base_url and config.use_webhook:
            webhook_url = f"{webhook_base_url}/onvif/{config.slug}"

        self.event_manager = EventManager(
            device=self.device,
            name=config.name,
            uid=config.identifier,
            on_event=self._on_event,
            ignored_topics=config.ignored_topics,
            webhook_url=webhook_url,
            register_webhook_handler=register_webhook_handler if webhook_url else None,
            on_availability=self._on_availability,
            pull_timeout=config.pull_timeout,
            pull_hard_slack=config.pull_hard_slack,
            offline_grace=config.offline_grace,
            flap_recovery=config.flap_recovery,
            stuck_on_timeout=config.stuck_on_timeout,
            subscription_time=config.subscription_time,
            subscription_restart_interval=config.subscription_restart_interval,
            pullpoint_message_limit=config.pullpoint_message_limit,
            pullpoint_cooldown=config.pullpoint_cooldown,
            flap_pull_retry_delay=config.flap_pull_retry_delay,
            flap_resub_delay=config.flap_resub_delay,
            teardown_timeout=config.teardown_timeout,
            periodic_reconnect_hours=config.periodic_reconnect_hours,
            stale_event_reconnect_hours=config.stale_event_reconnect_hours,
        )

        # Optional: a physical doorbell button (see doorbell.py), observed
        # via UDP broadcast entirely independently of, and asynchronously
        # to, the ONVIF event stream above. Registered with the bridge's
        # shared DoorbellManager in Bridge.async_start (a socket is bound
        # per distinct port, not per camera).
        self._supervisor_task: asyncio.Task[None] | None = None
        self._retry_initial = (
            config.connect_retry_interval
            if config.connect_retry_interval is not None
            else CONNECT_RETRY_INTERVAL
        )
        self._retry_max = max(
            self._retry_initial,
            config.connect_retry_max_interval
            if config.connect_retry_max_interval is not None
            else CONNECT_RETRY_MAX_INTERVAL,
        )

        self.doorbell: DoorbellListener | None = None
        if config.doorbell_enabled:
            self.doorbell = DoorbellListener(
                name=config.name,
                ip=config.resolved_doorbell_ip,
                port=config.doorbell_port,
                pulse_seconds=config.doorbell_pulse_seconds,
                on_state=self.event_manager.handle_doorbell_event,
            )

    def _on_event(self, event: ParsedEvent) -> None:
        """EventManager callback: publish the parsed event to MQTT.

        Topic layout depends on `mqtt.topic_style` (see config.py):
        - "per_event" (default): one MQTT topic per ONVIF event type, e.g.
          onvif/front_door/tns1:RuleEngine/CellMotionDetector/Motion
        - "by_name": one MQTT topic per slugified onvif_parsers event name,
          e.g. onvif/front_door/vehicle_detection (from event.name
          "Vehicle Detection")
        - "flat": a single topic per camera for every event, e.g.
          onvif/front_door/event
        The ONVIF topic (and the original, unslugified event name) is
        always present in the payload either way (see
        ParsedEvent.as_mqtt_payload), so no information is lost by choosing
        "by_name" or "flat".
        """
        if self._topic_style == "by_name":
            mqtt_topic = f"{self._mqtt_topic_prefix}/{slugify(event.name, fallback='event')}"
        elif self._topic_style == "flat":
            mqtt_topic = f"{self._mqtt_topic_prefix}/event"
        else:
            mqtt_topic = f"{self._mqtt_topic_prefix}/{event.topic}"
        self._mqtt.publish(mqtt_topic, event.as_mqtt_payload(self.config.name))

    def _on_availability(self, available: bool) -> None:
        """EventManager callback: publish honest online/offline status.

        Retained so a consumer connecting later immediately sees the
        camera's current state rather than "no message yet".
        """
        payload = AVAILABILITY_ONLINE if available else AVAILABILITY_OFFLINE
        self._mqtt.publish_raw(self._availability_topic, payload, retain=True)

    async def async_start(self) -> bool:
        if not (self.config.use_pullpoint or self.config.use_webhook):
            # ONVIF deliberately disabled for this camera (e.g. a
            # doorbell-only setup with no ONVIF events wanted at all) -
            # skip the ONVIF connection entirely rather than attempting it
            # anyway and then warning that it "failed": nothing failed,
            # this is the requested configuration. The doorbell listener
            # (if configured) is started independently in
            # Bridge.async_start and needs no ONVIF connection.
            LOGGER.info(
                "%s: ONVIF disabled (use_pullpoint=false, use_webhook=false)"
                "%s",
                self.config.name,
                " - doorbell only" if self.doorbell else "",
            )
            return bool(self.doorbell)

        if not (self.config.use_pullpoint or self.event_manager.webhook_manager.is_configured()):
            # Nothing could ever start: PullPoint is off and the webhook is
            # either off or not usable (webhook.enabled / webhook.base_url).
            # That's a configuration problem, not a connection problem -
            # retrying would never help, so say so once instead.
            LOGGER.error(
                "%s: no usable event transport - use_pullpoint is off and the "
                "webhook is not configured (needs webhook.enabled and "
                "webhook.base_url, plus use_webhook on this camera)",
                self.config.name,
            )
            return bool(self.doorbell)

        # Connecting happens in a supervised background task rather than
        # inline: an unreachable camera must neither delay the others (or the
        # doorbell listener) nor be given up on after a single failed
        # attempt - see _async_supervise.
        self._supervisor_task = asyncio.get_running_loop().create_task(
            self._async_supervise()
        )
        return True

    async def _async_try_connect(self) -> bool:
        """One connection attempt: ONVIF discovery, then start the transports."""
        await self.device.update_xaddrs()
        started = await self.event_manager.async_start(
            try_pullpoint=self.config.use_pullpoint,
            try_webhook=self.config.use_webhook,
        )
        if started:
            LOGGER.info(
                "%s: event processing started (pullpoint=%s, webhook=%s)",
                self.config.name,
                self.event_manager.pullpoint_manager.started,
                self.event_manager.webhook_manager.started,
            )
        return started

    async def _async_reset_event_manager(self) -> None:
        """Return the EventManager to a clean, restartable state.

        A failed `async_start` leaves the transports in state "failed", and
        their `async_start` asserts "stopped" - so without this a second
        attempt would die on that assert instead of retrying. As a side
        effect it reports the camera offline (once, on change), which also
        overwrites a stale retained "online" from a previous bridge run.
        """
        try:
            await self.event_manager.async_stop()
        except Exception:  # noqa: BLE001 - cleanup must never end the supervisor
            LOGGER.debug("%s: error resetting event manager", self.config.name, exc_info=True)

    async def _async_wait_until_down(self) -> None:
        """Return once no event transport has been up for SUPERVISE_DOWN_GRACE."""
        down_since: float | None = None
        while True:
            await asyncio.sleep(SUPERVISE_POLL_INTERVAL)
            if self.event_manager.started:
                down_since = None
                continue
            now = time.monotonic()
            if down_since is None:
                down_since = now
            elif now - down_since >= SUPERVISE_DOWN_GRACE:
                return

    async def _async_supervise(self) -> None:
        """Connect to the camera, retrying until it works - and again if lost.

        Fixes a camera that is unreachable when the bridge starts being
        abandoned for good after one failed attempt (and, since the
        time-based reconnect tasks are only created by a *successful*
        EventManager.async_start, never getting those either). Retries use
        exponential backoff between connect_retry_interval and
        connect_retry_max_interval and never stop. Once connected it keeps
        watching: if every transport is lost (e.g. a webhook-only camera
        whose resubscribe failed), it reconnects from scratch.
        """
        name = self.config.name
        attempt = 0
        delay = self._retry_initial
        first_failure_at: float | None = None
        while True:
            attempt += 1
            if attempt == 1:
                LOGGER.info(
                    "%s: connecting to ONVIF device %s ...", name, self.config.host
                )
            else:
                LOGGER.debug("%s: connection attempt %d ...", name, attempt)
            error: Exception | None = None
            try:
                started = await self._async_try_connect()
            except Exception as err:  # noqa: BLE001 - every failure means "retry"
                started = False
                error = err
                LOGGER.debug("%s: connection attempt %d failed", name, attempt, exc_info=True)

            if started:
                if first_failure_at is not None:
                    LOGGER.info(
                        "%s: connected after %d attempts (%.0fs)",
                        name,
                        attempt,
                        time.monotonic() - first_failure_at,
                    )
                attempt = 0
                delay = self._retry_initial
                first_failure_at = None
                await self._async_wait_until_down()
                LOGGER.warning(
                    "%s: no event transport has been active for %ss; "
                    "reconnecting from scratch",
                    name,
                    SUPERVISE_DOWN_GRACE,
                )
                await self._async_reset_event_manager()
                continue

            await self._async_reset_event_manager()
            if first_failure_at is None:
                first_failure_at = time.monotonic()
            reason = (
                str(error) or type(error).__name__
                if error is not None
                else "neither PullPoint nor webhook could be started"
            )
            if attempt == 1:
                LOGGER.warning(
                    "%s: could not connect (%s); retrying, %.0fs to %.0fs "
                    "apart, until it succeeds",
                    name,
                    reason,
                    self._retry_initial,
                    self._retry_max,
                )
            elif attempt % CONNECT_RETRY_LOG_EVERY == 0:
                LOGGER.warning(
                    "%s: still not connected after %d attempts / %.0fs (%s)",
                    name,
                    attempt,
                    time.monotonic() - first_failure_at,
                    reason,
                )
            await asyncio.sleep(delay)
            delay = min(delay * 2, self._retry_max)

    async def async_stop(self) -> None:
        if self._supervisor_task:
            self._supervisor_task.cancel()
            try:
                await self._supervisor_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._supervisor_task = None
        if self.doorbell:
            await self.doorbell.async_stop()
        await self.event_manager.async_stop()
        await self.device.close()


class Bridge:
    """Starts and stops all configured cameras plus the MQTT connection."""

    def __init__(self, config: BridgeConfig) -> None:
        self.config = config
        self.mqtt = MqttPublisher(config.mqtt)
        self.doorbell_manager = DoorbellManager()
        self._cameras: list[CameraBridge] = []
        self._web_app: web.Application | None = None
        self._web_runner: web.AppRunner | None = None

    def _register_webhook_handler(self, slug: str):
        """Return a register_handler function for the given camera."""
        assert self._web_app is not None

        def _register(handler) -> None:
            self._web_app.router.add_post(f"/onvif/{slug}", handler)

        return _register

    async def async_start(self) -> None:
        await self.mqtt.async_start()

        webhook_cfg = self.config.webhook
        if webhook_cfg.enabled:
            self._web_app = web.Application()

        for cam_cfg in self.config.cameras:
            register_handler = (
                self._register_webhook_handler(cam_cfg.slug)
                if webhook_cfg.enabled and cam_cfg.use_webhook
                else None
            )
            camera = CameraBridge(
                cam_cfg,
                self.mqtt,
                self.config.mqtt.base_topic,
                self.config.mqtt.topic_style,
                webhook_cfg.base_url if webhook_cfg.enabled else None,
                register_handler,
            )
            self._cameras.append(camera)
            if camera.doorbell:
                # Independent of, and not gated on, the ONVIF connection
                # below: a doorbell button works over a plain UDP broadcast
                # and has nothing to do with the camera's ONVIF service, so
                # it's started here regardless of whether ONVIF startup
                # below succeeds.
                await camera.doorbell.async_start(self.doorbell_manager)

        if webhook_cfg.enabled:
            self._web_runner = web.AppRunner(self._web_app)
            await self._web_runner.setup()
            site = web.TCPSite(
                self._web_runner, webhook_cfg.listen_host, webhook_cfg.listen_port
            )
            await site.start()
            LOGGER.info(
                "Webhook server listening on %s:%s (externally reachable at %s)",
                webhook_cfg.listen_host,
                webhook_cfg.listen_port,
                webhook_cfg.base_url,
            )

        results = await asyncio.gather(
            *(camera.async_start() for camera in self._cameras),
            return_exceptions=True,
        )
        for cam_cfg, result in zip(self.config.cameras, results):
            if isinstance(result, Exception):
                LOGGER.error(
                    "%s: startup failed: %s", cam_cfg.name, result, exc_info=result
                )

    async def async_stop(self) -> None:
        await asyncio.gather(
            *(camera.async_stop() for camera in self._cameras),
            return_exceptions=True,
        )
        await self.doorbell_manager.async_close()
        if self._web_runner:
            await self._web_runner.cleanup()
        await self.mqtt.async_stop()

    async def async_run_forever(self) -> None:
        """Start the bridge and run until async_stop() is called."""
        await self.async_start()
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        try:
            import signal

            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, stop_event.set)
        except (NotImplementedError, ImportError):
            # Not available for all signals on e.g. Windows.
            pass

        LOGGER.info("ONVIF-MQTT bridge is running. Press Ctrl+C to stop.")
        await stop_event.wait()
        LOGGER.info("Shutting down ...")
        await self.async_stop()
