"""ONVIF event handling without any Home Assistant dependency.

This is a port of homeassistant/components/onvif/event_manager.py. Anything
tied to HA entities, ConfigEntry, hass.async_create_background_task, HA's
webhook helper, or homeassistant.util.dt has been removed and replaced with
plain asyncio/aiohttp. Instead of turning events into HA entities, every
event returned by `onvif_parsers.parse()` is handed to a callback, which the
bridge uses to publish it to MQTT.

On top of the original component's behavior, this module also ports three
bug fixes from the `tapo-onvif-events` project (see const.py's module
docstring for background and links):

1. **Stuck-on watchdog + rebaseline** (`EventManager.async_check_stuck` /
   `async_rebaseline`): fixes binary "on" events with no matching "off". A
   detection held "on" for more than `STUCK_ON_TIMEOUT` with no fresh
   `true` is force-cleared to "off"; every tracked "on" key is also forced
   off whenever a fresh subscription is (re)established, since the camera
   keeps no state across a reconnect. This runs independently of transport
   (PullPoint or webhook), because a dropped closing event can happen on
   either.
2. **Flap recovery** (`PullPointManager._async_pull_once`): fixes the
   reconnect strategy that used to tear the subscription down on every
   transient network hiccup (discarding the camera's queued events). A
   transient transport error during `PullMessages` (a dropped connection,
   not a subscription fault) is now retried on the *same* subscription at a
   fast cadence; only a non-transient error, a hard-timeout stall, or a
   sustained zero-success stretch escalates to a full resubscribe.
3. **Duplicate-event suppression** (`EventManager._update_binary_state`):
   many cameras resend the same `true` repeatedly (e.g. once a second) for
   as long as a detection condition holds. Only a real off<->on transition
   (or the first time a uid is seen) is published to MQTT; repeats are
   suppressed, though they still keep the stuck-on watchdog timer above
   refreshed.

Reconnect/retry behavior for actual subscription creation/renewal is left
equivalent to the original component:
- Subscription creation still goes through onvif-zeep-async's
  `retry_connection_error` decorator (unchanged, imported from the `onvif`
  package as-is), gated by `SUBSCRIPTION_ATTEMPTS`.
- The PullPoint <-> webhook failover logic (`async_webhook_failed`,
  `async_webhook_working`, pause/resume) is unchanged.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import time
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import replace

import aiohttp
import onvif_parsers
import onvif_parsers.util
from aiohttp import web
from onvif import ONVIFCamera
from onvif.client import (
    NotificationManager,
    PullPointManager as ONVIFPullPointManager,
    retry_connection_error,
)
from onvif.exceptions import ONVIFError
from onvif.util import stringify_onvif_error
from zeep.exceptions import Fault, TransportError, ValidationError, XMLParseError

from const import (
    FLAP_CLEAR_AFTER,
    FLAP_HEARTBEAT,
    FLAP_OFFLINE_GRACE,
    FLAP_PULL_RETRY_DELAY,
    FLAP_RESUB_DELAY,
    FLAP_THRESHOLD,
    FLAP_WINDOW,
    LOGGER,
    PULL_HARD_SLACK,
    PULLPOINT_COOLDOWN_TIME,
    PULLPOINT_MESSAGE_LIMIT,
    PULLPOINT_POLL_TIME,
    STUCK_ON_CHECK_INTERVAL,
    STUCK_ON_TIMEOUT,
    SUBSCRIPTION_ATTEMPTS,
    SUBSCRIPTION_RESTART_INTERVAL_ON_ERROR,
    SUBSCRIPTION_TIME,
    TEARDOWN_TIMEOUT,
)
from event_parser import ParsedEvent, extract_source, local_datetime_or_none

# -- Exception groups, unchanged from the original event_manager.py --------
SUBSCRIPTION_ERRORS = (Fault, TimeoutError, TransportError)
CREATE_ERRORS = (
    ONVIFError,
    Fault,
    aiohttp.ClientError,
    asyncio.TimeoutError,
    XMLParseError,
    ValidationError,
)
UNSUBSCRIBE_ERRORS = (XMLParseError, *SUBSCRIPTION_ERRORS)
RENEW_ERRORS = (
    ONVIFError,
    aiohttp.ClientError,
    asyncio.TimeoutError,
    XMLParseError,
    *SUBSCRIPTION_ERRORS,
)

# -- Flap recovery: transient transport errors (ported from tapo-onvif-events)
# All of aiohttp's ServerDisconnectedError / ClientConnectionResetError /
# ClientOSError subclass ClientConnectionError; ConnectionError covers the
# bare-socket variants. A real subscription fault (zeep Fault) is NOT one of
# these, so it still escalates to a resubscribe instead of being retried in
# place.
_TRANSIENT_PULL_ERRORS = (aiohttp.ClientConnectionError, ConnectionError)


def _is_transient(err: BaseException) -> bool:
    """True if err (or its cause/context chain) is a transport disconnect."""
    seen = 0
    current: BaseException | None = err
    while current is not None and seen < 8:
        if isinstance(current, _TRANSIENT_PULL_ERRORS):
            return True
        current = current.__cause__ or current.__context__
        seen += 1
    return False


# Callback invoked for every parsed ONVIF event.
EventCallback = Callable[[ParsedEvent], None]


class EventManager:
    """Manages PullPoint and webhook subscriptions for a single camera."""

    def __init__(
        self,
        device: ONVIFCamera,
        name: str,
        uid: str,
        on_event: EventCallback,
        ignored_topics: set[str] | None = None,
        webhook_url: str | None = None,
        register_webhook_handler: Callable[
            [Callable[[web.Request], Awaitable[web.Response]]], None
        ]
        | None = None,
        on_availability: Callable[[bool], None] | None = None,
        pull_timeout: float | None = None,
        pull_hard_slack: float | None = None,
        offline_grace: float | None = None,
        flap_recovery: bool = True,
        stuck_on_timeout: float | None = None,
        subscription_time: float | None = None,
        subscription_restart_interval: float | None = None,
        pullpoint_message_limit: int | None = None,
        pullpoint_cooldown: float | None = None,
        flap_pull_retry_delay: float | None = None,
        flap_resub_delay: float | None = None,
        teardown_timeout: float | None = None,
        periodic_reconnect_hours: float | None = None,
        stale_event_reconnect_hours: float | None = None,
    ) -> None:
        self.device = device
        self.name = name
        self.uid = uid
        self._on_event = on_event
        self._on_availability = on_availability
        self._available: bool | None = None
        self._ignored_topics: set[str] = set(ignored_topics or set())
        # Per-camera overrides for the PullPoint long-poll duration and its
        # hard client-side ceiling; None falls back to the bridge-wide
        # PULLPOINT_POLL_TIME/PULL_HARD_SLACK defaults. See config.py,
        # CameraConfig.pull_timeout/pull_hard_slack for when/why to set
        # these.
        self.pull_timeout_seconds = (
            pull_timeout if pull_timeout is not None else PULLPOINT_POLL_TIME.total_seconds()
        )
        self.pull_hard_slack_seconds = (
            pull_hard_slack if pull_hard_slack is not None else PULL_HARD_SLACK
        )
        # Per-camera override for how long a sustained pull failure is
        # tolerated before the camera is reported offline / a fresh
        # subscription is forced; None falls back to the bridge-wide
        # FLAP_OFFLINE_GRACE default. See config.py, CameraConfig.
        # offline_grace for when/why to set this.
        self.offline_grace_seconds = (
            offline_grace if offline_grace is not None else FLAP_OFFLINE_GRACE
        )
        # Per-camera toggle for fix #2's in-place retry strategy; see
        # config.py, CameraConfig.flap_recovery for when/why to disable it.
        self.flap_recovery_enabled = flap_recovery
        # Further per-camera timing overrides; see config.py,
        # CameraConfig's docstring above each field for when/why to set
        # these. All fall back to their const.py bridge-wide default.
        self.stuck_on_timeout_seconds = (
            stuck_on_timeout if stuck_on_timeout is not None else STUCK_ON_TIMEOUT
        )
        self.subscription_time = (
            dt.timedelta(seconds=subscription_time)
            if subscription_time is not None
            else SUBSCRIPTION_TIME
        )
        self.subscription_restart_interval_seconds = (
            subscription_restart_interval
            if subscription_restart_interval is not None
            else SUBSCRIPTION_RESTART_INTERVAL_ON_ERROR
        )
        self.pullpoint_message_limit = (
            pullpoint_message_limit
            if pullpoint_message_limit is not None
            else PULLPOINT_MESSAGE_LIMIT
        )
        self.pullpoint_cooldown_seconds = (
            pullpoint_cooldown if pullpoint_cooldown is not None else PULLPOINT_COOLDOWN_TIME
        )
        self.flap_pull_retry_delay_seconds = (
            flap_pull_retry_delay
            if flap_pull_retry_delay is not None
            else FLAP_PULL_RETRY_DELAY
        )
        self.flap_resub_delay_seconds = (
            flap_resub_delay if flap_resub_delay is not None else FLAP_RESUB_DELAY
        )
        self.teardown_timeout_seconds = (
            teardown_timeout if teardown_timeout is not None else TEARDOWN_TIMEOUT
        )
        # Periodic unconditional reconnect (see const.py module docstring's
        # bullet on this, and CameraConfig.periodic_reconnect_hours for the
        # full rationale): unlike every other fix in this module, this
        # doesn't react to any detected failure - it defends against a
        # class of camera firmware bug where the ONVIF transport and
        # subscription stay perfectly healthy while the camera's own event
        # engine silently stops producing events for days, invisible at
        # the protocol level. None disables it (default).
        self.periodic_reconnect_hours = periodic_reconnect_hours
        self._periodic_reconnect_task: asyncio.Task[None] | None = None
        # Event-aware alternative/complement to periodic_reconnect_hours
        # above: only force a reconnect once no *event* has actually been
        # received in this many hours, rather than unconditionally on a
        # fixed schedule. Preferred whenever a camera does receive events
        # somewhat regularly, since it never disrupts a camera that's
        # already working fine - periodic_reconnect_hours would reconnect
        # it anyway, every time, for no reason. `_last_event_time` is
        # seeded to "now" so a slow start isn't immediately mistaken for
        # staleness, and is updated on every successfully-parsed event
        # (see async_parse_messages) regardless of dedup/suppression.
        self.stale_event_reconnect_hours = stale_event_reconnect_hours
        self._last_event_time = time.monotonic()
        self._stale_event_check_task: asyncio.Task[None] | None = None
        # Topics without a registered onvif_parsers parser: logged once and
        # then silenced, same as the module-level UNHANDLED_TOPICS set in
        # the original component.
        self._unhandled_topics: set[str] = set()

        # -- stuck-on watchdog state (see const.py: STUCK_ON_*) ------------
        # Monotonic time we last received a `true` for each binary_sensor
        # uid (updated on every received `true`, including repeats that
        # don't change the value) - drives the stuck-on watchdog below.
        self._last_true: dict[str, float] = {}
        # The most recently received "on" ParsedEvent for each uid, kept so
        # we can synthesize a matching "off" with the same metadata
        # (name/platform/device_class/topic) when force-clearing it.
        self._last_event: dict[str, ParsedEvent] = {}
        # -- duplicate-event suppression (ported from tapo-onvif-events) ---
        # Last value actually published to MQTT for each binary_sensor uid.
        # Many cameras resend the same `true` repeatedly (e.g. once a
        # second) for as long as a detection condition holds - without this,
        # every single repeat would turn into its own MQTT message. Only a
        # real off<->on transition (or the first time a uid is seen) is
        # published; see `_update_binary_state`.
        self._current_state: dict[str, bool] = {}
        self._watchdog_task: asyncio.Task[None] | None = None

        self.webhook_manager = WebHookManager(self, webhook_url, register_webhook_handler)
        self.pullpoint_manager = PullPointManager(self)

    @property
    def started(self) -> bool:
        return self.webhook_manager.started or self.pullpoint_manager.started

    async def async_start(self, try_pullpoint: bool, try_webhook: bool) -> bool:
        """Start event retrieval. PullPoint first, then webhook if requested."""
        event_via_pull_point = (
            try_pullpoint and await self.pullpoint_manager.async_start()
        )
        events_via_webhook = try_webhook and await self.webhook_manager.async_start()
        started = events_via_webhook or event_via_pull_point
        if started:
            self._last_event_time = time.monotonic()
            self._watchdog_task = asyncio.get_event_loop().create_task(
                self._async_watchdog_loop()
            )
            if self.periodic_reconnect_hours:
                self._periodic_reconnect_task = asyncio.get_event_loop().create_task(
                    self._async_periodic_reconnect_loop()
                )
            if self.stale_event_reconnect_hours:
                self._stale_event_check_task = asyncio.get_event_loop().create_task(
                    self._async_stale_event_check_loop()
                )
        return started

    async def async_stop(self) -> None:
        """Unsubscribe from all event sources."""
        if self._watchdog_task:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._watchdog_task = None
        if self._periodic_reconnect_task:
            self._periodic_reconnect_task.cancel()
            try:
                await self._periodic_reconnect_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._periodic_reconnect_task = None
        if self._stale_event_check_task:
            self._stale_event_check_task.cancel()
            try:
                await self._stale_event_check_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._stale_event_check_task = None
        await self.pullpoint_manager.async_stop()
        await self.webhook_manager.async_stop()
        # A clean, intentional shutdown is not a flap - report it explicitly
        # rather than leaving the last "online" retained.
        self.async_set_available(False)

    async def _async_force_reconnect_all(self) -> None:
        """Force a fresh subscription on whichever transport(s) are active."""
        if self.pullpoint_manager.state in ("started", "paused"):
            await self.pullpoint_manager.async_force_reconnect()
        if self.webhook_manager.state == "started":
            await self.webhook_manager.async_force_reconnect()

    async def _async_periodic_reconnect_loop(self) -> None:
        """Unconditionally force a fresh subscription every N hours.

        See `periodic_reconnect_hours`'s assignment above and
        `CameraConfig.periodic_reconnect_hours` for the full rationale: this
        is a preventive measure, not a reaction to any detected failure -
        it runs on a schedule regardless of whether PullMessages/webhook
        currently look perfectly healthy, since the failure mode it guards
        against (the camera's own event engine going silently stale) is
        invisible at the ONVIF protocol level.

        Unconditional by design: it reconnects a camera that's already
        working fine just as readily as one that's gone stale, since it has
        no way to tell the two apart. If a camera does receive events with
        some regularity, prefer `stale_event_reconnect_hours`
        (`_async_stale_event_check_loop` below) instead, which only
        reconnects once it can actually tell something's wrong.
        """
        assert self.periodic_reconnect_hours is not None
        interval = self.periodic_reconnect_hours * 3600
        try:
            while True:
                await asyncio.sleep(interval)
                LOGGER.info(
                    "%s: periodic reconnect (every %sh) due - forcing a "
                    "fresh subscription as a preventive measure, "
                    "independent of any detected failure",
                    self.name,
                    self.periodic_reconnect_hours,
                )
                await self._async_force_reconnect_all()
        except asyncio.CancelledError:
            pass

    async def _async_check_stale_event(self) -> None:
        """Perform exactly one staleness check; see the loop below for the
        schedule this runs on. Split out so it can be tested/called
        directly without waiting on real sleep timing.
        """
        assert self.stale_event_reconnect_hours is not None
        threshold = self.stale_event_reconnect_hours * 3600
        idle = time.monotonic() - self._last_event_time
        if idle < threshold:
            return
        LOGGER.warning(
            "%s: no event received in %.1fh (threshold %sh) despite "
            "the connection reporting healthy - forcing a fresh "
            "subscription in case the camera's own event engine has "
            "gone stale",
            self.name,
            idle / 3600,
            self.stale_event_reconnect_hours,
        )
        await self._async_force_reconnect_all()
        # Reset the clock regardless of whether the reconnect above
        # actually fixed anything, so a camera that's genuinely just quiet
        # (nothing happened - not necessarily broken) isn't reconnected
        # again immediately on the very next check.
        self._last_event_time = time.monotonic()

    async def _async_stale_event_check_loop(self) -> None:
        """Force a fresh subscription once no event has arrived in N hours.

        Event-aware alternative/complement to `_async_periodic_reconnect_loop`
        above (see `stale_event_reconnect_hours`'s assignment for the
        rationale): checks periodically rather than reconnecting
        unconditionally on a fixed schedule, so a camera that keeps
        receiving events with some regularity is never disrupted by this -
        only one that's gone genuinely silent for longer than the
        configured threshold is.

        The check cadence is derived from the threshold itself (roughly 10
        checks across the window, floored at 60s so a very short threshold
        doesn't busy-loop) rather than being a separate setting: precise
        timing doesn't matter here, only that a stale camera gets noticed
        reasonably promptly relative to its own threshold.
        """
        assert self.stale_event_reconnect_hours is not None
        check_interval = max(60.0, self.stale_event_reconnect_hours * 360)
        try:
            while True:
                await asyncio.sleep(check_interval)
                await self._async_check_stale_event()
        except asyncio.CancelledError:
            pass

    async def async_parse_messages(self, messages) -> None:
        """Parse incoming notification messages via onvif_parsers.

        This mirrors the original `EventManager.async_parse_messages`
        one-to-one, using the exact same call into the parser library
        (`onvif_parsers.parse(topic, uid, msg)`) and the same error
        handling, just publishing to MQTT instead of updating HA entities.
        The stuck-on watchdog bookkeeping (see module docstring) is layered
        on top before dispatching.
        """
        for msg in messages:
            # Guard against an empty message.
            if not msg.Topic:
                continue

            # Topics may look like the following:
            #
            #   tns1:RuleEngine/CellMotionDetector/Motion//.
            #   tns1:RuleEngine/CellMotionDetector/Motion
            #   tns1:RuleEngine/CellMotionDetector/Motion/
            #   tns1:UserAlarm/IVA/HumanShapeDetect
            #
            # onvif_parsers expects the topic to be:
            #   tns1:RuleEngine/CellMotionDetector/Motion
            topic = msg.Topic._value_1.rstrip("/.")  # noqa: SLF001

            if topic in self._ignored_topics:
                continue

            try:
                events = await onvif_parsers.parse(topic, self.uid, msg)
                error = None
            except onvif_parsers.errors.UnknownTopicError:
                if topic not in self._unhandled_topics:
                    LOGGER.warning(
                        "%s: No registered parser for event from %s: %s",
                        self.name,
                        self.uid,
                        onvif_parsers.util.event_to_debug_format(msg),
                    )
                    self._unhandled_topics.add(topic)
                continue
            except (AttributeError, KeyError) as e:
                events = []
                error = e

            if not events:
                LOGGER.warning(
                    "%s: Unable to parse event from %s: %s: %s",
                    self.name,
                    self.uid,
                    error,
                    onvif_parsers.util.event_to_debug_format(msg),
                )
                continue

            for event in events:
                # A genuine, successfully-parsed event just arrived from
                # the camera - update the staleness clock used by
                # `stale_event_reconnect_hours` regardless of whether this
                # particular event ends up published or suppressed as a
                # duplicate (fix #3): dedup affects what's *republished*,
                # not whether the camera's event engine is demonstrably
                # still alive and producing output.
                self._last_event_time = time.monotonic()

                value = event.value
                if event.device_class == "timestamp" and isinstance(value, str):
                    value = local_datetime_or_none(value)

                parsed_event = ParsedEvent(
                    uid=event.uid,
                    name=event.name,
                    platform=event.platform,
                    topic=topic,
                    device_class=event.device_class,
                    unit_of_measurement=event.unit_of_measurement,
                    value=value,
                    entity_category=event.entity_category,
                    entity_enabled=event.entity_enabled,
                    source=extract_source(msg),
                )
                self._handle_parsed_event(parsed_event)

    def _handle_parsed_event(self, event: ParsedEvent) -> None:
        """Run dedup/watchdog bookkeeping for a binary_sensor event, then
        dispatch it if it should be published.

        Shared by `async_parse_messages` (ONVIF-sourced events) and
        `handle_doorbell_event` (see below) so both go through the exact
        same dedup (fix #3) and stuck-on watchdog (fix #1) pipeline and end
        up published identically.
        """
        if event.platform == "binary_sensor" and isinstance(event.value, bool):
            should_dispatch = self._update_binary_state(event)
        else:
            should_dispatch = True
        if should_dispatch:
            self._dispatch(event)

    def handle_doorbell_event(self, value: bool) -> None:
        """Publish a doorbell button press/release (see doorbell.py).

        Routed through the same dedup + stuck-on watchdog pipeline as any
        ONVIF-sourced binary_sensor event (`_handle_parsed_event`), so it
        ends up published exactly like one - same payload shape, same
        `topic_style` handling in `bridge.py` - and self-heals the same way
        if a crash ever drops the closing "off" pulse.

        Deliberately does NOT touch `_last_event_time`
        (`stale_event_reconnect_hours`'s clock, see its assignment above):
        that clock specifically tracks the ONVIF PullPoint/webhook event
        engine's health, and a doorbell button is a separate physical
        mechanism (a UDP broadcast, nothing to do with ONVIF) - the two
        engines can each be broken independently of the other, so mixing
        their signals would risk masking a genuinely dead ONVIF event
        engine just because the doorbell button still works.
        """
        self._handle_parsed_event(
            ParsedEvent(
                uid=f"{self.uid}_doorbell",
                name="Doorbell",
                platform="binary_sensor",
                topic="doorbell",
                value=value,
            )
        )

    def _update_binary_state(self, event: ParsedEvent) -> bool:
        """Update stuck-on + duplicate-suppression bookkeeping.

        Returns True if `event` represents a real off<->on transition (or
        the first time this uid was seen) and should be published; False if
        it merely repeats the currently-known state and should be
        suppressed. Watchdog bookkeeping (`_last_true`/`_last_event`) is
        updated on every received `true` regardless of the return value, so
        a flood of duplicate `true`s still keeps resetting the stuck-on
        timer even though only the first one is published.
        """
        uid = event.uid
        value = event.value
        if value:
            self._last_true[uid] = time.monotonic()
            self._last_event[uid] = event
        else:
            # A real "off" arrived: the watchdog no longer needs to guard it.
            self._last_true.pop(uid, None)
            self._last_event.pop(uid, None)

        previous = self._current_state.get(uid)
        self._current_state[uid] = value
        changed = previous != value
        if not changed:
            LOGGER.debug(
                "%s: suppressing duplicate %s=%s (no state change)",
                self.name,
                uid,
                value,
            )
        return changed

    def _dispatch(self, event: ParsedEvent) -> None:
        try:
            self._on_event(event)
        except Exception:  # noqa: BLE001 - a broken callback must not stop the bridge
            LOGGER.exception(
                "%s: Error in event callback for topic %s", self.name, event.topic
            )

    def uids_currently_on(self) -> list[str]:
        """Return the uids the watchdog currently believes are "on"."""
        return list(self._last_true)

    def async_check_stuck(self) -> None:
        """Force off any binary_sensor uid held on past STUCK_ON_TIMEOUT.

        Self-heals a dropped-off latch (a detector that emits an `on` with
        no matching `off`). A genuinely active subject keeps refreshing
        `_last_true` via repeated `true`s, so this never fires for it.
        """
        now = time.monotonic()
        timeout = self.stuck_on_timeout_seconds
        for uid, last_true in list(self._last_true.items()):
            if now - last_true <= timeout:
                continue
            self._force_off(uid, f"stuck on for >{timeout:.0f}s with no fresh event")

    async def async_rebaseline(self) -> None:
        """Force every currently-tracked "on" key off.

        Called on every (re)subscribe: the camera keeps no detection state
        across a reconnect (or its own reboot), so any "on" we are still
        holding is stale. A genuinely still-active detection re-fires
        cleanly on the camera's next real event.
        """
        for uid in list(self._last_true):
            self._force_off(uid, "rebaseline after (re)subscribe")

    def _force_off(self, uid: str, reason: str) -> None:
        event = self._last_event.get(uid)
        self._last_true.pop(uid, None)
        self._last_event.pop(uid, None)
        if event is None:
            return
        # Keep dedup state in sync: a genuine "false" from the camera after
        # this point must still be suppressed as a duplicate, and a genuine
        # "true" must be recognized as a real transition again.
        self._current_state[uid] = False
        LOGGER.warning("%s: forcing %s off (%s)", self.name, uid, reason)
        self._dispatch(replace(event, value=False))

    async def _async_watchdog_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(STUCK_ON_CHECK_INTERVAL)
                self.async_check_stuck()
        except asyncio.CancelledError:
            pass

    def async_set_available(self, value: bool) -> None:
        """Update honest availability and notify the bridge on change.

        Availability starts unknown (`None`) and only flips to False once a
        transport has actually observed a sustained failure (see
        `PullPointManager._account_flap`), not merely because a subscribe
        hasn't happened yet - so a slow startup doesn't briefly flash
        "offline".
        """
        if self._available == value:
            return
        self._available = value
        if value:
            LOGGER.info("%s: availability changed to online", self.name)
        else:
            # Going offline is the one availability transition that
            # actually warrants attention: it only happens after a
            # sustained stretch with no successful pull (FLAP_OFFLINE_GRACE,
            # see PullPointManager._account_flap) - a real, ongoing problem,
            # not routine self-healing.
            LOGGER.warning("%s: availability changed to offline", self.name)
        if self._on_availability is not None:
            try:
                self._on_availability(value)
            except Exception:  # noqa: BLE001 - a broken callback must not stop the bridge
                LOGGER.exception("%s: error in availability callback", self.name)

    def async_mark_events_stale(self) -> None:
        """Called when a subscription is lost."""
        LOGGER.debug("%s: Subscription lost, events are considered stale", self.name)

    def async_webhook_failed(self) -> None:
        """Webhook isn't working -> fall back to PullPoint."""
        if self.pullpoint_manager.state != "paused":
            return
        LOGGER.debug("%s: Switching to PullPoint for events", self.name)
        self.pullpoint_manager.async_resume()

    def async_webhook_working(self) -> None:
        """Webhook is working -> pause PullPoint."""
        if self.pullpoint_manager.state != "started":
            return
        LOGGER.debug("%s: Switching to webhook for events", self.name)
        self.pullpoint_manager.async_pause()


class PullPointManager:
    """ONVIF PullPoint manager.

    As long as the webhook is working, the PullPoint subscription is kept
    alive (renewed), but not actively polled. If the webhook fails,
    PullPoint takes over polling again.

    Runs a continuous pull loop (rather than a schedule-one-pull-at-a-time
    pattern) so it can retry a single stuck long-poll or a burst of
    transient transport errors on the *same* subscription (flap recovery,
    see const.py and the module docstring) before falling back to tearing
    the subscription down.
    """

    def __init__(self, event_manager: EventManager) -> None:
        self.state = "stopped"  # stopped | started | paused | failed
        self._event_manager = event_manager
        self._device = event_manager.device
        self._name = event_manager.name

        self._pullpoint_manager: ONVIFPullPointManager | None = None
        self._pull_loop_task: asyncio.Task[None] | None = None

        # -- flap-recovery bookkeeping (see const.py: FLAP_*) --------------
        # Monotonic time of the last PullMessages call that actually
        # completed (even empty). Seeded on (re)subscribe so a fresh
        # subscription always gets a full FLAP_OFFLINE_GRACE window.
        self._last_ok = time.monotonic()
        # Monotonic time of the last (re)subscribe. Escalation from in-place
        # retries to a full refresh is gated on both this and _last_ok, so a
        # sustained zero-success stretch can't collapse the rapid retry into
        # a single attempt per refresh.
        self._last_resub = time.monotonic()
        # Monotonic timestamps of recent transient pull failures, pruned to
        # the last FLAP_WINDOW seconds. Windowed rather than a consecutive
        # counter: a camera that fails, succeeds, fails, succeeds (never 3
        # *in a row*) still gets recognized and quieted down, instead of
        # logging a WARNING on every single failure forever. See
        # `_account_flap`.
        self._fail_timestamps: deque[float] = deque()
        self._last_fail_time: float | None = None
        self._flap_active = False
        self._flap_since: float | None = None
        self._flap_last_notice = 0.0

    @property
    def started(self) -> bool:
        return self.state == "started"

    async def async_start(self) -> bool:
        assert self.state == "stopped", "PullPoint manager already started"
        LOGGER.debug("%s: Starting PullPoint manager", self._name)
        if not await self._async_start_pullpoint():
            self.state = "failed"
            return False
        self._last_resub = time.monotonic()
        self._last_ok = time.monotonic()
        await self._event_manager.async_rebaseline()
        # Subscribing successfully is our best initial signal; the flap
        # tracker (_account_flap/_note_pull_ok) refines this once real
        # pulls start completing (or failing) on this subscription.
        self._event_manager.async_set_available(True)
        self.state = "started"
        self._pull_loop_task = asyncio.get_event_loop().create_task(
            self._async_pull_loop()
        )
        return True

    def async_pause(self) -> None:
        LOGGER.debug("%s: Pausing PullPoint manager", self._name)
        self.state = "paused"
        if self._pull_loop_task:
            self._pull_loop_task.cancel()
            self._pull_loop_task = None
        if self._pullpoint_manager:
            self._pullpoint_manager.pause()
        # We do not unsubscribe from the pullpoint subscription and instead
        # let the subscription expire, since some cameras terminate all
        # subscriptions on unsubscribe, which would break the webhook.

    def async_resume(self) -> None:
        LOGGER.debug("%s: Resuming PullPoint manager", self._name)
        self.state = "started"
        if self._pullpoint_manager:
            self._pullpoint_manager.resume()
        # Reset flap/offline-grace bookkeeping, same as a fresh (re)subscribe
        # (see async_start/_async_pull_loop). While paused, no pulls happen
        # at all, so _last_ok/_last_resub go stale - possibly by hours, if
        # the webhook was healthy for a long stretch before failing and
        # handing back to PullPoint. Without this reset, a single ordinary
        # transient failure on the very first pull after resuming would
        # immediately see `now - _last_ok` as enormous and incorrectly
        # report the camera offline (see _account_flap's FLAP_OFFLINE_GRACE
        # check) - even though the real outage, if any, just started.
        now = time.monotonic()
        self._last_ok = now
        self._last_resub = now
        self._fail_timestamps.clear()
        self._last_fail_time = None
        self._flap_active = False
        self._flap_since = None
        self._pull_loop_task = asyncio.get_event_loop().create_task(
            self._async_pull_loop()
        )

    async def async_stop(self) -> None:
        self.state = "stopped"
        if self._pull_loop_task:
            self._pull_loop_task.cancel()
            try:
                await self._pull_loop_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._pull_loop_task = None
        await self._async_unsubscribe_pullpoint()

    async def async_force_reconnect(self) -> None:
        """Tear down and recreate the PullPoint subscription unconditionally.

        Used by `EventManager`'s periodic-reconnect loop (see
        `CameraConfig.periodic_reconnect_hours`) as a preventive measure
        against a known class of camera firmware bug: the ONVIF transport
        and subscription stay perfectly healthy (`PullMessages` keeps
        succeeding, `available` correctly stays True) while the camera's
        own event/analytics engine silently stops producing any events for
        days - invisible at the protocol level, since there is nothing to
        detect an error from. A fresh subscription sometimes "wakes" a
        stuck camera-side event engine back up even though the connection
        was never actually the problem. Only called when `state` is
        "started" or "paused" (i.e. a subscription actually exists);
        no-op-safe to call on either, and restores whichever of the two
        states it was in afterwards.
        """
        was_started = self.state == "started"
        was_paused = self.state == "paused"
        LOGGER.info("%s: periodic PullPoint reconnect", self._name)
        if self._pull_loop_task:
            self._pull_loop_task.cancel()
            try:
                await self._pull_loop_task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
            self._pull_loop_task = None
        await self._async_unsubscribe_pullpoint()
        ok = await self._async_start_pullpoint()
        now = time.monotonic()
        self._last_resub = now
        self._last_ok = now
        self._fail_timestamps.clear()
        self._last_fail_time = None
        self._flap_active = False
        self._flap_since = None
        if ok:
            await self._event_manager.async_rebaseline()
            self._event_manager.async_set_available(True)
        else:
            LOGGER.warning(
                "%s: periodic reconnect's immediate resubscribe attempt "
                "failed; restarting the pull loop anyway so its own "
                "backoff-retry logic keeps trying rather than leaving "
                "this camera with no active subscription attempt at all",
                self._name,
            )
        # Restart the pull loop (with its own robust resubscribe-on-failure
        # logic) regardless of whether the immediate attempt above
        # succeeded, and restore whichever of the two active states this
        # was in beforehand.
        if was_paused:
            self.state = "paused"
            if self._pullpoint_manager:
                self._pullpoint_manager.pause()
        elif was_started:
            self.state = "started"
            self._pull_loop_task = asyncio.get_event_loop().create_task(
                self._async_pull_loop()
            )

    async def _async_start_pullpoint(self) -> bool:
        try:
            await self._async_create_pullpoint_subscription()
        except CREATE_ERRORS as err:
            LOGGER.debug(
                "%s: Device does not support PullPoint service or has too "
                "many subscriptions: %s",
                self._name,
                stringify_onvif_error(err),
            )
            return False
        return True

    @retry_connection_error(SUBSCRIPTION_ATTEMPTS)
    async def _async_create_pullpoint_subscription(self) -> None:
        self._pullpoint_manager = await self._device.create_pullpoint_manager(
            self._event_manager.subscription_time, self._event_manager.async_mark_events_stale
        )
        await self._pullpoint_manager.set_synchronization_point()

    async def _async_unsubscribe_pullpoint(self) -> None:
        if not self._pullpoint_manager or self._pullpoint_manager.closed:
            self._pullpoint_manager = None
            return
        LOGGER.debug("%s: Unsubscribing from PullPoint", self._name)
        teardown_timeout = self._event_manager.teardown_timeout_seconds
        try:
            # Guarded with a short timeout: right after a stalled poll or a
            # burst of transient errors, the same wedged connection could
            # otherwise make this shutdown itself hang, defeating the point
            # of forcing a fresh subscription in the first place.
            await asyncio.wait_for(
                self._pullpoint_manager.shutdown(), timeout=teardown_timeout
            )
        except asyncio.TimeoutError:
            LOGGER.debug(
                "%s: PullPoint unsubscribe timed out after %ss (connection "
                "likely still wedged); abandoning it and subscribing fresh",
                self._name,
                teardown_timeout,
            )
        except UNSUBSCRIBE_ERRORS as err:
            LOGGER.debug(
                "%s: Failed to unsubscribe PullPoint subscription; this is "
                "normal if the device restarted: %s",
                self._name,
                stringify_onvif_error(err),
            )
        self._pullpoint_manager = None

    # -- flap recovery bookkeeping ---------------------------------------

    def _note_pull_ok(self) -> None:
        """Record a successful long-poll: restores availability.

        Called after every PullMessages that completes (including empty
        idle returns). Does NOT immediately clear an active flap on a lone
        success - see `_account_flap` for why - only once FLAP_CLEAR_AFTER
        seconds have passed without a further failure.
        """
        now = time.monotonic()
        self._last_ok = now
        self._event_manager.async_set_available(True)
        if self._flap_active and (
            self._last_fail_time is None or now - self._last_fail_time >= FLAP_CLEAR_AFTER
        ):
            dur = now - (self._flap_since or now)
            LOGGER.info(
                "%s: ONVIF flap cleared after %.0fs; event flow restored",
                self._name,
                dur,
            )
            self._flap_active = False
            self._flap_since = None
            self._fail_timestamps.clear()

    def _account_flap(self, err: Exception) -> None:
        """Shared flap bookkeeping: windowed failure tracking, heartbeat.

        A transient pull failure retried on the same subscription (see
        `_async_pull_once`) is, by design, routine self-healing: the retry
        typically lands within `FLAP_PULL_RETRY_DELAY` (0.25s), no event is
        lost, and no operator action is possible or needed - so none of
        this logs at WARNING. (A `ServerDisconnectedError` here is not a
        normal ONVIF long-poll timeout completing - `PullMessages` would
        return, even if empty, in that case - it's the camera dropping the
        underlying connection before the call could complete at all, which
        is exactly the failure mode flap recovery exists to absorb.)
        WARNING is reserved for things that actually need attention: a
        forced resubscribe (`PullPointManager._async_pull_loop`) or the
        camera going offline (`EventManager.async_set_available`).

        Once FLAP_THRESHOLD failures land within the last FLAP_WINDOW
        seconds - not necessarily consecutively - repeated per-attempt logs
        collapse into one INFO plus a periodic heartbeat, instead of one
        line per failed attempt. Windowed rather than a simple consecutive-
        failure counter because some cameras (observed on certain Tapo
        firmware) drop a large fraction of individual PullMessages
        connections but recover on almost every retry (fail, succeed, fail,
        succeed) - a "3 in a row" counter would rarely or never trip.
        """
        now = time.monotonic()
        self._last_fail_time = now
        self._fail_timestamps.append(now)
        while self._fail_timestamps and now - self._fail_timestamps[0] > FLAP_WINDOW:
            self._fail_timestamps.popleft()

        if not self._flap_active and len(self._fail_timestamps) >= FLAP_THRESHOLD:
            self._flap_active = True
            self._flap_since = now
            self._flap_last_notice = now
            LOGGER.info(
                "%s: ONVIF flap detected (%d pull failures in the last "
                "%.0fs); holding the subscription and retrying pulls in "
                "place - self-healing, no action needed unless it persists",
                self._name,
                len(self._fail_timestamps),
                FLAP_WINDOW,
            )

        # Honest availability: no successful pull for a while => degraded,
        # rather than every consumer still reading a deceptively healthy
        # "off" while the event feed is actually down. This is the one
        # WARNING-worthy consequence of a flap (logged inside
        # async_set_available), not the individual retries themselves.
        if now - self._last_ok >= self._event_manager.offline_grace_seconds:
            self._event_manager.async_set_available(False)

        if self._flap_active:
            LOGGER.debug("%s: pull failed (transient): %r", self._name, err)
            if now - self._flap_last_notice >= FLAP_HEARTBEAT:
                self._flap_last_notice = now
                LOGGER.info(
                    "%s: ONVIF still flapping after %.0fs (%d fails in the "
                    "last %.0fs); still retrying in place",
                    self._name,
                    now - (self._flap_since or now),
                    len(self._fail_timestamps),
                    FLAP_WINDOW,
                )
        else:
            LOGGER.debug(
                "%s: pull failed (transient): %r; retrying", self._name, err
            )

    # -- pull loop ---------------------------------------------------------

    async def _async_pull_once(self) -> None:
        """Perform exactly one long-poll iteration.

        Transient transport errors are retried on the SAME subscription
        (flap recovery); anything else (including a hard-timeout stall) is
        raised for the caller (`_async_pull_loop`) to handle by forcing a
        fresh subscription.
        """
        if self._pullpoint_manager is None:
            # No active subscription to pull from (e.g. a preceding forced
            # reconnect's resubscribe attempt failed). Raise so the caller
            # (_async_pull_loop) runs its normal resubscribe-with-backoff
            # path instead of crashing the loop outright.
            raise RuntimeError("No active PullPoint subscription to pull from")
        service = self._pullpoint_manager.get_service()
        pull_timeout = dt.timedelta(seconds=self._event_manager.pull_timeout_seconds)
        hard_timeout = (
            self._event_manager.pull_timeout_seconds
            + self._event_manager.pull_hard_slack_seconds
        )

        while True:
            poll_start = time.monotonic()
            try:
                response = await asyncio.wait_for(
                    service.PullMessages(
                        {
                            "MessageLimit": self._event_manager.pullpoint_message_limit,
                            "Timeout": pull_timeout,
                        }
                    ),
                    timeout=hard_timeout,
                )
            except asyncio.TimeoutError as err:
                # The camera held the long-poll open indefinitely (connection
                # alive, but never completing), freezing this loop - and any
                # closing "false" queued behind it - for as long as we let it.
                # Force-abort and let the caller resubscribe; the resulting
                # rebaseline clears any stranded "on" key.
                #
                # If this fires *regularly* for a given camera rather than as
                # a rare one-off, see config.py's CameraConfig.pull_hard_slack
                # docstring: it likely means this camera's normal long-poll
                # round-trip legitimately runs close to or past the default
                # ceiling, and pull_hard_slack should be raised for it rather
                # than treating every occurrence as a real stall.
                on_uids = self._event_manager.uids_currently_on()
                raise RuntimeError(
                    f"PullMessages stalled >{hard_timeout:.0f}s (camera held "
                    "the long-poll open; keys on: "
                    f"{', '.join(on_uids) if on_uids else 'none'}); forcing "
                    "reconnect"
                ) from err
            except Exception as err:  # noqa: BLE001
                if not _is_transient(err):
                    raise
                # FLAP RECOVERY (primary fix, per-camera opt-out via
                # CameraConfig.flap_recovery). A dropped/refused pull
                # connection does not necessarily invalidate the PullPoint
                # subscription, so on a transient transport disconnect we
                # retry the pull on the SAME subscription instead of falling
                # through to a teardown+resubscribe, which would discard the
                # camera's queued events. Only escalate once no pull has
                # succeeded for offline_grace, counted both since the last
                # success and since the last (re)subscribe.
                #
                # Bookkeeping (_account_flap: logging, availability) always
                # runs so those stay accurate either way; only the in-place
                # retry itself is skipped when disabled for this camera,
                # immediately escalating to a fresh subscription instead -
                # for a camera whose disconnects are known to mean the
                # subscription itself died, not just the transport, so
                # retrying it in place would just waste time up to
                # offline_grace before recovering anyway.
                self._account_flap(err)
                if not self._event_manager.flap_recovery_enabled:
                    raise
                now = time.monotonic()
                offline_grace = self._event_manager.offline_grace_seconds
                if now - self._last_ok >= offline_grace and now - self._last_resub >= offline_grace:
                    raise
                await asyncio.sleep(self._event_manager.flap_pull_retry_delay_seconds)
                continue
            break

        self._note_pull_ok()
        messages = getattr(response, "NotificationMessage", None) or []
        LOGGER.debug(
            "%s: PullMessages returned after %.1fs with %d message(s)",
            self._name,
            time.monotonic() - poll_start,
            len(messages),
        )
        if messages:
            await self._event_manager.async_parse_messages(messages)
        # Runs every iteration (>= every long-poll timeout even when idle),
        # so a stuck key self-clears within ~one timeout of stuck_on_timeout.
        self._event_manager.async_check_stuck()
        await asyncio.sleep(self._event_manager.pullpoint_cooldown_seconds)

    async def _async_pull_loop(self) -> None:
        """Continuously pull messages, resubscribing on non-transient errors."""
        try:
            while self.state == "started":
                try:
                    await self._async_pull_once()
                except asyncio.CancelledError:
                    raise
                except Exception as err:  # noqa: BLE001 - force a fresh subscription
                    LOGGER.warning(
                        "%s: PullPoint pull failed, forcing a fresh "
                        "subscription: %r",
                        self._name,
                        err,
                    )
                    await self._async_unsubscribe_pullpoint()
                    if self.state != "started":
                        return
                    await asyncio.sleep(self._event_manager.flap_resub_delay_seconds)
                    if self.state != "started":
                        return
                    if not await self._async_start_pullpoint():
                        restart_interval = (
                            self._event_manager.subscription_restart_interval_seconds
                        )
                        LOGGER.debug(
                            "%s: resubscribe failed, retrying in %ss",
                            self._name,
                            restart_interval,
                        )
                        await asyncio.sleep(restart_interval)
                        continue
                    self._last_resub = time.monotonic()
                    self._last_ok = time.monotonic()
                    # A fresh subscription is a clean slate: any flap being
                    # tracked against the old (now torn-down) subscription
                    # no longer applies to this one.
                    self._fail_timestamps.clear()
                    self._last_fail_time = None
                    self._flap_active = False
                    self._flap_since = None
                    await self._event_manager.async_rebaseline()
                    LOGGER.info("%s: PullPoint resubscribed", self._name)
        except asyncio.CancelledError:
            pass


class WebHookManager:
    """Manages the ONVIF webhook subscription.

    If the camera supports webhooks and our webhook is reachable, it is used
    instead of PullPoint polling for events.

    Unlike Home Assistant, this class does not register its handler with a
    global webhook dispatcher itself. Instead it receives an already
    reachable `webhook_url` and a `register_handler` function from the
    outside (the bridge), which registers the POST handler with the shared
    aiohttp server.
    """

    def __init__(
        self,
        event_manager: EventManager,
        webhook_url: str | None = None,
        register_handler: Callable[[Callable[[web.Request], Awaitable[web.Response]]], None]
        | None = None,
    ) -> None:
        self.state = "stopped"  # stopped | started | failed
        self._event_manager = event_manager
        self._device = event_manager.device
        self._name = event_manager.name
        self._webhook_url = webhook_url
        self._register_handler = register_handler
        self._notification_manager: NotificationManager | None = None
        # Register the aiohttp route right away, synchronously, rather than
        # lazily in async_start(). aiohttp freezes an Application's router
        # as soon as its AppRunner is set up (`await runner.setup()`), which
        # in the bridge's startup sequence happens once, after all
        # CameraBridge/EventManager/WebHookManager instances (and therefore
        # this constructor) have already run for every configured camera -
        # calling add_post() any later, e.g. from async_start(), would hit
        # an already-frozen router and raise "Cannot register a resource
        # into frozen router".
        if self.is_configured():
            self._register_handler(self._async_handle_webhook)

    @property
    def started(self) -> bool:
        return self.state == "started"

    def is_configured(self) -> bool:
        return bool(self._webhook_url and self._register_handler)

    async def async_start(self) -> bool:
        if not self.is_configured():
            LOGGER.debug(
                "%s: No webhook configured, skipping webhook start", self._name
            )
            return False
        LOGGER.debug("%s: Starting webhook manager", self._name)
        assert self.state == "stopped", "Webhook manager already started"
        if not await self._async_start_webhook():
            self.state = "failed"
            return False
        self.state = "started"
        # Unlike PullPoint there is no long-poll heartbeat to refine this
        # further: a webhook that receives no events for a while may simply
        # mean nothing happened, not that the feed is degraded. So webhook
        # availability tracks only "is a subscription currently up".
        self._event_manager.async_set_available(True)
        return True

    async def async_stop(self) -> None:
        self.state = "stopped"
        await self._async_unsubscribe_webhook()

    async def async_force_reconnect(self) -> None:
        """Tear down and recreate the webhook subscription unconditionally.

        Used by `EventManager`'s periodic-reconnect loop (see
        `CameraConfig.periodic_reconnect_hours`); see
        `PullPointManager.async_force_reconnect`'s docstring for why. Unlike
        that one, this never touches the aiohttp route registration (done
        once, in `__init__`, and left untouched here) - only the underlying
        ONVIF subscription is torn down and recreated.
        """
        if self.state != "started":
            return
        LOGGER.info("%s: periodic webhook reconnect", self._name)
        await self._async_unsubscribe_webhook()
        if not await self._async_start_webhook():
            LOGGER.warning(
                "%s: periodic reconnect failed to resubscribe the webhook; "
                "will keep retrying via the normal subscribe path",
                self._name,
            )
            self.state = "failed"
            self._event_manager.async_webhook_failed()
            return
        # _async_create_webhook_subscription() already rebaselines and the
        # subscription succeeding is itself the availability signal for
        # webhook mode (see async_start above).
        self._event_manager.async_set_available(True)

    @retry_connection_error(SUBSCRIPTION_ATTEMPTS)
    async def _async_create_webhook_subscription(self) -> None:
        LOGGER.debug(
            "%s: Creating webhook subscription with URL: %s",
            self._name,
            self._webhook_url,
        )
        try:
            self._notification_manager = await self._device.create_notification_manager(
                address=self._webhook_url,
                interval=self._event_manager.subscription_time,
                subscription_lost_callback=self._event_manager.async_mark_events_stale,
            )
        except ValidationError:
            # This should only happen if there is a problem with the webhook
            # URL that is causing it to not be well formed.
            LOGGER.exception(
                "%s: Validation error while creating webhook subscription "
                "(URL is likely malformed)",
                self._name,
            )
            raise
        await self._notification_manager.set_synchronization_point()
        # Rebaseline (bug fix #1): the camera keeps no detection state across
        # a reconnect, so clear any stale "on" this camera's events were
        # still holding before the fresh subscription starts delivering.
        await self._event_manager.async_rebaseline()
        LOGGER.debug(
            "%s: Webhook subscription created with URL: %s",
            self._name,
            self._webhook_url,
        )

    async def _async_start_webhook(self) -> bool:
        try:
            await self._async_create_webhook_subscription()
        except CREATE_ERRORS as err:
            self._event_manager.async_webhook_failed()
            LOGGER.debug(
                "%s: Device does not support notification service or has "
                "too many subscriptions: %s",
                self._name,
                stringify_onvif_error(err),
            )
            return False
        return True

    async def _async_handle_webhook(self, request: web.Request) -> web.Response:
        """aiohttp handler for incoming webhook POST requests."""
        content: bytes | None = None
        try:
            content = await request.read()
        except ConnectionResetError as ex:
            LOGGER.error("%s: Error reading webhook: %s", self._name, ex)
            return web.Response()

        asyncio.get_event_loop().create_task(self._async_process_webhook(content))
        return web.Response()

    async def _async_process_webhook(self, content: bytes | None) -> None:
        event_manager = self._event_manager
        if content is None:
            # The webhook is marked as not working since something went
            # wrong. It is marked as working again once we receive a valid
            # notification.
            event_manager.async_webhook_failed()
            return
        if not self._notification_manager:
            LOGGER.debug(
                "%s: Received webhook before notification manager is set up",
                self._name,
            )
            return
        if not (result := self._notification_manager.process(content)):
            LOGGER.debug("%s: Failed to process webhook content", self._name)
            return
        LOGGER.debug(
            "%s: Processed webhook with %s event(s)",
            self._name,
            len(result.NotificationMessage),
        )
        event_manager.async_webhook_working()
        # Reaffirm availability on every successfully processed delivery,
        # not just the initial subscribe (see async_start above) - mirrors
        # PullPointManager._note_pull_ok, which does the same for every
        # successful pull. Without this, a camera that briefly flapped
        # offline on PullPoint (fix #4) *before* its first webhook message
        # ever arrived and paused PullPoint (async_webhook_working, which
        # also stops any further PullPoint flap activity for good) would
        # stay stuck reporting "offline" forever afterwards, even though
        # the webhook keeps delivering events perfectly reliably from then
        # on - nothing would ever flip it back to "online" again.
        event_manager.async_set_available(True)
        await event_manager.async_parse_messages(result.NotificationMessage)

    async def _async_unsubscribe_webhook(self) -> None:
        if not self._notification_manager or self._notification_manager.closed:
            return
        LOGGER.debug("%s: Unsubscribing from webhook", self._name)
        teardown_timeout = self._event_manager.teardown_timeout_seconds
        try:
            await asyncio.wait_for(
                self._notification_manager.shutdown(), timeout=teardown_timeout
            )
        except asyncio.TimeoutError:
            LOGGER.debug(
                "%s: Webhook unsubscribe timed out after %ss, abandoning it",
                self._name,
                teardown_timeout,
            )
        except UNSUBSCRIBE_ERRORS as err:
            LOGGER.debug(
                "%s: Failed to unsubscribe webhook subscription; this is "
                "normal if the device restarted: %s",
                self._name,
                stringify_onvif_error(err),
            )
        self._notification_manager = None
