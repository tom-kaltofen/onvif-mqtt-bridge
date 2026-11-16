"""Configuration model for the ONVIF-MQTT bridge.

Replaces Home Assistant's ConfigEntry / config_flow with a simple, static
YAML configuration file.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_UMLAUT_MAP = str.maketrans(
    {"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss", "Ä": "Ae", "Ö": "Oe", "Ü": "Ue"}
)


def slugify(value: str, fallback: str = "camera") -> str:
    """Build a safe, lowercase, underscore-separated identifier.

    Used for the camera slug (webhook path / MQTT topic prefix) and, with
    `mqtt.topic_style: by_name`, for turning an event's `name` (from
    onvif_parsers, e.g. "Vehicle Detection") into an MQTT topic segment
    (e.g. "vehicle_detection").
    """
    value = value.translate(_UMLAUT_MAP).strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_") or fallback


def _opt_float(data: dict[str, Any], key: str) -> float | None:
    return float(data[key]) if key in data else None


def _opt_int(data: dict[str, Any], key: str) -> int | None:
    return int(data[key]) if key in data else None


@dataclass
class MqttConfig:
    """Connection details for the MQTT broker."""

    host: str = "localhost"
    port: int = 1883
    username: str | None = None
    password: str | None = None
    client_id: str = "onvif-mqtt-bridge"
    use_tls: bool = False
    tls_insecure: bool = False
    ca_certs: str | None = None
    qos: int = 0
    retain: bool = False
    # Base topic under which all events are published, e.g.
    # onvif/<camera>/<onvif-topic>
    base_topic: str = "onvif"
    # "per_event" (default): <base_topic>/<camera>/<onvif-topic>, e.g.
    #   onvif/front_door/tns1:RuleEngine/CellMotionDetector/Motion
    # Lets consumers subscribe/filter per event type with MQTT wildcards
    # (e.g. "onvif/front_door/tns1:RuleEngine/CellMotionDetector/+") and
    # gives each event type its own independently retained last value.
    #
    # "by_name": <base_topic>/<camera>/<slugified event name>, e.g.
    #   onvif/front_door/vehicle_detection
    # (event.name "Vehicle Detection" -> lowercased, spaces/punctuation
    # replaced with "_"; see config.slugify). Shorter and more readable
    # than the raw ONVIF topic while still giving each event type its own
    # topic/retained value, but note that onvif_parsers doesn't guarantee
    # globally unique names across every topic - two distinct ONVIF topics
    # could in principle slugify to the same name and share one MQTT topic.
    # Use "per_event" if that matters for your setup.
    #
    # "flat": every event for a camera goes to a single
    # <base_topic>/<camera>/event topic; the ONVIF topic is still in the
    # payload's "topic" field (nothing is lost), just not in the MQTT topic
    # itself. Simpler for a consumer that subscribes once per camera and
    # always inspects the JSON payload anyway, at the cost of losing
    # per-event-type retained-message semantics and topic-based filtering.
    topic_style: str = "per_event"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MqttConfig:
        topic_style = data.get("topic_style", cls.topic_style)
        if topic_style not in ("per_event", "by_name", "flat"):
            raise ValueError(
                "mqtt.topic_style must be 'per_event', 'by_name' or 'flat', "
                f"got {topic_style!r}"
            )
        return cls(
            host=data.get("host", cls.host),
            port=int(data.get("port", cls.port)),
            username=data.get("username"),
            password=data.get("password"),
            client_id=data.get("client_id", cls.client_id),
            use_tls=bool(data.get("use_tls", cls.use_tls)),
            tls_insecure=bool(data.get("tls_insecure", cls.tls_insecure)),
            ca_certs=data.get("ca_certs"),
            qos=int(data.get("qos", cls.qos)),
            retain=bool(data.get("retain", cls.retain)),
            base_topic=data.get("base_topic", cls.base_topic).rstrip("/"),
            topic_style=topic_style,
        )


@dataclass
class WebhookConfig:
    """Configuration for the optional ONVIF webhook receiver.

    Home Assistant could auto-detect its own external URL
    (helpers.network.get_url). That mechanism does not exist here, so the
    base URL under which this bridge is reachable from the camera must be
    given explicitly.
    """

    enabled: bool = False
    listen_host: str = "0.0.0.0"
    listen_port: int = 8976
    # Base URL reachable from the camera, e.g. "http://192.168.1.5:8976"
    base_url: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WebhookConfig:
        return cls(
            enabled=bool(data.get("enabled", cls.enabled)),
            listen_host=data.get("listen_host", cls.listen_host),
            listen_port=int(data.get("listen_port", cls.listen_port)),
            base_url=(data.get("base_url") or "").rstrip("/") or None,
        )


@dataclass
class CameraConfig:
    """Connection details and options for a single ONVIF camera."""

    name: str
    host: str
    port: int = 80
    username: str | None = None
    password: str | None = None
    use_pullpoint: bool = True
    use_webhook: bool = True
    ignored_topics: set[str] = field(default_factory=set)
    # Stable identifier passed to onvif_parsers.parse() as `uid` and used to
    # prefix each entity's uid (e.g. "<uid>_<topic>"). Home Assistant used the
    # device's MAC address (discovered during config_flow) for this. Since we
    # have no such discovery step here, it defaults to the camera's slug but
    # can be overridden (e.g. with the camera's real MAC address) for
    # stability across renames.
    unique_id: str | None = None
    # Long-poll duration (seconds) requested from the camera via ONVIF
    # PullMessages, and extra slack (seconds) added on top as a hard,
    # client-side ceiling for that same call (see event_manager.py,
    # PullPointManager._async_pull_once / const.PULL_HARD_SLACK). None uses
    # the bridge-wide defaults (const.PULLPOINT_POLL_TIME /
    # const.PULL_HARD_SLACK).
    #
    # If the bridge logs "PullPoint pull failed, forcing a fresh
    # subscription: RuntimeError('PullMessages stalled >Ns ...')" for this
    # camera *regularly* rather than as a rare one-off, it means this
    # camera's normal (non-stuck) long-poll round-trip - camera/network
    # overhead included - sometimes legitimately exceeds the default 70s
    # ceiling (60s poll + 10s slack), and the ceiling is falsely tripping a
    # real subscription teardown+recreate on a connection that was actually
    # still fine. Raise pull_hard_slack for that camera (e.g. to 20-30) to
    # give it more headroom before the bridge gives up on it.
    pull_timeout: float | None = None
    pull_hard_slack: float | None = None
    # Seconds with no successful PullMessages before the camera is reported
    # offline over MQTT and (once also past this long since the last
    # (re)subscribe) a fresh subscription is forced. None uses the
    # bridge-wide default (const.FLAP_OFFLINE_GRACE, 120s).
    #
    # If the log shows "availability changed to offline" followed by
    # "PullPoint pull failed, forcing a fresh subscription:
    # <SomeTransientError>" (the *original* transient error, not a
    # 'PullMessages stalled' RuntimeError - that one is pull_hard_slack's
    # concern, see above) for this camera regularly, it means flap recovery
    # (fix #2) kept retrying in place through repeated failures for the
    # full offline_grace window without a single pull succeeding - a
    # genuinely sustained outage from the bridge's perspective, not stale
    # bookkeeping. If this camera's connectivity is simply worse than the
    # default 120s budget - e.g. it reliably recovers on its own within a
    # few minutes without ever needing a forced resubscribe - raise
    # offline_grace to give it more time before being written off.
    offline_grace: float | None = None
    # Whether transient pull errors (fix #2, "flap recovery") are retried
    # on the same subscription before escalating, or immediately force a
    # fresh subscription like the original component always did. Defaults
    # to enabled: for a camera that essentially never sees transient
    # errors, the retry path simply never triggers (dormant/no-op), and
    # for a flaky one it avoids discarding already-queued events on a mere
    # transport blip (see README, bug fix #2). Set to false only if you
    # know a specific camera's disconnects reliably mean the *subscription*
    # itself died (not just the transport) - in that case, retrying the
    # dead subscription in place is pure wasted time up to offline_grace,
    # and resubscribing immediately recovers faster. This only affects
    # fix #2's in-place retry strategy; the other fixes (stuck-on watchdog,
    # dedup, guarded teardown, availability, hard-timeout, etc.) are always
    # active regardless of this setting.
    flap_recovery: bool = True
    # Some cameras return a broken SubscriptionReference address for
    # PullPoint/webhook subscriptions - wrong port, sometimes even an
    # internal-only IP - which is unreachable even though the camera itself
    # is reachable fine on `host`/`port`. When True, forces every such
    # address the camera returns (also XAddrs, snapshot URIs) to use this
    # camera's `host`/`port` instead of trusting the camera-advertised one;
    # see onvif.client.ONVIFCamera's `nat_override` parameter. Only the
    # host:port is rewritten - the path is kept, so this still targets the
    # correct subscription. Safe to enable even if you're not behind NAT;
    # it only has an effect on cameras that advertise a bad address.
    nat_override: bool = False
    # Further per-camera timing overrides, all optional (None/unset uses
    # the bridge-wide const.py default). Unlike pull_timeout/pull_hard_slack/
    # offline_grace/flap_recovery above, these are lower-traffic knobs -
    # reach for them only if a specific camera's behavior calls for it:
    #
    # - stuck_on_timeout: seconds a binary_sensor event may stay "on" with
    #   no fresh `true` before the watchdog (fix #1) force-clears it to
    #   "off". Default 60. Raise it for a detector with legitimately long
    #   dwell times (e.g. an occupancy/loitering rule) if it's getting
    #   force-cleared while still genuinely active.
    # - subscription_time: lifetime (seconds) requested for each PullPoint/
    #   webhook subscription, renewed automatically before it expires.
    #   Default 600 (10 min). Lower it if a camera enforces a shorter
    #   maximum, or has a low concurrent-subscription limit and you'd
    #   rather it expire (and get cleaned up) faster if the bridge crashes
    #   without unsubscribing.
    # - subscription_restart_interval: seconds to wait before retrying
    #   after a *non-transient* subscribe/resubscribe failure (e.g. the
    #   camera rejected the request outright) - as opposed to flap
    #   recovery's fast in-place retry, which only applies to transient
    #   transport errors. Default 60.
    # - pullpoint_message_limit: max events requested per PullMessages
    #   call. Default 100. Lower it for a camera known to send very large
    #   individual events (bandwidth/memory), raise it if a camera tends to
    #   queue up bursts larger than 100 between polls.
    # - pullpoint_cooldown: seconds to wait between successful pull
    #   iterations. Default 0.75. Raising it trades a little event latency
    #   for less load on a camera that struggles to keep up with polling.
    # - flap_pull_retry_delay: seconds between in-place retry attempts
    #   during flap recovery (fix #2). Default 0.25.
    # - flap_resub_delay: seconds to pause before re-subscribing after an
    #   escalation (fix #2/#8). Default 1.0.
    # - teardown_timeout: seconds to wait for Unsubscribe/shutdown to
    #   complete before abandoning it outright (fix #5). Default 5.
    stuck_on_timeout: float | None = None
    subscription_time: float | None = None
    subscription_restart_interval: float | None = None
    pullpoint_message_limit: int | None = None
    pullpoint_cooldown: float | None = None
    flap_pull_retry_delay: float | None = None
    flap_resub_delay: float | None = None
    teardown_timeout: float | None = None
    # Unconditionally force a fresh PullPoint/webhook subscription every
    # this many hours, regardless of whether anything currently looks
    # unhealthy. None (default) disables this.
    #
    # Unlike every other option above, this isn't a reaction to a detected
    # failure - it's a preventive measure against a known class of camera
    # firmware bug where the ONVIF transport and subscription stay
    # perfectly healthy (PullMessages keeps succeeding, the camera keeps
    # showing "online") while the camera's own event/analytics engine
    # silently stops producing any events at all, sometimes for days -
    # invisible at the protocol level, since there's nothing that actually
    # fails for the bridge to detect. A fresh subscription sometimes
    # "wakes" a camera back up from this state even though the connection
    # itself was never the problem.
    #
    # Unconditional, so it reconnects a camera that's already working fine
    # just as readily as one that's gone stale - it has no way to tell the
    # two apart. If this camera does receive events with some regularity,
    # prefer stale_event_reconnect_hours below instead, which only
    # reconnects once it can actually tell something's wrong. Reach for
    # this one instead for a camera so quiet under normal operation
    # (e.g. a rarely-used space) that "no events for a while" isn't by
    # itself a useful staleness signal - a sensible starting point is once
    # every 24 hours. The cost either way is one brief (sub-second,
    # typically) interruption per cycle: the old subscription is
    # unsubscribed and a new one created, and any binary_sensor state
    # currently tracked "on" is rebaselined to "off" (see bug fix #1's
    # async_rebaseline) as part of it, exactly as happens on any other
    # (re)subscribe.
    periodic_reconnect_hours: float | None = None
    # Event-aware alternative/complement to periodic_reconnect_hours above:
    # force a fresh subscription once no *event* has actually been received
    # in this many hours, rather than unconditionally on a fixed schedule.
    # None (default) disables this.
    #
    # Preferred over periodic_reconnect_hours whenever this camera does
    # receive events with some regularity (the normal case): it never
    # disrupts a camera that's already working fine, since it only
    # reconnects once there's actual evidence of a problem (no event for
    # longer than would be normal for this camera) - periodic_reconnect_hours
    # would reconnect it anyway, every cycle, for no reason. A genuinely
    # quiet-but-healthy stretch (nothing happened) also resets the clock
    # once a reconnect is forced, so an occasional slow day doesn't cause
    # reconnects every single check afterwards.
    #
    # Set this to comfortably longer than the longest gap you'd expect
    # between events on this camera during normal operation - e.g. 48-72
    # for a camera that reliably sees at least one event a day or two, so a
    # single unusually quiet day doesn't itself trigger a reconnect.
    stale_event_reconnect_hours: float | None = None
    # Some camera/doorbell combos additionally have a physical doorbell
    # button that isn't exposed as an ONVIF event at all: pressing it makes
    # the doorbell unit send a UDP broadcast packet from its own IP,
    # observed entirely independently of, and asynchronously to, the
    # ONVIF PullPoint/webhook event stream (see doorbell.py). Disabled by
    # default.
    # Initial-connection retry, in seconds (None = bridge-wide defaults,
    # const.CONNECT_RETRY_INTERVAL / CONNECT_RETRY_MAX_INTERVAL = 5 / 60). If
    # the camera can't be reached - at startup (e.g. the bridge came up
    # before the network did) or later, after every event transport has been
    # down for a while - the bridge keeps retrying, starting at
    # connect_retry_interval and doubling up to connect_retry_max_interval
    # between attempts, until it connects. It never gives up.
    connect_retry_interval: float | None = None
    connect_retry_max_interval: float | None = None
    doorbell_enabled: bool = False
    # IP address the doorbell broadcasts from. Defaults to this camera's
    # own `host` if omitted, which is correct whenever the doorbell button
    # is part of the same physical unit as the camera (the common case);
    # only set this explicitly if the doorbell has a different IP than the
    # camera itself.
    doorbell_ip: str | None = None
    # UDP destination port the doorbell broadcasts a press to. Check your
    # doorbell's documentation/traffic - 20005 was observed for one model
    # and is used here only as a common starting point, not a guarantee.
    doorbell_port: int = 20005
    # How long (seconds) the resulting MQTT "on" state is held before the
    # bridge publishes "off" again, mirroring the reference script's own
    # fixed pulse behavior.
    doorbell_pulse_seconds: float = 2.0

    @property
    def slug(self) -> str:
        """Unique, MQTT-/URL-safe identifier derived from the name."""
        return slugify(self.name)

    @property
    def identifier(self) -> str:
        """Stable identifier used as `uid` for onvif_parsers.parse()."""
        return self.unique_id or self.slug

    @property
    def resolved_doorbell_ip(self) -> str:
        """Doorbell source IP, defaulting to this camera's own `host`."""
        return self.doorbell_ip or self.host

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CameraConfig:
        if "name" not in data or "host" not in data:
            raise ValueError("Every camera requires at least 'name' and 'host'")
        return cls(
            name=data["name"],
            host=data["host"],
            port=int(data.get("port", 80)),
            username=data.get("username"),
            password=data.get("password"),
            use_pullpoint=bool(data.get("use_pullpoint", True)),
            use_webhook=bool(data.get("use_webhook", True)),
            ignored_topics=set(data.get("ignored_topics", [])),
            unique_id=data.get("unique_id"),
            pull_timeout=_opt_float(data, "pull_timeout"),
            pull_hard_slack=_opt_float(data, "pull_hard_slack"),
            nat_override=bool(data.get("nat_override", False)),
            offline_grace=_opt_float(data, "offline_grace"),
            flap_recovery=bool(data.get("flap_recovery", True)),
            stuck_on_timeout=_opt_float(data, "stuck_on_timeout"),
            subscription_time=_opt_float(data, "subscription_time"),
            subscription_restart_interval=_opt_float(
                data, "subscription_restart_interval"
            ),
            pullpoint_message_limit=_opt_int(data, "pullpoint_message_limit"),
            pullpoint_cooldown=_opt_float(data, "pullpoint_cooldown"),
            flap_pull_retry_delay=_opt_float(data, "flap_pull_retry_delay"),
            flap_resub_delay=_opt_float(data, "flap_resub_delay"),
            teardown_timeout=_opt_float(data, "teardown_timeout"),
            periodic_reconnect_hours=_opt_float(data, "periodic_reconnect_hours"),
            stale_event_reconnect_hours=_opt_float(
                data, "stale_event_reconnect_hours"
            ),
            connect_retry_interval=_opt_float(data, "connect_retry_interval"),
            connect_retry_max_interval=_opt_float(data, "connect_retry_max_interval"),
            doorbell_enabled=bool(data.get("doorbell_enabled", False)),
            doorbell_ip=data.get("doorbell_ip"),
            doorbell_port=int(data.get("doorbell_port", 20005)),
            doorbell_pulse_seconds=float(data.get("doorbell_pulse_seconds", 2.0)),
        )


@dataclass
class BridgeConfig:
    """Overall bridge configuration."""

    mqtt: MqttConfig
    webhook: WebhookConfig
    cameras: list[CameraConfig]

    @classmethod
    def from_file(cls, path: str | Path) -> BridgeConfig:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        mqtt = MqttConfig.from_dict(raw.get("mqtt", {}))
        webhook = WebhookConfig.from_dict(raw.get("webhook", {}))
        cameras_raw = raw.get("cameras") or []
        if not cameras_raw:
            raise ValueError("Configuration contains no cameras ('cameras: []')")
        cameras = [CameraConfig.from_dict(c) for c in cameras_raw]
        if webhook.enabled and not webhook.base_url:
            raise ValueError(
                "webhook.enabled is set, but webhook.base_url is missing "
                "(base URL of this bridge, reachable from the camera)"
            )
        return cls(mqtt=mqtt, webhook=webhook, cameras=cameras)
