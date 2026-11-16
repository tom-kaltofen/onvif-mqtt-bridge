"""Parsed-event model and value normalization for MQTT publishing.

Actual message parsing is delegated to the `onvif_parsers` library (the same
library used by Home Assistant's ONVIF integration), so every event is
published to MQTT in the same normalized shape Home Assistant would use to
build its entities - see `event_manager.EventManager.async_parse_messages`
for where `onvif_parsers.parse()` is called.

This module only defines:
- `ParsedEvent`: a plain dataclass mirroring `onvif_parsers.model.EventEntity`
  plus the originating ONVIF topic, ready to be serialized as an MQTT
  payload.
- `local_datetime_or_none`: the same timestamp normalization Home Assistant
  applied to `device_class == "timestamp"` values (see original
  `event_manager.py`, `_local_datetime_or_none`).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ParsedEvent:
    """A normalized ONVIF event, as produced by onvif_parsers, ready for MQTT.

    Field names intentionally mirror `onvif_parsers.model.EventEntity` so the
    MQTT payload matches the parser library's output one-to-one; `topic`
    is added on top since the original topic isn't part of `EventEntity`.
    """

    uid: str
    name: str
    platform: str
    topic: str
    device_class: str | None = None
    unit_of_measurement: str | None = None
    value: Any = None
    entity_category: str | None = None
    entity_enabled: bool = True
    # The raw ONVIF `Source` SimpleItems of the originating message
    # (Name -> Value), e.g. {"VideoSourceConfigurationToken": "vsconf_2",
    # "Rule": "MyRule"}. This is what tells apart events from different
    # lenses/channels of a multi-lens camera - each lens is its own
    # VideoSource - whose events otherwise share the same topic and name.
    # Empty for events that don't originate from an ONVIF message (e.g. the
    # doorbell button).
    source: dict[str, str] = field(default_factory=dict)

    def as_mqtt_payload(self, camera_name: str) -> dict[str, Any]:
        """Build a JSON-serializable dict for the MQTT payload."""
        return {
            "camera": camera_name,
            "topic": self.topic,
            "uid": self.uid,
            "name": self.name,
            "platform": self.platform,
            "device_class": self.device_class,
            "unit_of_measurement": self.unit_of_measurement,
            "value": _jsonify(self.value),
            "entity_category": self.entity_category,
            "entity_enabled": self.entity_enabled,
            "source": _jsonify_dict(self.source),
        }


def _jsonify_dict(values: dict[str, Any]) -> dict[str, Any]:
    return {str(k): _jsonify(v) for k, v in values.items()}


def extract_source(msg: Any) -> dict[str, str]:
    """Return the ONVIF `Source` SimpleItems of a raw message as a dict.

    Defensive on purpose: this runs on every event, and cameras differ in
    whether `Source` is present and whether `SimpleItem` is a list or a
    single item. Anything unexpected yields an empty dict rather than
    breaking event delivery.
    """
    try:
        payload = msg.Message._value_1  # noqa: SLF001
        items = payload.Source.SimpleItem
    except AttributeError:
        return {}
    if items is None:
        return {}
    if not isinstance(items, (list, tuple)):
        items = [items]
    result: dict[str, str] = {}
    for item in items:
        name = getattr(item, "Name", None)
        if name is not None:
            result[str(name)] = getattr(item, "Value", None)
    return result


def _jsonify(value: Any) -> Any:
    """Make zeep/parser values (datetime, Decimal, ...) JSON-safe."""
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def local_datetime_or_none(value: str) -> dt.datetime | None:
    """Convert a timestamp string to a timezone-aware local datetime.

    Mirrors the original component's `_local_datetime_or_none`, which used
    `homeassistant.util.dt.parse_datetime` + `as_local`. Some cameras return
    broken timestamps like '0000-00-00T00:00:00Z' (e.g. Hikvision), which
    must not raise but simply be treated as "unknown" (return None).
    """
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone()
