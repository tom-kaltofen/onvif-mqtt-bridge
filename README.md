# ONVIF → MQTT Bridge

**Forward ONVIF camera events as MQTT messages**

The bridge subscribes to the ONVIF event stream of one or more IP cameras
(motion, person/vehicle detection, tamper, ...) and publishes every event as
a JSON message to an MQTT broker. It can also listen for the UDP broadcast
of a physical doorbell button and publish that the same way.

It is built from the event handling of the
[Home Assistant ONVIF integration](https://github.com/home-assistant/core),
parses events with the same [onvif-parsers](https://github.com/openvideolibs/onvif-parsers)
library, talks ONVIF through
[python-onvif-zeep-async](https://github.com/openvideolibs/python-onvif-zeep-async),
publishes with [paho-mqtt](https://github.com/eclipse-paho/paho.mqtt.python), and adds the
reliability fixes from [tapo-onvif-events](https://github.com/catversary/tapo-onvif-events)
that cameras with flaky ONVIF stacks need. See [References](#references).

## Features

- **Many cameras, one process** - each with its own settings.
- **PullPoint and webhook** subscriptions, with automatic failover between them.
- **Uniform events** - every vendor's notifications are normalized by
  `onvif-parsers` into the same JSON shape (`name`, `platform`,
  `device_class`, `value`, ...), including which lens/video source triggered it.
- **Three MQTT topic layouts** - per ONVIF topic, per event name, or one topic per camera.
- **Availability topics** per camera and for the bridge (with MQTT Last Will).
- **Built to survive flaky cameras** - never gives up connecting, retries
  dropped connections in place, clears stuck "on" states, publishes
  repeated "on" events only once, and can reconnect cameras that go silently
  stale. See [Reliability](#reliability).
- **Optional doorbell button** support via UDP broadcast.
- **systemd service** files and an installer.

Developed with the help of Claude Code and heavily tested against three different Tapo cameras (C720, C575D, D235). Other ONVIF
cameras with an Events service should work but have not been tested so far.

## Contents

[How it works](#how-it-works) ·
[Requirements](#requirements) ·
[Quick start](#quick-start) ·
[Configuration](#configuration) ·
[MQTT interface](#mqtt-interface) ·
[Reliability](#reliability) ·
[Doorbell button](#doorbell-button) ·
[Running as a systemd service](#running-as-a-systemd-service) ·
[Troubleshooting](#troubleshooting) ·
[Project layout](#project-layout) ·
[Differences from the Home Assistant integration](#differences-from-the-home-assistant-integration) ·
[References](#references) ·
[License](#license)

## How it works

```
 IP camera ---- ONVIF PullPoint (polling) or webhook (push) ----+
                                                                 |
 doorbell button ---- UDP broadcast ----------------------------+--> onvif-mqtt-bridge --MQTT--> broker --> openHAB, Home Assistant,
                                                                                                            Node-RED, scripts, ...
```

For each camera the bridge opens an ONVIF event subscription, parses every
notification with `onvif-parsers`, filters out repeats, and publishes the
result. PullPoint (the bridge polls the camera) works with practically every
camera. A webhook (the camera pushes to the bridge) is faster but needs a
URL the camera can reach; while it works, polling is paused, and polling
takes over again automatically if the webhook stops.

## Requirements

- Python 3 (developed on 3.11 and 3.12)
- An MQTT broker
- An ONVIF camera with an Events service. For Tapo cameras this is
  typically port **2020** with the camera account created in the Tapo app.
- Python packages from `requirements.txt`:
  [onvif-zeep-async](https://github.com/openvideolibs/python-onvif-zeep-async),
  [onvif-parsers](https://github.com/openvideolibs/onvif-parsers),
  [paho-mqtt](https://github.com/eclipse-paho/paho.mqtt.python),
  [aiohttp](https://github.com/aio-libs/aiohttp) (webhook receiver),
  [PyYAML](https://github.com/yaml/pyyaml/) (configuration file)

## Quick start

```bash
git clone <repository-url> onvif-mqtt-bridge
cd onvif-mqtt-bridge

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

cp config.example.yaml config.yaml
$EDITOR config.yaml               # broker and cameras

./onvif_mqtt_bridge.py --config config.yaml
```

Watch the events arrive:

```bash
mosquitto_sub -h <broker> -t 'onvif/#' -v
```

Command line options: `-c/--config <file>` (default `config.yaml`),
`-v/--verbose` (debug logging, including `onvif`/`zeep`), `--version`.
`onvif_mqtt_bridge.py` is a plain executable script; keep all the `.py` files
in the same directory (they import each other without packages), but that
directory can be anywhere.

To run it permanently, see [Running as a systemd service](#running-as-a-systemd-service).

## Configuration

Everything is configured in one YAML file. [`config.example.yaml`](config.example.yaml)
documents every option in place; a minimal setup looks like this:

```yaml
mqtt:
  host: 192.168.1.10
  username: mqtt_user
  password: mqtt_password

cameras:
  - name: Front Door
    host: 192.168.1.20
    port: 2020
    username: camera_account
    password: camera_password
```

To use the webhook as well, add:

```yaml
webhook:
  enabled: true
  base_url: "http://192.168.1.50:8976"   # how the CAMERA reaches this machine
```

### `mqtt`

| Option | Default | Description |
|---|---|---|
| `host` / `port` | `localhost` / `1883` | Broker address |
| `username` / `password` | - | Broker credentials |
| `client_id` | `onvif-mqtt-bridge` | MQTT client id |
| `use_tls` / `tls_insecure` / `ca_certs` | `false` / `false` / - | TLS settings |
| `qos` / `retain` | `0` / `false` | For event messages |
| `base_topic` | `onvif` | Prefix for all topics |
| `topic_style` | `per_event` | `per_event`, `by_name` or `flat` - see [MQTT interface](#mqtt-interface) |

### `webhook` (optional)

| Option | Default | Description |
|---|---|---|
| `enabled` | `false` | Start the webhook receiver |
| `listen_host` / `listen_port` | `0.0.0.0` / `8976` | Where the bridge listens |
| `base_url` | - | **Required if enabled.** URL under which the *camera* can reach this machine, e.g. `http://192.168.1.50:8976`. Each camera is registered under `<base_url>/onvif/<camera-slug>`. |

### `cameras`

| Option | Default | Description |
|---|---|---|
| `name` | **required** | Camera name; also used for the topic (slugified, e.g. `Front Door` → `front_door`) |
| `host` | **required** | Camera address |
| `port` | `80` | ONVIF port (Tapo: `2020`) |
| `username` / `password` | - | Camera credentials |
| `use_pullpoint` | `true` | Poll the camera for events |
| `use_webhook` | `true` | Let the camera push events (needs the global `webhook` block) |
| `ignored_topics` | `[]` | ONVIF topics to drop, e.g. `tns1:VideoSource/ImageTooBlurry/ImagingService` |
| `unique_id` | slug of `name` | Stable id used in event `uid`s (e.g. the camera's MAC address). Set it if you plan to rename the camera. |
| `nat_override` | `false` | Use `host:port` for every address the camera advertises. Needed if a camera reports an unreachable subscription address (see [Troubleshooting](#troubleshooting)). |

If both `use_pullpoint` and `use_webhook` are `false`, the bridge skips ONVIF for
that camera entirely (useful for a doorbell-only setup).

#### Reliability tuning (all optional)

The defaults suit most cameras - change one only when a camera's log points at
it. What each setting protects against is explained in [Reliability](#reliability).

| Option | Default | Description |
|---|---|---|
| `connect_retry_interval` / `connect_retry_max_interval` | `5` / `60` s | Backoff between connection attempts (doubles up to the maximum) |
| `flap_recovery` | `true` | Retry dropped pull connections in place instead of resubscribing |
| `flap_pull_retry_delay` | `0.25` s | Delay between those in-place retries |
| `flap_resub_delay` | `1.0` s | Pause before re-subscribing after an escalation |
| `offline_grace` | `120` s | Without a successful pull for this long, the camera is reported offline and a fresh subscription is forced |
| `pull_timeout` | `60` s | Long-poll duration requested from the camera |
| `pull_hard_slack` | `10` s | Extra time on top of `pull_timeout` before a hung poll is aborted |
| `pullpoint_message_limit` | `100` | Max events per poll |
| `pullpoint_cooldown` | `0.75` s | Pause between successful polls |
| `subscription_time` | `600` s | Lifetime requested for each subscription (renewed automatically) |
| `subscription_restart_interval` | `60` s | Wait after a non-transient (re)subscribe failure |
| `teardown_timeout` | `5` s | Max wait for an unsubscribe before abandoning it |
| `stuck_on_timeout` | `60` s | A `binary_sensor` still "on" this long without a fresh `true` is forced off |
| `stale_event_reconnect_hours` | off | Force a fresh subscription if no event arrived for this many hours |
| `periodic_reconnect_hours` | off | Force a fresh subscription every N hours, unconditionally |

#### Doorbell options

See [Doorbell button](#doorbell-button): `doorbell_enabled`, `doorbell_ip`,
`doorbell_port`, `doorbell_pulse_seconds`.

## MQTT interface

### Event topics

Every event is published as JSON. The topic depends on `mqtt.topic_style`:

| `topic_style` | Topic | Example |
|---|---|---|
| `per_event` (default) | `<base>/<camera>/<onvif-topic>` | `onvif/front_door/tns1:RuleEngine/CellMotionDetector/Motion` |
| `by_name` | `<base>/<camera>/<event name, slugified>` | `onvif/front_door/cell_motion_detection` |
| `flat` | `<base>/<camera>/event` | `onvif/front_door/event` |

`per_event` lets you filter by event type with MQTT wildcards and gives each type its
own retained value. `by_name` is shorter, but two different ONVIF topics could in
principle slugify to the same name. `flat` is the simplest if you always read the
JSON anyway. The payload is identical in all three; only the topic differs.

### Status topics

Retained plain text, `online` / `offline`:

| Topic | Meaning |
|---|---|
| `<base>/<camera>/status` | Whether the camera's event feed is up. Goes `offline` only after `offline_grace` without a successful pull, or while the bridge cannot connect. |
| `<base>/bridge/status` | The bridge process itself, backed by MQTT Last Will - the broker publishes `offline` if the bridge dies without disconnecting cleanly. |

### Payload

```json
{
  "camera": "Front Door",
  "topic": "tns1:RuleEngine/CellMotionDetector/Motion",
  "uid": "front_door_tns1:RuleEngine/CellMotionDetector/Motion_vsconf_1_va_1_MyMotionDetectorRule",
  "name": "Cell Motion Detection",
  "platform": "binary_sensor",
  "device_class": "motion",
  "unit_of_measurement": null,
  "value": true,
  "entity_category": null,
  "entity_enabled": true,
  "source": {
    "VideoSourceConfigurationToken": "vsconf_1",
    "VideoAnalyticsConfigurationToken": "va_1",
    "Rule": "MyMotionDetectorRule"
  }
}
```

| Field | Description |
|---|---|
| `camera` | Camera name from the config |
| `topic` | Original ONVIF topic (`doorbell` for the doorbell button) |
| `uid` | Unique id of this event source (built by `onvif-parsers` from camera id, topic and source) |
| `name`, `platform`, `device_class`, `unit_of_measurement`, `entity_category`, `entity_enabled` | As produced by `onvif-parsers`; `platform` is `binary_sensor` or `sensor` |
| `value` | `true`/`false` for binary events, otherwise a number, string or ISO-8601 timestamp |
| `source` | The raw ONVIF `Source` items of the notification |

Notes:

- **Binary events are published only on a real change.** Cameras often repeat
  `true` every second while something is detected; you get one `true` and one `false`.
  Numeric and timestamp events are always published.
- **Multi-lens cameras:** each lens is its own ONVIF video source. Events from
  different lenses share topic and `name`; `source` (typically
  `VideoSourceConfigurationToken`) tells them apart, and so does the `uid`. The token
  values are chosen by the camera firmware - look at a real event to see them.
- A `false` may be published by the bridge itself when a camera never sent one (see
  [Reliability](#reliability)); it looks like any other event.

## Reliability

Cheap ONVIF cameras misbehave in ways that make a naive bridge lose events, get
stuck, or silently stop. This is how the bridge copes; the reasoning behind each
point is in [docs/design-notes.md](docs/design-notes.md).

| Problem | What the bridge does | Options |
|---|---|---|
| Camera unreachable at startup (e.g. bridge started before the network) or lost later | Retries forever with exponential backoff, in the background, so it delays neither other cameras nor the doorbell. Reconnects from scratch if every event transport is lost. Reports `offline` meanwhile. | `connect_retry_interval`, `connect_retry_max_interval` |
| Camera drops most pull connections (common on Tapo) | Retries the pull on the *same* subscription instead of tearing it down, so queued events are not lost; escalates to a fresh subscription only after a sustained failure. | `flap_recovery`, `flap_pull_retry_delay`, `offline_grace` |
| Camera holds a long-poll open forever | Aborts it after `pull_timeout + pull_hard_slack` and creates a fresh subscription. | `pull_timeout`, `pull_hard_slack` |
| "On" event without a matching "off" | A watchdog forces the state off after `stuck_on_timeout`; all tracked "on" states are cleared on every (re)subscribe. | `stuck_on_timeout` |
| Same "on" repeated every second | Binary events are published only on change. | - |
| Status showing a healthy "off" while the feed is down | Per-camera availability topic; `offline` after a sustained stretch without a successful pull. | `offline_grace` |
| Unsubscribe hangs on a wedged connection | Teardown is time-limited and abandoned if it hangs. | `teardown_timeout` |
| Camera shows "online" but its event engine died silently | Optional forced reconnect - when no event arrived for N hours (preferred), or every N hours. | `stale_event_reconnect_hours`, `periodic_reconnect_hours` |
| Camera advertises an unreachable subscription address | `nat_override` forces `host:port` for all advertised addresses. | `nat_override` |

Logging is deliberately quiet for self-healing behavior: individual retries are
`DEBUG` and the start/end of a flap is `INFO`. `WARNING` marks things worth a look: a
forced resubscribe, a camera going offline, a state forced off by the watchdog, a
stale camera being reconnected, and connection failures (the first one, then a
reminder every 10 attempts).

## Doorbell button

Some camera/doorbell units have a physical button that is **not** an ONVIF event:
pressing it makes the unit send a UDP broadcast from its own IP. The bridge can
listen for that, independently of (and asynchronously to) the ONVIF event stream. The
approach is based on the doorbell code in
[debian-D235-Doorbell-Press-MQTT](https://github.com/AriaGen/debian-D235-Doorbell-Press-MQTT/tree/main).

```yaml
cameras:
  - name: Front Door
    host: 192.168.1.20
    doorbell_enabled: true
    # doorbell_ip: 192.168.1.20      # default: the camera's host
    # doorbell_port: 20005           # UDP port the doorbell broadcasts to
    # doorbell_pulse_seconds: 2
```

A press is published like any other event - `name: "Doorbell"`, `platform:
"binary_sensor"`, topic `doorbell` (so `onvif/front_door/doorbell` with the default
`per_event` style) - as `value: true`, then `value: false` after
`doorbell_pulse_seconds`. `source` is empty.

- One UDP socket is opened per distinct `doorbell_port`; packets are matched to cameras
  by source IP, so several doorbells can share a port.
- If a port cannot be bound, that doorbell is skipped with an error; the rest of the
  bridge keeps running.
- The doorbell works on a camera with ONVIF disabled
  (`use_pullpoint: false`, `use_webhook: false`).
- The UDP port and payload are model-specific. `20005` was observed on a Tapo D235;
  check your unit's traffic (e.g. `tcpdump -i any udp port 20005`).

## Running as a systemd service

```bash
sudo systemd/install.sh
```

This creates an unprivileged `onvif-mqtt-bridge` user, installs the code and a virtualenv
to `/opt/onvif-mqtt-bridge`, puts an example config at
`/etc/onvif-mqtt-bridge/config.yaml` (never overwriting an existing one), and installs
and enables the unit. It is safe to re-run to update. Then:

```bash
sudo $EDITOR /etc/onvif-mqtt-bridge/config.yaml
sudo systemctl start onvif-mqtt-bridge
journalctl -u onvif-mqtt-bridge -f
```

<details>
<summary>Manual installation</summary>

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin onvif-mqtt-bridge
sudo mkdir -p /opt/onvif-mqtt-bridge /etc/onvif-mqtt-bridge
sudo cp *.py requirements.txt /opt/onvif-mqtt-bridge/
sudo python3 -m venv /opt/onvif-mqtt-bridge/venv
sudo /opt/onvif-mqtt-bridge/venv/bin/pip install -r /opt/onvif-mqtt-bridge/requirements.txt
sudo cp config.example.yaml /etc/onvif-mqtt-bridge/config.yaml
sudo $EDITOR /etc/onvif-mqtt-bridge/config.yaml
sudo chown -R onvif-mqtt-bridge:onvif-mqtt-bridge /opt/onvif-mqtt-bridge /etc/onvif-mqtt-bridge

sudo cp systemd/onvif-mqtt-bridge.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now onvif-mqtt-bridge
```

</details>

The unit runs the bridge sandboxed (`NoNewPrivileges`, `ProtectSystem=strict`,
`ProtectHome`), restarts it if the process dies, and stops it with `SIGINT` so it can
unsubscribe from the cameras and publish `offline` first. If you install elsewhere,
adjust the paths in the unit file. Process restarts are only a safety net: connection
problems are handled inside the bridge without restarting it.

## Troubleshooting

| Symptom | Cause / what to do |
|---|---|
| `could not connect (... Network is unreachable)` at startup | The camera or network was not up yet. The bridge keeps retrying by itself; nothing to do. Tune `connect_retry_*` if you want a different cadence. |
| Nothing arrives, no subscription is created | Check `host`/`port`/credentials (Tapo: port 2020, camera account). Run with `-v` for the full ONVIF conversation. |
| Webhook never delivers | `webhook.base_url` must be reachable **from the camera** (not `localhost`), and the port (default 8976) must be open. PullPoint keeps working as a fallback. |
| PullPoint/webhook cannot connect although the camera is reachable | The camera may advertise a wrong subscription address (e.g. port `1024`). Set `nat_override: true` for that camera. |
| Many `ServerDisconnectedError` lines at `DEBUG` | Normal for Tapo cameras, which drop most pull connections. Flap recovery retries them; no events are lost. |
| `PullMessages stalled >70s ... forcing reconnect` regularly | Your camera's normal long-poll sometimes takes longer than `pull_timeout + pull_hard_slack`. Raise `pull_hard_slack` (e.g. 20-30). |
| Camera flaps `offline` / `online` but events still arrive | Pulls succeed only rarely, around the `offline_grace` threshold. Raise `offline_grace`, and enable the webhook if the camera supports it. |
| Camera `online` but no events for days | The camera's own event engine may have hung. Set `stale_event_reconnect_hours` (e.g. 72), or `periodic_reconnect_hours` (e.g. 24) for a very quiet camera. |
| A motion state seems stuck "on" | It is cleared after `stuck_on_timeout` (60 s) without a fresh `true`. Lower it, or raise it for detectors that stay active for long. |
| Doorbell press not seen | Check `doorbell_ip` and `doorbell_port` against the real traffic (`tcpdump -i any udp port <port>`), and that nothing else binds the port. |
| `no usable event transport` | PullPoint is off and the webhook is not configured. Enable `use_pullpoint`, or set `webhook.enabled` with `webhook.base_url`. |

## Project layout

```
onvif_mqtt_bridge.py   entry point (run this)
bridge.py              ties cameras, event managers, doorbell and MQTT together; connection supervision
event_manager.py       PullPoint / webhook handling, parsing, dedup, watchdog, reconnect logic
event_parser.py        event model and payload serialization
mqtt_publisher.py      paho-mqtt wrapper (reconnect, Last Will)
doorbell.py            optional UDP doorbell listener
config.py, const.py    configuration model and defaults
config.example.yaml    documented example configuration
requirements.txt
systemd/               service unit and installer
docs/design-notes.md   why the reliability features exist and how they work
```

There is no package: the modules import each other with plain imports, so run
`onvif_mqtt_bridge.py` directly.

## Differences from the Home Assistant integration

| Home Assistant | This bridge |
|---|---|
| Turns events into HA entities | Publishes each parsed event as MQTT JSON |
| Webhook URL detected automatically | `webhook.base_url` must be configured |
| Device id from the MAC found during setup | `unique_id` per camera (default: slug of `name`) |
| UI configuration flow | Static `config.yaml` |
| Home Assistant's async helpers and webhook component | Plain `asyncio` and [aiohttp](https://github.com/aio-libs/aiohttp) |

The entity, camera, button, switch and diagnostics parts of the integration have no
role in an MQTT bridge and are not included. Event *parsing* is identical, since both use
`onvif-parsers`.

## References

- [Home Assistant Core](https://github.com/home-assistant/core) - the ONVIF integration whose event handling this project is derived from (Apache-2.0).
- [tapo-onvif-events](https://github.com/catversary/tapo-onvif-events) - Tapo bug fixes: the reconnect, stuck-on and dedup approach ported here (MIT).
- [onvif-parsers](https://github.com/openvideolibs/onvif-parsers) - ONVIF event parsing (Apache-2.0).
- [python-onvif-zeep-async](https://github.com/openvideolibs/python-onvif-zeep-async) - asynchronous ONVIF client (MIT).
- [debian-D235-Doorbell-Press-MQTT](https://github.com/AriaGen/debian-D235-Doorbell-Press-MQTT/tree/main) - doorbell press code this bridge's doorbell support is based on.
- [Eclipse Paho MQTT Python client](https://github.com/eclipse-paho/paho.mqtt.python) - MQTT client (EPL-2.0 / EDL-1.0).
- [aiohttp](https://github.com/aio-libs/aiohttp) - asynchronous HTTP client and server; receives the camera webhooks (Apache-2.0).
- [PyYAML](https://github.com/yaml/pyyaml/) - YAML parser for the configuration file (MIT).

## License

See [LICENSE](LICENSE). Code derived from, and libraries used by, this project keep
their own licenses, listed under [References](#references).
