"""Constants for the ONVIF-MQTT bridge.

Base subscription timing (SUBSCRIPTION_*, PULLPOINT_*) is taken 1:1 from the
Home Assistant ONVIF integration since it has proven reliable in practice.

STUCK_ON_* and FLAP_*/PULL_HARD_SLACK are ported from the `tapo-onvif-events`
project (https://github.com/, "Tapo ONVIF Events" custom component), which
fixed two real-world bugs also present in the original HA component:

1. Binary "on" events without a matching "off" (a dropped/lost closing
   event leaves a detection latched on forever). Fixed by a transport-
   independent "stuck on" watchdog plus a state rebaseline on every
   (re)subscribe. See `event_manager.EventManager.async_check_stuck` /
   `async_rebaseline`.
2. A reconnect strategy that tore the subscription down on every transient
   network hiccup, discarding the camera's queued events and causing
   multi-hour event loss on flaky links. Fixed by retrying `PullMessages`
   on the *same* subscription for transient transport errors instead of
   immediately resubscribing. See `event_manager.PullPointManager`.
3. Every entity/consumer still reading a deceptively healthy "off" while
   the event feed was actually down. Fixed with an honest per-camera
   availability topic, only marked offline after a sustained stretch with
   no successful pull. See `AVAILABILITY_*` below and
   `bridge.CameraBridge`.
"""

from __future__ import annotations

import datetime as dt
import logging

LOGGER = logging.getLogger("onvif_mqtt_bridge")

# We only keep a subscription alive for 10 minutes and renew it every
# 8 minutes. This avoids accumulating subscriptions on the camera that
# could not be cleaned up otherwise (ONVIF has no way to list existing
# subscriptions).
SUBSCRIPTION_TIME = dt.timedelta(minutes=10)

# Number of attempts when creating/renewing a subscription.
SUBSCRIPTION_ATTEMPTS = 2

# Delay before retrying to (re)start a subscription after a *non-transient*
# error (e.g. the camera rejected the subscription outright).
SUBSCRIPTION_RESTART_INTERVAL_ON_ERROR = 60

PULLPOINT_POLL_TIME = dt.timedelta(seconds=60)
PULLPOINT_MESSAGE_LIMIT = 100
PULLPOINT_COOLDOWN_TIME = 0.75

# Topics that are ignored by default (e.g. because they are extremely
# frequent and uninteresting). Can be extended per camera in the
# configuration.
DEFAULT_IGNORED_TOPICS: set[str] = {"tns1:MediaControl/VideoEncoderConfiguration"}

# --- Stuck-on watchdog (bug fix #1: "on" events with no matching "off") -----
# A binary_sensor-style event held "on" this long with no fresh `true` from
# the camera is treated as a stale/latched value (its closing `false` was
# dropped) and force-cleared to "off". Some detectors (e.g. a basic
# CellMotion/IsMotion detector) are known to occasionally emit an `on` with
# no matching `off`; detectors that stream repeated `true`s while a subject
# is genuinely present never trip this, since every repeat refreshes the
# timer.
STUCK_ON_TIMEOUT = 60
# How often the watchdog checks for stale "on" values.
STUCK_ON_CHECK_INTERVAL = 10

# --- Flap recovery (bug fix #2: reconnect losing events) --------------------
# Hard client-side ceiling on a single PullMessages call: requested ONVIF
# long-poll timeout (PULLPOINT_POLL_TIME) + this slack. A healthy camera
# always returns within PULLPOINT_POLL_TIME; some cameras occasionally hold
# the long-poll connection open indefinitely (alive, but never completing),
# which freezes the pull loop - and the stuck-on watchdog riding on top of
# it - for hours. Past this ceiling the poll is force-aborted and a fresh
# subscription is created, which also re-baselines every tracked key to off.
PULL_HARD_SLACK = 10
# A dropped/refused pull connection (aiohttp.ClientConnectionError /
# ConnectionError, which also covers aiohttp.ServerDisconnectedError) does
# NOT necessarily invalidate the PullPoint subscription: some cameras keep
# queuing events on a still-valid subscription while individual PullMessages
# connections keep failing. So on a transient transport error we retry
# PullMessages on the SAME subscription at this cadence instead of tearing
# it down immediately - which would discard the camera's queued events and
# is what previously caused multi-hour silent event gaps on flaky links.
FLAP_PULL_RETRY_DELAY = 0.25
# Escalate from in-place retries to a full subscription refresh only once no
# pull has succeeded for this long, counted both since the last success and
# since the last (re)subscribe - so a sustained zero-success stretch can't
# permanently collapse the rapid retry into a single attempt per refresh.
# Must exceed one idle long-poll (PULLPOINT_POLL_TIME) so a quiet, healthy
# camera - which still completes an empty PullMessages every long-poll -
# never trips it.
FLAP_OFFLINE_GRACE = 120
# Small, fixed pause before re-subscribing after an escalation, so the
# bridge resumes rapid pulling on the fresh subscription quickly without
# hammering CreatePullPointSubscription (heavier than a pull, and cameras
# often have a low subscription cap).
FLAP_RESUB_DELAY = 1.0
# Transient pull failures within FLAP_WINDOW seconds before a "flap" is
# logged as a single WARNING (with per-retry noise dropped to DEBUG) instead
# of one WARNING per failed attempt. Deliberately windowed rather than
# "N consecutive": some cameras (observed on certain Tapo firmware) drop a
# large fraction of individual PullMessages connections but recover on
# almost every retry - i.e. fail, succeed, fail, succeed - which would
# never reach a "3 in a row" threshold and would instead log a WARNING on
# every single failure forever, even though nothing is actually wrong
# (flap recovery is still transparently absorbing every drop).
FLAP_THRESHOLD = 3
FLAP_WINDOW = 10
# Once flapping, require this many seconds without a further failure before
# declaring it cleared (rather than the very next lone success), so a
# chronically-dropping camera doesn't flip the log noise on and off with
# every other pull.
FLAP_CLEAR_AFTER = 30
# While flapping, emit one heartbeat WARNING this often so a long outage is
# still visible in the logs without flooding them.
FLAP_HEARTBEAT = 300

# Guard subscription teardown (Unsubscribe/shutdown, device.close()) with this
# timeout. Right after a stalled poll or a burst of transient errors, the
# same wedged connection could otherwise make the teardown itself hang,
# defeating the point of the hard-timeout/flap-recovery fixes above.
TEARDOWN_TIMEOUT = 5

# --- Availability (bug fix #3: entities stayed a deceptively healthy "off") -
# Payloads published to the per-camera availability topic
# (<base_topic>/<slug>/status) and the bridge-level status topic
# (<base_topic>/bridge/status). A camera is marked OFFLINE once no
# PullMessages has succeeded for FLAP_OFFLINE_GRACE (see above) - not on the
# very first transient hiccup - so a brief blip doesn't flap availability.
AVAILABILITY_ONLINE = "online"
AVAILABILITY_OFFLINE = "offline"

# --- Initial connection / supervision (bridge.py: CameraBridge) --------------
# A camera that can't be reached when the bridge starts (typically a boot
# race: the bridge comes up before the network does, "Network is
# unreachable") is retried indefinitely with exponential backoff between
# these two bounds, instead of being given up on after one failed attempt.
CONNECT_RETRY_INTERVAL = 5
CONNECT_RETRY_MAX_INTERVAL = 60
# While a camera stays unreachable, only the first failure and then one
# reminder every this many attempts is logged at WARNING (the rest is DEBUG),
# so a camera that is off for a day doesn't flood the log.
CONNECT_RETRY_LOG_EVERY = 10
# Once connected, the supervisor checks this often whether at least one event
# transport (PullPoint or webhook) is still up, and only treats them as
# lost - and reconnects from scratch - after they've been down for the grace
# period, so a transport that is briefly mid-reconnect isn't mistaken for a
# dead one.
SUPERVISE_POLL_INTERVAL = 10
SUPERVISE_DOWN_GRACE = 30
