# Design notes

Background on the reliability behavior of the ONVIF-MQTT bridge: what went
wrong in practice, why, and how each problem is handled. The
[README](../README.md) has the user-facing summary and the configuration
reference; this document is for anyone who wants the reasoning, or who is
changing `event_manager.py` / `bridge.py`.

Fixes 1-5 port the approach of
[tapo-onvif-events](https://github.com/catversary/tapo-onvif-events) (see
[References](../README.md#references)); fixes 6-14 were found while running this
bridge against real Tapo cameras. Function and constant names refer to the
source files in the repository root.

## Reconnect behavior on connection loss

Two independent reconnect mechanisms are involved, and both are kept
identical to their respective originals:

- **ONVIF side** (camera <-> bridge): subscription creation and renewal
  still goes through onvif-zeep-async's `retry_connection_error` decorator,
  unchanged, gated by `SUBSCRIPTION_ATTEMPTS`. The
  PullPoint <-> webhook failover logic (pause/resume on webhook
  success/failure) is unchanged. See `event_manager.py` for details.
- **MQTT side** (bridge <-> broker): left at paho-mqtt's own default
  behavior. `loop_start()` runs `loop_forever(retry_first_connection=True)`
  in a background thread, which keeps retrying the connection - both the
  initial one and any later reconnect - with exponential backoff (1s up to
  120s) for as long as the process runs. No custom reconnect logic was
  added on top; see `mqtt_publisher.py`.

## Bug fixes ported from `tapo-onvif-events`

Five real-world bugs, also present in the original Home Assistant ONVIF
integration, were fixed by porting the approach used by the
[`tapo-onvif-events`](https://github.com/catversary/tapo-onvif-events) Home Assistant custom component
(a from-scratch ONVIF PullPoint client that traced and fixed these issues in
production against Tapo cameras). All five fixes are always on and apply to
every camera/vendor, not just Tapo, since they address the *transport and
event-modeling layer*, not vendor-specific ONVIF quirks.

**1. Binary "on" events with no matching "off"**

Some cameras occasionally emit a detection `on` event and then drop the
closing `off` (observed on basic CellMotion/`IsMotion` detectors). Left
unhandled, an MQTT consumer that tracks "is motion currently active" from
the last received value would see it stuck `true` forever.

Fixed with a transport-independent "stuck-on" watchdog
(`EventManager.async_check_stuck`, `const.STUCK_ON_TIMEOUT` = 60s): for
every `binary_sensor`-platform event, the bridge tracks the time of the
last received `true`. If a key has been "on" for longer than
`STUCK_ON_TIMEOUT` with no fresh `true` (a genuinely active detection keeps
resending `true`, which keeps resetting the timer), the bridge synthesizes
and publishes a matching `false` event with the same `uid`/`name`/
`platform`/`device_class`/`topic`. The same tracked state is also force-
cleared to `off` on every fresh (re)subscribe (`async_rebaseline`), since
a camera keeps no detection state across a reconnect or its own reboot -
any "on" the bridge is still holding at that point is stale, and a
genuinely still-active detection re-fires cleanly on the camera's next
real event.

**2. A reconnect strategy that lost events**

The original approach tears the subscription down and creates a brand new
one on *any* PullPoint error, including a merely transient dropped
connection. That discards the events the camera had already queued for
delivery (`Unsubscribe` invalidates them) and re-baselines state - on a
flaky link that drops the connection every few seconds, this caused
multi-hour silent event gaps in production.

Fixed in `PullPointManager._async_pull_once` (`const.FLAP_*`,
`PULL_HARD_SLACK`): a transient transport error during `PullMessages`
(`aiohttp.ClientConnectionError` / `ConnectionError`, which also covers
`aiohttp.ServerDisconnectedError`) no longer tears the subscription down -
it is retried on the *same* subscription every `FLAP_PULL_RETRY_DELAY`
(0.25s), since a dropped pull connection does not necessarily invalidate
the subscription itself. Escalation to a full resubscribe only happens on
a non-transient error, a hard client-side timeout on the long-poll itself
(`PULL_HARD_SLACK` = long-poll timeout + 10s, guarding against a camera
that holds the connection open without ever completing the call), or once
no pull has succeeded for `FLAP_OFFLINE_GRACE` (120s). Repeated transient
failures are logged as a single WARNING plus a periodic heartbeat instead
of one line per retry (`FLAP_THRESHOLD`, `FLAP_HEARTBEAT`).

**3. One MQTT message per repeated "on" event instead of per transition**

Many cameras resend the same `true` repeatedly (observed up to roughly once
a second) for as long as a detection condition holds, rather than sending
it once and then only the closing `false`. Without deduplication, every
single repeat would turn into its own MQTT message.

Fixed in `EventManager._update_binary_state`: for `binary_sensor`-platform
events, the bridge remembers the last value actually published for each
`uid` and only publishes again on a real off<->on transition (or the first
time a `uid` is seen); repeats of the currently-known value are suppressed
(logged at DEBUG). Suppressed repeats still update the stuck-on watchdog's
timer (fix #1 above), so a flood of duplicate `true`s correctly keeps a
genuinely active detection from being force-cleared. A forced-off event
(from the watchdog or a rebaseline) updates the same dedup state, so a
subsequent real `false` from the camera is still correctly recognized as a
duplicate, and a subsequent real `true` is still correctly recognized as a
transition. Non-`binary_sensor` events (numeric sensors, timestamps, etc.)
are never deduplicated - every value is still published, matching the
original component's behavior for those.

**4. Every consumer still seeing a deceptively healthy "off" while the event
feed was down**

While a flap or a stalled subscription was in progress, MQTT consumers had
no way to tell "camera confirmed motion off" from "we simply haven't heard
from the camera in a while" - both looked identical (the last-known value).

Fixed with an honest, per-camera availability topic:
`<base_topic>/<slug>/status`, payload `online`/`offline` (retained). It is
set `online` as soon as a subscription is (re)established, and - for the
PullPoint transport, which has a natural long-poll heartbeat - flips to
`offline` once no `PullMessages` has succeeded for `FLAP_OFFLINE_GRACE`
(120s), rather than on the very first transient hiccup. A webhook-only
camera's availability instead tracks whether its subscription is currently
up, since a push-based transport has no equivalent idle heartbeat to
degrade on (a healthy but quiet camera would otherwise be flagged
"offline" for having nothing to report). The bridge itself also publishes
an overall `<base_topic>/bridge/status` using MQTT's Last Will and
Testament, so an unclean process death (crash, kill, network drop) is
reported by the broker even when the bridge can't report it itself; a
clean shutdown publishes `offline` explicitly (an LWT does not fire on a
clean disconnect).

**5. A stalled connection could make its own teardown hang too**

Escalating to a fresh subscription (bug fixes #1/#2 above) unsubscribes the
old one first. Right after a stalled long-poll or a burst of transient
errors, that same wedged connection could make the `Unsubscribe`/`shutdown`
call itself hang indefinitely, silently defeating the hard-timeout and
flap-recovery fixes it was supposed to complete.

Fixed by guarding every subscription teardown
(`PullPointManager._async_unsubscribe_pullpoint`,
`WebHookManager._async_unsubscribe_webhook`) with a `TEARDOWN_TIMEOUT` (5s):
past that, the old subscription object is simply abandoned (never awaited
again) and a fresh one is created, instead of blocking the reconnect on a
connection that was already the problem.

## Additional fixes found in this bridge


Two more bugs surfaced from actually running this bridge, on top of the
ports above:

**6. Webhook mode failed with "Cannot register a resource into frozen
router"**

`bridge.py`'s startup registered each camera's webhook HTTP route
(`app.router.add_post(...)`) lazily, inside `WebHookManager.async_start()`
- which ran *after* `await AppRunner.setup()` had already been called on
the shared aiohttp `Application`. aiohttp freezes an application's router
as soon as its `AppRunner` is set up, so any route registration after that
point raises `RuntimeError: Cannot register a resource into frozen router`,
breaking webhook mode entirely.

Fixed by registering each camera's route synchronously in
`WebHookManager.__init__` (i.e. as soon as the camera's `EventManager` is
constructed), which in `Bridge.async_start()` happens for every camera
*before* the shared `AppRunner` is ever set up - not lazily during the
webhook subscription's own `async_start()`.

**7. Sustained WARNING log spam from a camera that fails most pulls but
recovers almost every retry**

Two compounding issues, both about logging only - neither affects whether
events are actually captured (flap recovery already absorbs every drop
regardless of the logging).

First, `tapo-onvif-events`' own flap-deflood logic (and this bridge's
initial port of it, described in fix #2 above) counts *consecutive*
transient failures: once `FLAP_THRESHOLD` (3) land in a row, further
retries are quieted down. That works for a camera that fails in bursts,
but some cameras (in the field, observed on a Tapo C720/D235) instead
fail, succeed, fail, succeed - individual retries recover almost
immediately, so 3 *consecutive* failures rarely or never happen, the
threshold is never crossed, and every single recovered failure logs its
own line indefinitely.

Fixed by switching the flap detector from a consecutive-failure counter to
a sliding time window (`const.FLAP_WINDOW` = 10s): `FLAP_THRESHOLD`
failures within the last `FLAP_WINDOW` seconds - consecutive or not - now
trip the same quiet/heartbeat mode. To avoid flapping the log noise itself
on and off with every other pull, a detected flap is only declared cleared
after `FLAP_CLEAR_AFTER` (30s) with no further failure, rather than on the
very next lone success.

Second, even the throttled version still logged this at WARNING, which
overstates it: a `ServerDisconnectedError` here is not a normal ONVIF
long-poll timeout completing (`PullMessages` always returns, even if
empty, when the camera holds the connection open for the requested
duration) - it's the camera dropping the underlying connection before the
call could complete at all, which is exactly the failure mode flap
recovery exists to absorb, transparently, within `FLAP_PULL_RETRY_DELAY`
(0.25s). Since this requires no operator action and loses no events, it
doesn't warrant a WARNING. Every log line in the transient-retry/flap
lifecycle (`PullPointManager._account_flap`/`_note_pull_ok`) was moved to
DEBUG (individual retries) or INFO (flap detected/cleared, periodic
heartbeat) accordingly. WARNING is now reserved for the two consequences
that actually matter: a forced resubscribe
(`PullPointManager._async_pull_loop`, when a failure turns out to be
non-transient or recovery didn't succeed within `FLAP_OFFLINE_GRACE`) and
the camera's availability actually going offline
(`EventManager.async_set_available`).

**8. No way to tune the hard-timeout ceiling for a camera that's merely
slow, not stuck**

The hard-timeout stall guard (fix #2 above, `PULL_HARD_SLACK`) is a fixed,
bridge-wide 70s ceiling (60s requested long-poll + 10s slack). Unlike the
transient-retry case in fix #7, this one *does* force a real subscription
teardown+recreate when it fires - a legitimately more expensive operation,
so it correctly stays at WARNING. But a fixed global ceiling can't account
for a specific camera whose normal (non-stuck) long-poll round-trip -
camera/network overhead included - sometimes legitimately runs close to or
past 70s: for that camera, the guard meant to catch a genuinely wedged
connection instead regularly fires on connections that were actually still
fine, forcing needless resubscribes (each one briefly interrupting the
event flow and re-baselining tracked state).

Fixed by making both the requested long-poll duration and its hard-timeout
slack configurable per camera (`pull_timeout`/`pull_hard_slack` in
`config.yaml`, see `config.example.yaml`), defaulting to the previous
bridge-wide 60/10 if omitted. If this WARNING fires *regularly* for a
specific camera rather than as a rare one-off, raise that camera's
`pull_hard_slack` (e.g. to 20-30) to give it more headroom before the
bridge gives up on it.

**9. A camera-advertised SubscriptionReference address that's unreachable**

Per the ONVIF/WS-BaseNotification spec, `CreatePullPointSubscription` (and
the webhook `Subscribe` call) returns a `SubscriptionReference` - an
endpoint address the client is supposed to use for every subsequent
`PullMessages`/`Renew`/`Unsubscribe` call on that specific subscription,
separate from the general "events" service address used to create it.
Many camera firmwares, however, return a broken one: the wrong port
(a fixed `1024` has been observed), or even an internal-only IP - address
that's unreachable even though the camera itself is reachable fine.

`onvif-zeep-async` (the library this bridge uses for all ONVIF
communication) already has a built-in fix for exactly this, gated behind
its `nat_override` constructor parameter (off by default, so cameras that
already report the address correctly are unaffected): when enabled, every
such camera-advertised address (subscription references, XAddrs, snapshot
URIs) has its host:port forcibly rewritten to the camera's configured
`host`/`port` - only the host:port, so the path (and therefore which
specific subscription is addressed) is preserved.

This bridge didn't expose that parameter at all, so there was no way to
enable it short of patching the library. Fixed by adding a per-camera
`nat_override: true` option in `config.yaml` (see `config.example.yaml`),
passed straight through to `ONVIFCamera(...)` in `bridge.py`.

**10. Spurious "offline" reports right after PullPoint resumes from a long
pause**

With both PullPoint and webhook enabled (the default), PullPoint is paused
- its pull loop task cancelled outright - whenever the webhook is doing the
job (`EventManager.async_webhook_working`), and only resumed if the
webhook later fails (`async_webhook_failed`). While paused, no pulls
happen at all, so `PullPointManager._last_ok` (the timestamp the offline
check in fix #4 is based on) goes stale - possibly by hours, if the
webhook had been healthy for a long stretch beforehand. `async_resume()`
didn't reset it. So the very next *ordinary* transient pull failure right
after resuming - not a real 120s outage, just a routine hiccup - saw
`now - _last_ok` as enormous and immediately, incorrectly reported the
camera offline over MQTT, typically self-correcting within moments (once
that pull's in-place retry - fix #2 - succeeds) since the event feed
itself (via the webhook, or PullPoint's own quick recovery) was never
actually interrupted for anywhere near that long.

Fixed by resetting the flap/offline-grace bookkeeping
(`_last_ok`/`_last_resub`/the failure-timestamp window) in
`PullPointManager.async_resume()`, the same way a fresh (re)subscribe
already does - a resume restarts the polling cadence from scratch, so
stale bookkeeping from before the pause must not carry over. The
equivalent reset was also added after a successful forced resubscribe
(fix #2/#8's escalation path), which reset `_last_ok`/`_last_resub` but not
the flap-window/heartbeat state, for the same reason.

**11. No per-camera control over how long a sustained outage is tolerated,
or over flap recovery itself**

Even with fix #10 above, a genuinely sustained failure - flap recovery
(fix #2) retrying in place through repeated transient errors for the full
`FLAP_OFFLINE_GRACE` (120s) without a single pull ever succeeding - will
correctly mark the camera offline and eventually force a fresh
subscription. That's working as intended, but the fixed, bridge-wide 120s
budget can't fit every camera: one with connectivity flaky enough to
occasionally exceed it, but that otherwise reliably recovers on its own
within a few minutes, would get written off (and its state rebaselined)
more aggressively than necessary.

Fixed by adding a per-camera `offline_grace` override in `config.yaml`
(seconds), defaulting to the previous bridge-wide 120 if omitted - the
same override pattern as fix #8's `pull_hard_slack`.

Separately: `tapo-onvif-events` itself ships flap recovery as an opt-in,
default-off setting - a deliberate rollout caution for introducing new
behavior into an existing, previously-untouched code path, not a sign the
behavior is risky. This bridge instead defaults it to *on*, since the
in-place retry is a no-op for a camera that never sees transient pull
errors, and a clear win (avoids discarding already-queued events) for one
that does - there isn't a scenario where a healthy camera is worse off for
having it available. There is, however, a genuine trade-off for a specific
camera whose disconnects are known to reliably mean the *subscription*
itself died (not just the transport): retrying that dead subscription in
place is pure wasted time up to `offline_grace`, and resubscribing
immediately would recover faster. Added a per-camera `flap_recovery: false`
override for exactly that case (`config.example.yaml`); all the *other*
fixes above (stuck-on watchdog, dedup, guarded teardown, availability,
hard-timeout, etc.) stay active regardless of this setting - it only
affects fix #2's in-place-retry strategy.

**12. A camera can report "online" for days with a silently dead event engine**

Every fix above reacts to something the bridge can actually detect at the
ONVIF protocol level - a dropped connection, a stalled call, a sustained
failure. But there's a class of camera firmware bug that's invisible at
that level entirely: the ONVIF web service itself stays perfectly
responsive (`PullMessages` keeps succeeding, correctly keeping the camera
"online") while the camera's own event/motion-detection engine silently
stops producing *any* events after a long uptime - there is no error to
react to, because nothing about the transport or subscription is actually
wrong.

Fixed with an unconditional, scheduled periodic reconnect
(`EventManager._async_periodic_reconnect_loop`,
`CameraConfig.periodic_reconnect_hours`, disabled by default): every N
hours, force a fresh PullPoint and/or webhook subscription regardless of
whether any failure was detected - a fresh subscription sometimes "wakes"
a stuck camera-side event engine back up even though the connection was
never the problem. Implemented by reusing the existing teardown/resubscribe
machinery (`PullPointManager`/`WebHookManager.async_force_reconnect`)
rather than recreating the `ONVIFCamera` device or `EventManager` from
scratch, specifically so it never touches the webhook's aiohttp route
registration (done once, see fix #6) - only the underlying ONVIF
subscription is torn down and recreated.

Being unconditional is also this approach's weakness: it reconnects a
camera that's already reporting events perfectly normally just as readily
as a genuinely stale one, since a fixed schedule has no way to tell the
two apart - a real (if brief) disruption for no reason, on every cycle, for
any camera that didn't actually need it. Fixed with an event-aware
alternative (`EventManager._async_stale_event_check_loop`,
`CameraConfig.stale_event_reconnect_hours`, also disabled by default,
preferred whenever a camera does see events with some regularity): instead
of reconnecting on a fixed schedule, the bridge tracks the time of the
last *successfully parsed event* (`_last_event_time`, updated in
`async_parse_messages` regardless of whether that particular event was
then published or suppressed as a duplicate by fix #3 - dedup affects
republishing, not whether the camera's event engine is demonstrably still
alive) and only forces a reconnect once that idle time exceeds the
configured threshold, checked periodically at a cadence derived from the
threshold itself. A camera that keeps producing events is therefore never
touched by this; the clock also resets after each forced reconnect, so a
single unusually quiet stretch that turns out to be nothing (rather than a
stuck event engine) doesn't keep re-triggering a reconnect on every
subsequent check.

Two robustness gaps surfaced while building this and were fixed alongside
it, both about what happens if a forced reconnect's own resubscribe
attempt fails (e.g. the camera happens to be briefly unreachable exactly
when it fires):
- `PullPointManager.async_force_reconnect` used to leave the manager in a
  dead `"failed"` state with no active task and no scheduled retry at all
  in that case - stuck until the *next* scheduled reconnect, potentially
  many hours away. Fixed to always restart the normal pull loop afterwards
  regardless of whether the immediate attempt succeeded, so its own
  existing backoff-retry logic (fix #2's escalation path) takes over
  instead.
- That path relied on `_async_pull_once` asserting `_pullpoint_manager is
  not None`, which would surface as a raw, uninformative
  `AssertionError` if a resubscribe left it `None` (also strippable
  entirely by Python's `-O` flag). Changed to a regular, descriptive
  exception so it flows through the same escalation/retry handling as any
  other pull failure.

**13. A camera stuck "offline" while its webhook keeps delivering events**

With both PullPoint and webhook enabled (the default), PullPoint always
starts first and is only paused once the webhook delivers its first
message (`EventManager.async_webhook_working`). Availability (fix #4) is
reaffirmed to `online` by every successful PullPoint pull
(`PullPointManager._note_pull_ok`) - but webhook deliveries never did the
same: `WebHookManager._async_process_webhook` dispatched the events and
called `async_webhook_working()`, and that was it. Webhook only set
availability once, at the initial subscribe.

So if PullPoint flapped long enough to cross `offline_grace` *before* the
first webhook message arrived (entirely plausible on a camera with the
Tapo connection-dropping quirk, see fix #7), the camera was marked
`offline`; then the first webhook message arrived and paused PullPoint -
which also meant PullPoint could never report `online` again - and nothing
else ever flipped availability back. The camera stayed stuck on `offline`
indefinitely while the webhook delivered every event reliably. The visible
symptom: "goes online, then shortly after offline, but still receives ONVIF
events".

Fixed by having `_async_process_webhook` reaffirm `online` on every
successfully processed delivery, mirroring `_note_pull_ok`. Repeated
deliveries are no-ops once already `online` (`async_set_available` only
acts on a change), so this causes no extra MQTT traffic.

**14. A camera unreachable at startup was never retried (and the time-based
reconnects never started for it)**

If a camera couldn't be reached when the bridge started - typically a boot
race, with the bridge up before the network ("Cannot connect to host ...
[Network is unreachable]") - `update_xaddrs()` raised, `CameraBridge`
logged `startup failed` and gave up for the lifetime of the process. Two
more things went wrong with it:

- `periodic_reconnect_hours` and `stale_event_reconnect_hours` are created
  inside `EventManager.async_start`, and only after it succeeded - for a
  camera that never connected they simply never existed, so no amount of
  waiting triggered a new connection attempt.
- Even a half-failed start (camera reachable, subscription refused) could not
  have been repeated: `PullPointManager`/`WebHookManager.async_start` assert
  state `"stopped"`, and a failed start leaves `"failed"`.

Connecting is now supervised per camera (`CameraBridge._async_supervise`, a
background task, so an unreachable camera delays neither the other cameras
nor the doorbell listener nor the bridge's own startup). It retries
forever with exponential backoff (`connect_retry_interval` to
`connect_retry_max_interval`, default 5s to 60s), resets the EventManager
between attempts so it is restartable, and publishes `offline` right away
(which also replaces a stale retained `online` from a previous run). Only the
first failure and then one reminder every 10 attempts are logged at
WARNING; the rest is DEBUG. The retry loop is stronger than any
hours-based timer, so for a never-connected camera it replaces them; once
connected, `periodic_reconnect_hours`/`stale_event_reconnect_hours` start as
before.

The supervisor keeps watching afterwards: if no event transport (PullPoint
or webhook) has been up for 30 seconds - e.g. a webhook-only camera whose
resubscribe failed, which nothing retried before - it reconnects from
scratch. A configuration that can never work (PullPoint off and the webhook
not configured) is reported once as an error instead of being retried.

## Doorbell button: design notes

**On the stuck-on watchdog for doorbell presses specifically:** unlike an
ONVIF event, where the closing "off" depends on the camera itself sending
it and can get lost on the wire (fix #1's original motivation), a doorbell
press's "on" and "off" are both entirely the bridge's own responsibility
(`DoorbellListener._async_pulse`, a fixed `on -> sleep -> off` the bridge
controls end to end). Its `try/finally` already guarantees "off" is sent
even if the pulse is cancelled mid-sleep (e.g. bridge shutdown) or if
publishing "on" itself unexpectedly fails - the only way "off" could still
be lost is the process dying outright, at which point the watchdog would
not be running to help either. So routing doorbell presses through the
same dedup/watchdog pipeline as ONVIF events is defense-in-depth and
code-path consistency, not a load-bearing necessity the way it is for
ONVIF events.

**A camera can use only the doorbell, with ONVIF fully disabled** (set
both `use_pullpoint: false` and `use_webhook: false`): the bridge skips
the ONVIF connection entirely for that camera rather than attempting it
and then warning it "failed" - nothing failed, that's the requested
configuration - while the doorbell listener (if `doorbell_enabled: true`)
still works exactly the same, since it never depended on ONVIF being
connected in the first place.
