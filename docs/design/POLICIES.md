# Design: Policy objects for Messenger drivers

Status: DRAFT for discussion (2026-08-06)
Goals: grouped, typed configuration (policies) • backward compatibility for
the existing ecosystem • isolation from raw NATS API • per-driver defaults.

Grounded in a usage scan of all serverish consumers (TOI, oca-fits-proc,
halina9000, ocabox-tcs, oca_monitor, pms, ocabox):
- `deliver_policy` **strings** + `opt_start_time` are used everywhere → hard compat surface;
- `error_behavior` / `on_missed_messages` / `raise_on_publish_error` are used **nowhere** → free to restructure;
- ofp holds 8 long-lived `get_singlepublisher` objects publishing repeatedly → confirms single-publisher misuse;
- `ensure_open` has no external users.

## 1. Policy taxonomy

All policies are **frozen dataclasses** (immutable → safe as shared defaults;
no mutable-default traps, cf. #34). They live in
`serverish/messenger/policies.py` and are exported from `serverish.messenger`.

### 1.1 DeliveryPolicy — where to start, when to stop (readers)

Variants as types — invalid combinations are unrepresentable (no more
"by_start_time requires opt_start_time" runtime checks):

```python
DeliverAll()                      # deliver_policy='all'
DeliverLast()                     # 'last'
DeliverNew()                      # 'new'
DeliverLastPerSubject()           # 'last_per_subject'
DeliverFromTime(start: datetime)  # 'by_start_time' + opt_start_time
DeliverFromSeq(seq: int)          # 'by_start_sequence' + opt_start_seq
```

Common field on the base class:

```python
until: Until = Until.FOREVER      # FOREVER — live subscription (today: nowait=False)
                                  # END_OF_DATA — stop when server confirms drained (today: nowait=True)
```

Driver-specific stop conditions (progress reader's `stop_when_done`,
document reader's `initial_wait`) stay as driver params — they are not
delivery semantics. Translation to NATS consumer config happens in ONE
place (`DeliveryPolicy.to_consumer_cfg()`), which is the isolation point.

### 1.2 ErrorPolicy — what to do when things break (readers + publishers)

```python
UNLIMITED = math.inf   # explicit "no limit"; None NEVER means infinity
                       # (deliberately NOT named FOREVER — Until.FOREVER is a
                       # different concept: "keep following live data")

@dataclass(frozen=True)
class RetryPolicy:
    attempts: int | float | None = None  # 0 = no retry; UNLIMITED = keep retrying
    delay: float | None = None           # initial inter-attempt delay [s]
    backoff: float | None = None         # delay multiplier per attempt
    max_delay: float | None = None
    total_timeout: float | None = None   # UNLIMITED = never give up on time budget

@dataclass(frozen=True)
class ErrorPolicy:
    on_error: OnError | None = None    # RAISE | FINISH | WAIT (readers) / RAISE | LOG (publishers)
    on_missed: OnMissed | None = None  # SKIP | REPLAY (readers)
    retry: RetryPolicy | None = None
```

Uniform rule (all policies, cf. §1.4): **`None` always and only means
"driver default"**. Unlimited is always the explicit `UNLIMITED`, never an
implicit meaning of None — e.g. today's WAIT-mode reader default becomes
`RetryPolicy(attempts=UNLIMITED, delay=0.2, max_delay=15, total_timeout=UNLIMITED)`,
stated in the driver, not hidden in a None.

Absorbs: `error_behavior`, `on_missed_messages` (readers),
`raise_on_publish_error`, `ack_timeout_retries` (publishers). The reader's
WAIT-mode reconnect pacing (`min(0.2 + n/5, 15)`) becomes its default
RetryPolicy; the publisher's ack retries become
`RetryPolicy(attempts=2, delay=0)`.

### 1.3 BatchPolicy — how much to prefetch (readers)

```python
@dataclass(frozen=True)
class BatchPolicy:
    max_batch: int | None = None             # pull-size cap; the ONLY size a static
                                             # policy needs, the upper bound of a dynamic one
    dynamic: bool | None = None              # None → driver default (today: False)
    initial_batch: int | None = None         # dynamic only: size of the first pull
    max_time_in_memory: float | None = None  # dynamic only: target residency of a
                                             # prefetched message [s]
```

One class, explicit `dynamic` flag — not two classes and not a `batch` vs
`initial/max` field split. Rationale: `max_batch` means the same thing in
both modes (static: the pull size; dynamic: its upper bound), so the common
user expresses only the bound (`BatchPolicy(max_batch=500)`) and stays
**neutral on mode** — when the driver default later flips to
`dynamic=True`, those callers get the improvement for free. Only someone
who explicitly writes `dynamic=False` pins the static behaviour. Two
separate classes (or an `int` sugar) would force every caller to pick a
mode at write-time and freeze the ecosystem on static. Setting
`initial_batch`/`max_time_in_memory` together with `dynamic=False` is a
validation error.

Dynamic mode: the reader measures the consumer's actual consumption rate
and sizes the next pull to `clamp(rate × max_time_in_memory, 1, max_batch)`.
Slow consumer → small pulls (bounded memory & residency); fast consumer on a
high-RTT link → pulls grow toward `max_batch` (fewer round trips — see the
production profiling: 34 k msgs over ~290 ms RTT, pull size 100→1000 =
3.3× faster). `num_pending` (server-stamped) additionally caps the request
at what actually exists.

### 1.4 Defaults strategy

Policy fields default to **None = "driver decides"**. Each driver documents
its effective defaults (e.g. `MsgJournalPublisher` → `on_error=LOG`;
`MsgPublisher` → `on_error=RAISE`; `MsgReader` → `until=FOREVER`,
fixed batch 100). This keeps one policy type usable across drivers with
different sensibilities, exactly as today's per-class param defaults.

## 2. Backward compatibility & kwarg model

**One kwarg per policy group, dual-typed, with scalar sugar that is
permanent — only the satellite kwargs get deprecated.** Nothing ships
pre-deprecated: the simple spellings are first-class API forever.

| kwarg | permanent forms | absorbs (satellites, deprecated in Phase 3) |
|---|---|---|
| `deliver_policy` | `str` sugar (`'last'`, `'all'`, ...) or `DeliveryPolicy` object | `opt_start_time`, `nowait`, `opt_start_seq` (via consumer_cfg) |
| `batch_policy` | `BatchPolicy` object only — deliberately **no `int` sugar**: a scalar would mean "static forever" scattered across projects, blocking the planned flip of the driver default to `dynamic=True` | — (new; the pull size was never public API) |
| `error_policy` | `ErrorPolicy` object (no meaningful scalar) | `error_behavior`, `on_missed_messages`, `raise_on_publish_error`, `ack_timeout_retries` |

- Scalar sugar maps 1:1 to a policy object (`'last'` ≡ `DeliverLast()`,
  `batch=500` ≡ `BatchPolicy(batch=500)`) — one normalization helper
  (`resolve_policies(**kwargs)`), used by all factories and drivers.
- Passing both a policy object and one of its satellite kwargs is an error.
- Satellite kwargs stay **silent** in Phase 1 (they are all over the
  ecosystem); Phase 3 adds DeprecationWarning; removal not before 3.0.
- `consumer_cfg` passthrough stays as the documented, explicit escape hatch
  to raw NATS — isolation with a pressure valve.
- Note: a flat `batch=int` reader param was prototyped on
  `fix/fetch-available-eager-finish` (it produced the WAN profiling numbers
  above) and then withdrawn before merge — batch control ships only as
  `BatchPolicy`, in this design's PR.

## 3. API cleanups riding along

1. **Single publisher/reader off the public API**: the only legitimate use
   is one-shot convenience — which `single_publish()` / `single_read()`
   already provide. `get_singlepublisher` / `get_singlereader`
   (static + module) get a DeprecationWarning pointing to `single_publish`
   / `single_read` (one-shot) or `get_publisher` / `get_reader` (repeated).
   Classes become internal. Migration note for ofp: its 8 long-lived
   single-publishers should be plain `get_publisher` (publish works the
   same, without per-publish open/close churn).
2. **Publish on a closed publisher**: one-time-per-instance
   DeprecationWarning ("open the publisher or use `async with`"). Keeps
   working; paves the way for `open()` gaining real semantics (e.g. early
   stream-existence validation) without surprising anyone later.
3. **`ensure_open` docs**: explicitly document the restore-previous-state
   semantics ("if the driver was closed, it is closed again after the
   call") — the current name suggests it leaves the driver open. Internal
   audit: used only by driver implementations (incl. KV one-shots); no
   external users. Keep, document, and prefer explicit
   `async with` in examples so the temporary-open pattern doesn't spread.

## 4. Rollout

- **Phase 1**: `policies.py` (Delivery/Error/Retry/Batch), normalization
  layer, dual-typed `deliver_policy`, driver defaults as policies,
  deprecations from §3. All existing tests must pass unchanged (that is the
  compat proof), plus new policy tests.
- **Phase 2**: adaptive BatchPolicy (consumption-rate estimator in
  `fetch_available`, uses `num_pending`).
- **Phase 3** (separate, later): DeprecationWarning on legacy flat kwargs;
  removal not before a major version bump.

Out of scope here: loop-lag detector (#36), KV document reader.
