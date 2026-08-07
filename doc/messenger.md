# Messenger — user guide

NATS/JetStream messaging for serverish services: publishers, readers,
KV buckets, RPC. All examples assume `serverish[messenger]` installed and
a JetStream stream covering your subjects (streams are configured on the
server, not created by the library).

## Connecting

```python
from serverish.messenger import Messenger

msg = Messenger(name='my-service')          # name becomes meta.sender
async with msg.context(host='nats.example', port=4222):
    ...  # publish / read here
```

Long-running services usually call `await msg.open(...)` at startup and
`await msg.close()` at shutdown instead of the context manager.
`Messenger.close()` also closes any drivers left open (and warns about them).

## Publishing

### Repeated publishing

The usual pattern: the publisher is a member of your service/component,
opened at startup and closed at shutdown.

```python
from serverish.messenger import get_publisher

class MyService:
    def __init__(self):
        self.status_pub = get_publisher('svc.status.my-service')

    async def open(self):
        await self.status_pub.open()

    async def on_job_update(self, jobs: int):
        await self.status_pub.publish(data={'state': 'running', 'jobs': jobs})

    async def close(self):
        await self.status_pub.close()
```

For a locally scoped publisher there is a context-manager shortcut:

```python
async with get_publisher('svc.status.my-service') as pub:
    await pub.publish(data={'state': 'done'})
```

Either way, open publishers explicitly — publishing on a never-opened
publisher works but emits a deprecation warning and will become an error in 3.0.

### One-shot

```python
from serverish.messenger import single_publish

await single_publish('svc.status.my-service', data={'state': 'done'})
```

### Errors and retries

By default a failed publish raises. Missing JetStream acks are retried
transparently (safe: every publish carries a globally unique `Nats-Msg-Id`,
so the server deduplicates); when retries are exhausted you get
`MessengerPublishAckTimeout` — note the message *may* have been delivered.

```python
from serverish.messenger import ErrorPolicy, OnError, RetryPolicy, get_publisher

# non-critical telemetry: log errors instead of raising, retry acks harder
pub = get_publisher('telemetry.conditions.mysensor',
                    error_policy=ErrorPolicy(on_error=OnError.LOG,
                                             retry=RetryPolicy(attempts=5, delay=0.1)))
```

### Journal (human-readable log over NATS)

```python
from serverish.messenger import get_journalpublisher

journal = get_journalpublisher('tic.journal.mytelescope.pipeline')
await journal.info('Calibration started')     # never raises by default (on_error=LOG)
```

## Reading

Readers are async iterators yielding `(data, meta)` pairs.

### Follow a subject live

```python
from serverish.messenger import get_reader

async with get_reader('telemetry.weather.davis', deliver_policy='last') as reader:
    async for data, meta in reader:          # last known value first, then live
        print(data['measurements']['temperature'])
```

### Drain and stop (batch processing)

`DeliverAll(until=END_OF_DATA)` — read everything the server has, then stop.
End-of-data is server-confirmed (`num_pending == 0`), there is no fixed waiting.

```python
from serverish.messenger import DeliverAll, Until, get_reader

reader = get_reader('tic.status.zb08.fits.pipeline.raw',
                    deliver_policy=DeliverAll(until=Until.END_OF_DATA))
async for data, meta in reader:
    process(data)
# loop simply ends when the subject is drained
```

The legacy spelling `get_reader(subject, deliver_policy='all', nowait=True)`
does the same and stays supported.

### Time window (e.g. last 48 h of telemetry)

```python
from datetime import datetime, timedelta, timezone
from serverish.messenger import DeliverFromTime, Until, get_reader

start = datetime.now(timezone.utc) - timedelta(hours=48)
reader = get_reader('telemetry.power.>',
                    deliver_policy=DeliverFromTime(start, until=Until.END_OF_DATA))
async for data, meta in reader:
    ...
```

### Snapshot of many subjects (dashboard pattern)

One message per subject — the latest — then stop:

```python
from serverish.messenger import DeliverLastPerSubject, Until, get_reader

reader = get_reader('svc.status.>',
                    deliver_policy=DeliverLastPerSubject(until=Until.END_OF_DATA))
snapshot = {meta['nats']['subject']: data async for data, meta in reader}
```

### Callback instead of iteration

```python
from serverish.messenger import get_callbacksubscriber

def on_status(data, meta):
    update_ui(data)          # return False to stop the subscription

sub = get_callbacksubscriber('tic.status.zb08.dome.shutterstatus', deliver_policy='last')
await sub.open()
await sub.subscribe(on_status)
...
await sub.close()
```

### One-shot read

```python
from serverish.messenger import single_read

data, meta = await single_read('tic.config.observatory')
```

## Policies

Grouped, typed configuration. Strings like `'last'` are permanent sugar for
the simple cases; policy objects unlock the rest. `None` fields always mean
"driver default"; "no limit" is the explicit `UNLIMITED`.

| kwarg | accepts | controls |
|---|---|---|
| `deliver_policy` | `'all'/'last'/'new'/'last_per_subject'` or `DeliverAll/Last/New/LastPerSubject/FromTime(start)/FromSeq(seq)` (+ `until`) | where reading starts, when it stops |
| `error_policy` | `ErrorPolicy(on_error, on_missed, retry=RetryPolicy(...))` | reaction to failures; retry pacing (reader reconnects, publisher acks) |
| `batch_policy` | `BatchPolicy(max_batch, dynamic, initial_batch, max_time_in_memory)` | reader prefetch sizing |

Combining a policy object with its legacy satellite kwarg (e.g.
`DeliverLast()` + `nowait=True`) raises `PolicyConflict` — express everything
in the object.

### Batching: memory vs round trips

Default is **dynamic**: pulls start tiny and follow your actual consumption
rate so prefetched messages wait at most ~5 s in memory, never more than
10 000 per pull. Slow consumers get small pulls automatically; fast bulk
readers over high-latency links ramp up (each pull costs a network round trip).

```python
from serverish.messenger import BatchPolicy, get_reader

# constrained device: keep at most 50 messages in memory, ~1 s residency
reader = get_reader(subject, batch_policy=BatchPolicy(max_batch=50, max_time_in_memory=1.0))

# pin the pre-2.3 static behaviour
reader = get_reader(subject, batch_policy=BatchPolicy(dynamic=False, max_batch=100))
```

## KV buckets

Key-value store over JetStream. Values are regular serverish envelopes.

```python
from serverish.messenger import get_kvstore, kv_get, kv_put

async with get_kvstore('observatory-state', create_bucket=True) as store:
    await store.put('dome.position', {'az': 123.5})
    data, meta = await store.get('dome.position')
    keys = await store.keys()

data, meta = await kv_get('observatory-state', 'dome.position')   # one-shot
```

Watch keys for changes (iterator or callback):

```python
from serverish.messenger import get_kvreader

async with get_kvreader('observatory-state', key='dome.>') as watcher:
    async for data, meta in watcher:       # current values first, then changes
        if data is None:                   # delete/purge marker
            print('deleted:', meta['kv']['key'])
```

## RPC

Request/reply over core NATS (not JetStream — use a non-stream subject):

```python
from serverish.messenger import get_rpcresponder, request

def handler(rpc):
    rpc.set_response(data={'sum': rpc.data['a'] + rpc.data['b']})

responder = get_rpcresponder('svc.rpc.my-service.add')
await responder.open()
await responder.register_function(handler)

data, meta = await request('svc.rpc.my-service.add', data={'a': 2, 'b': 3})
```

## Messages and errors

Every message is an envelope: your `data` plus auto-filled `meta`
(`id`, `sender`, `ts` — 7-element UTC list, `message_type`, `tags`; after
delivery also `meta['nats']` with `seq`, `subject`, `num_pending`, ...).

All library exceptions derive from `ServerishError`:

```python
from serverish.base import ServerishError, MessengerReaderStopped, MessengerPublishAckTimeout

try:
    await pub.publish(data=payload)
except MessengerPublishAckTimeout:
    ...   # no ack - message MAY have been delivered (dedup makes re-publish safe)
except ServerishError:
    ...   # anything else from serverish
```

## Lifecycle summary

- Prefer `async with` for every driver; or `open()`/`close()` explicitly.
- A reader left open keeps a server-side consumer alive — close it.
- `Messenger.close()` closes forgotten drivers and disconnects.
- One-shot helpers (`single_publish`, `single_read`, `kv_get`, `kv_put`)
  manage open/close for you.
