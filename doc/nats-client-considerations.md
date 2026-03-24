
# NATS JetStream Client Under Adverse Conditions (Python asyncio)

Problems, Failure Modes, and Design Strategies

⸻

1. Context

System model
	•	Python client (asyncio, nats.py)
	•	JetStream (pull consumers)
	•	User code runs in same process, often:
	•	blocking I/O
	•	CPU-bound
	•	sometimes pathological (UI, sync libs)

Core constraint (CPython)
	•	GIL: tylko jeden wątek wykonuje bytecode naraz  ￼
	•	przełączanie między wątkami następuje okresowo między instrukcjami  ￼
	•	GIL:
	•	chroni runtime
	•	nie daje thread-safety logiki aplikacji

⸻

2. Fundamental mismatch

Asyncio assumptions
	•	cooperative scheduling
	•	tasks must await to yield

Real world
	•	user code:
	•	nie yielduje
	•	blokuje event loop
	•	nie kontrolujesz tego

➡️ Wniosek:

Nie możesz polegać na user-loop jako części systemu transportowego

⸻

3. Failure Modes (realne, produkcyjne)

3.1 Event loop starvation

Cause:
	•	sync I/O
	•	CPU loop
	•	long callback

Effect:
	•	brak read z socketu
	•	brak ACK flush
	•	brak PONG handling

Outcome:
	•	disconnect (client stale)
	•	albo server disconnect (slow consumer)

⸻

3.2 ACK starvation

Cause:
	•	ACK wykonywany w user-loop

Effect:
	•	ACK opóźniony lub brak

Outcome:
	•	redelivery
	•	duplikaty
	•	max_ack_pending exhaustion

⸻

3.3 Pull lifecycle break

Pull request:
	•	jest ephemeral
	•	powiązany z inbox

Disconnect →
	•	pull traci sens
	•	inbox przestaje być aktywny

➡️ wymagany restart pull-loop

⸻

3.4 Queue coupling bug

Jeśli:
	•	queue w user-loop
	•	enqueue zależy od user-loop

To:
	•	freeze loop = brak enqueue
	•	brak ACK (w trybie enqueue-ack)

➡️ fałszywe bezpieczeństwo

⸻

3.5 Thread-safety illusion
	•	asyncio.Queue nie jest thread-safe  ￼
	•	GIL ≠ thread safety
	•	multi-step operations mogą się przeplatać  ￼

⸻

4. Design goals

System ma być:
	1.	Transport-safe
	•	NATS connection nie zależy od user-loop
	2.	Backpressure-aware
	•	brak niekontrolowanego wzrostu pamięci
	3.	Semantically explicit
	•	ACK policy jawna
	4.	Failure-resilient
	•	reconnect + restart pull

⸻

5. Proven Architecture

5.1 Separation of concerns

NATS loop (thread A)
	•	socket I/O
	•	parsing
	•	pull
	•	enqueue
	•	ACK (opcjonalnie)

User loop (thread B)
	•	processing
	•	iteracja async for

⸻

5.2 Core invariant

User code nigdy nie blokuje NATS loop

⸻

6. Queue Architecture

6.1 Queue MUST live in NATS loop

Dlaczego:
	•	enqueue + ACK muszą być atomowe względem transportu
	•	user-loop nie jest deterministyczny

⸻

6.2 Cross-thread consumption pattern

User loop:

async def __anext__(self):
    cfut = asyncio.run_coroutine_threadsafe(
        self._q.get(),
        self._nats_loop
    )
    return await asyncio.wrap_future(cfut)

Properties:
	•	brak bezpośredniego dostępu do queue
	•	thread-safe
	•	brak locków

⸻

6.3 Backpressure strategies

Strategy A: Drop (UI)
	•	bounded queue
	•	overwrite / drop

Strategy B: Pause pull
	•	nie wysyłasz .NEXT
	•	JetStream buforuje

Strategy C: Spill
	•	durable buffer

⸻

7. ACK strategies (critical)

7.1 ACK-after-enqueue

Mechanism:
	•	enqueue in NATS loop
	•	immediate ACK

Pros:
	•	stabilność
	•	brak redelivery storm

Cons:
	•	possible data loss (unless durable queue)

⸻

7.2 ACK-after-process

Mechanism:
	•	ACK triggered by user

Pros:
	•	at-least-once

Cons:
	•	zależność od user-loop
	•	redelivery on freeze

⸻

7.3 Hybrid (recommended)

Use case	Strategy
UI / telemetry	enqueue-ack
jobs / tasks	process-ack


⸻

8. Consumer scaling strategy

Problem:
	•	user tworzy setki subskrypcji

Solution: Fan-in

Single consumer:
	•	FilterSubjects lub wildcard
	•	routing lokalny

Benefits:
	•	mniej consumerów
	•	mniejsze obciążenie serwera

Trade-off:
	•	większe max_ack_pending
	•	lokalny routing complexity

⸻

9. Reconnect strategy

On reconnect:
	1.	invalidate:
	•	inbox
	•	pull state
	2.	restart:
	•	pull loop
	•	iterator bridge
	3.	ensure consumer (idempotent)

⸻

10. What threads actually solve (precisely)

Works:

Scenario	Thread helps
Blocking I/O	YES (GIL released)
sleep	YES
short CPU bursts	PARTIAL

Does NOT work:

Scenario	Thread helps
long C-extension holding GIL	NO
process freeze	NO


⸻

11. Key insight (important)

Threading does NOT fix user bugs.
It isolates transport from user bugs.

⸻

12. Minimal correctness rules
	1.	Single pull-loop per consumer
	2.	Queue in NATS loop
	3.	No direct cross-thread queue access
	4.	Explicit ACK policy
	5.	Restart on reconnect
	6.	Dedup (stream sequence)

⸻

13. Optional improvements (advanced)
	•	batching delivery (reduce cross-thread overhead)
	•	coalescing (latest-value semantics)
	•	adaptive batch size
	•	latency watchdog (detect loop freeze)

⸻

Final takeaway

Jeśli chcesz stabilności:
	•	oddziel transport od usera
	•	ACK rób tam, gdzie masz kontrolę
	•	queue trzymaj tam, gdzie masz deterministykę

Reszta to tylko tuning.

⸻

Jeśli chcesz, mogę przerobić to na:
	•	formalny RFC (sekcje, diagramy, state machine)
	•	albo konkretny skeleton kodu dla serverish2 (gotowy do wklejenia)