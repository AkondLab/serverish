# Serverish — Subscription Reliability & Testing

## What This Is

Serverish is a Python library providing helpers for server-like projects, built on NATS/JetStream for messaging, with component lifecycle management and connection handling. This milestone focuses on finishing the subscription reliability improvements on the `fixsubscriptions` branch and building a comprehensive, automated test suite that proves these improvements work — including edge cases where master fails.

## Core Value

Reliable message delivery and subscription resilience under real-world conditions — NATS disconnects, consumer expiry, slow consumers, client-side freezes — with health monitoring that makes failures visible.

## Requirements

### Validated

- ✓ Layered library architecture (base, connection, messenger, schema) — existing
- ✓ NATS/JetStream connection lifecycle management — existing
- ✓ Publish/subscribe messaging patterns (publisher, reader, single, callback) — existing
- ✓ RPC request/reply pattern — existing
- ✓ Progress tracking publisher/reader — existing
- ✓ Journal (human-readable logging) publisher/reader — existing
- ✓ JSON Schema message validation — existing
- ✓ Component tree management via Manageable/Collector — existing
- ✓ LiveDocument real-time shared state — existing

### Active

- [ ] Health monitoring: health_status property on all drivers (publisher, reader, RPC responder, connection)
- [ ] Proactive consumer health checks (periodic verification, not just reactive)
- [ ] Reconnection handling: reader consumer recreation after NATS reconnect
- [ ] RPC responder auto-resubscribe after NATS reconnect
- [ ] Slow consumer detection and tracking at connection level
- [ ] Publisher error/success tracking with publish counts
- [ ] Increased inactive_threshold (300s) for ephemeral consumer resilience
- [ ] Automated test infrastructure: NATS server managed by pytest (no manual Docker)
- [ ] CI with JetStream: all NATS tests run in CI, not just skipped
- [ ] Regression test suite: comprehensive tests for all messenger patterns
- [ ] Edge case tests: NATS disconnect/reconnect, consumer expiry, slow consumers
- [ ] Client freeze tests: CPU-bound blocking scenarios
- [ ] Comparative tests: scenarios that fail on master but pass on fixsubscriptions
- [ ] Performance/timing tests: recovery speed benchmarks

### Out of Scope

- New messaging patterns — focus is reliability of existing patterns
- Breaking API changes — all improvements are additive
- Migration tooling — library consumers don't need to change code
- Multi-server NATS cluster testing — single server is sufficient for this milestone

## Context

- Branch `fixsubscriptions` has 2 commits ahead of master plus ~192 lines of uncommitted changes
- Uncommitted work is mostly complete: health_status on all drivers, consumer health checks, reconnection handling
- Current CI runs `nats:latest` without JetStream, so all NATS tests are skipped
- Existing tests use `@pytest.mark.skipif(ci, ...)` — this needs to change
- Docker compose for NATS is at `/Users/mka/projects/astro/oca_nats_config/docker/nats`
- Test approach: testcontainers-python for local, Docker service with JetStream for CI
- The concerns report flagged: hard `exit()` calls in reader, bare `except:` in RPC requester, access to nats-py private internals — these should be addressed as part of polish

## Constraints

- **Python**: ^3.10, async/await throughout
- **Dependencies**: Poetry-managed, nats-py ^2.7.2, param ^2.1.0
- **Testing**: pytest + pytest-asyncio, testcontainers for local NATS automation
- **CI**: GitHub Actions with NATS Docker service (JetStream enabled)
- **No relative imports**: project is installed as package
- **No datetime.utcnow**: use datetime.now(UTC)

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Fix CI Docker + testcontainers locally | User prefers Docker-based approach, familiar tooling | — Pending |
| Medium effort on code, major effort on testing | Code changes nearly done, proving reliability is the real goal | — Pending |
| Tests should demonstrate branch superiority | Not just regression — show scenarios where master fails | — Pending |
| Version bump to 1.6.0 | Already committed on branch, reflects significance of changes | ✓ Good |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd:transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd:complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-03-24 after initialization*
