# Central Notification Service — System Design v1.4

**Status:** Authoritative. This document supersedes `CNS-System-Design.docx` (v1.3), which is kept for history.
**Date:** July 2026
**Diagrams:** sources in [`../diagrams/`](../diagrams/) (Mermaid `.mmd` + rendered `.png`)

---

## 1. What changed in v1.4 (vs. v1.3)

v1.4 has one theme: **the simplest architecture that still meets every requirement**, plus two functional changes — AppSync replaces the entire custom in-app stack, and AI enrichment becomes a **gate**: an AI-enriched notification *waits in the AI pipeline and is never delivered to the client before enrichment completes*.

| v1.3 component | v1.4 — where it went |
|---|---|
| Router λ (+ route-q) | **Deleted.** Its irreducible jobs (RBAC recipient resolution, resolved/episode guard) moved into the single **Intake λ**; the routing half of its job is now **SNS message-attribute filtering** — `NotificationType.channels[]` becomes message attributes, channel queues subscribe with filter policies. Routing is configuration, not code. |
| Renderer λ | **Deleted.** Each channel λ renders its own channel's template with a shared template library. Rendering is channel-specific anyway (email body ≠ inbox card ≠ Slack blocks). |
| Inbox REST API + API Gateway WebSocket + connection-registry table | **Replaced by one AppSync GraphQL API** — queries, mutations, and real-time subscriptions in a single managed surface. AppSync manages every connection; the registry table, sync-on-connect plumbing, and the reconnect-storm risk (v1.3 risk R8) are gone. |
| Progressive-enhancement enrichment (baseline v1 delivered ≤1.5 s, AI upgrades to v2 in place; email holds 4 s; 60 s upgrade window; max-age discard) | **Replaced by the AI gate.** AI-type messages wait in `enrich-q`; **no channel ever sees them before enrichment succeeds**. The `contentVersion` v1/v2 machinery, the email-hold window, and the upgrade-window valve are all deleted — there is exactly one version of the content, identical on every channel. |
| Two full alternative architectures (A: serverless mesh, B: EKS + Kafka), iteration logs, pillar scoring | The doc now describes **one solution**. Solution B survives as a one-page scale-up appendix ([Appendix A](#appendix-a--scale-up-evolution-kafka--eks)) with explicit revisit triggers. |

What did **not** change: the canonical envelope, producer API, idempotency, `tenant#user` key design + IAM-level isolation, TTL / read-to-dismiss / group resolution, the unread counter, the audit contract, PII handling, and the DR posture. They are summarized here and remain as specified in v1.3 where noted.

---

## 2. Executive summary

**Goal.** One Central Notification Service (CNS): any backend hands it a notification — explicitly via API or implicitly via the company event bus — and CNS resolves recipients, delivers through the configured channels (in-app Bell inbox, email, Slack; more addable later), keeps a full audit trail, and optionally enriches the message with AI. Hard multi-tenant isolation, zero silently dropped events, and per-type lifecycle (TTL, read-to-dismiss, group resolution) are first-class requirements.

**Shape.** The entire pipeline is **one intake Lambda, one SNS topic, and one Lambda per channel**:

```
Producers (API GW HTTP | EventBridge rules)
   → Intake λ   validate · idempotency · payload→payloadRef · RBAC resolve · resolved-check
   → SNS topic  message attributes: channels[] (from NotificationType config) + stage
        stage=enrich   → enrich-q (SQS) ⟵ AI messages WAIT here — no channel sees them
                          → Enricher λ → republish stage=deliver (+ enriched fields)
        stage=deliver  ├→ inbox-q → Inbox λ  (render + DynamoDB write + AppSync publish)
                       ├→ email-q → Email λ  (render + SES)
                       └→ slack-q → Slack λ  (render + Slack API)
```

**NFR anchors.** Sustained 10,000 notifications/min (~167/s), bursts to 50,000/min (~833/s); no silent drops (DLQ per queue, depth-0 steady state, alarms otherwise); hard tenant isolation; producer `eventId` idempotency; PII never in logs.

**SLOs (v1.4 split).**

| Type | In-app producer→visible | Email handoff |
|---|---|---|
| Non-AI types (~95 % of volume) | p99 ≤ 1.5 s | p99 ≤ 5 s |
| AI-enriched types (opt-in per type) | p99 ≤ 10 s — **explicit, documented trade-off**: enrichment is on the delivery path by requirement | p99 ≤ 10 s |

---

## 3. Component walkthrough

![Component overview](../diagrams/v14_components.png)

### 3.1 Intake λ — the only place events enter

One Lambda, two triggers:

- **Explicit:** API Gateway HTTP `POST /v1/notifications` (service auth: SigV4/OAuth2; `tenantId` extracted server-side from the caller's claim — never trusted from the body).
- **Implicit:** EventBridge rules generated from the mapping table (`notify` and `resolve` mappings — unchanged from v1.3 §9.2, including shadow-mode deploys).

Per event it: (1) validates against the per-type JSON Schema; (2) performs idempotency with a conditional put of `tenant#eventId` (TTL 48 h) — duplicates return `202 {duplicate:true}`, so producers retry blindly and safely; (3) stores the payload once, encrypted under the tenant-tier CMK, so messages carry an opaque `payloadRef`, never PII; (4) resolves recipients — `user` passes through, `role@org` goes to the cached RBAC client (fresh ≤ 60 s, stale-while-revalidate ≤ 10 min); (5) runs the **resolved-check / episode guard** for resolvable types (trigger `eventTime ≤ resolvedAt` ⇒ suppressed as duplicate, audited); (6) **publishes to SNS and only then ACKs** — durable-publish-before-ack is what makes "no silent drops" mechanical.

If RBAC is down beyond the staleness bound: the API path returns 5xx (the producer's blind retry is safe via idempotency); the bus path relies on Lambda retry → DLQ + alarm. CNS never guesses membership.

Fan-out is chunked: recipients ride the message in chunks of ≤ 500, so a 5,000-recipient event is 10 messages; above the 5,000 cap the event is split into paced waves (everyone is still notified; an over-cap alarm tells product which types outgrew their audience).

### 3.2 SNS topic — routing as configuration

Each published message carries attributes taken directly from config: `channels` (from `NotificationType.channels[]`, e.g. `["inApp","email"]`), `stage` (`enrich` for AI types, `deliver` otherwise), `typeId`, `tenantId`.

Subscriptions are SQS queues with **filter policies**:

| Queue | Filter policy |
|---|---|
| `enrich-q` | `stage = enrich` |
| `inbox-q` | `stage = deliver AND channels contains inApp` |
| `email-q` | `stage = deliver AND channels contains email` |
| `slack-q` | `stage = deliver AND channels contains slack` |

Consequences: adding a channel is a new queue + λ + filter policy — zero pipeline change; a Slack outage cannot touch email or in-app latency (per-queue isolation, retry policy, and DLQ); and **AI-type messages are structurally invisible to channel queues** until the Enricher republishes them with `stage=deliver` — the "wait" is enforced by topology, not by discipline.

### 3.3 Channel λs — render + deliver, one per channel

Each channel λ renders its own template (shared library; Mustache-strict — an unknown variable is a render error → DLQ, never a blank message) and delivers:

- **Inbox λ:** `TransactWriteItems` — conditional `PutItem` on `notifId` (idempotent) + `ADD unread_ctr +1` — then publishes the item to **AppSync** (IAM-authorized mutation), which fans out to the user's live subscriptions. 5,000-recipient worst case ≈ 200 batch calls ≈ under 2 s.
- **Email λ:** outbox `DeliveryRecord` conditionally created before send (`sending → sent`), SES v2 with `X-CNS-Notif-Id`; SES event publishing (send/delivery/bounce/complaint/reject/delay) streams into the audit trail; bounce > 2 % / complaint > 0.1 % alarms; suppression list auto-grows.
- **Slack λ:** per-tenant bot token (Secrets Manager), token bucket per workspace/channel (~1 msg/s guidance), HTTP 429 honored via `Retry-After` delayed requeue.

Every consumer conditions on the deterministic `notifId = hash(eventId, recipient, channel)` — redrives and at-least-once delivery are safe everywhere.

### 3.4 The in-app surface — one AppSync GraphQL API

A single AppSync API replaces the v1.3 trio (Inbox REST API, WebSocket API, connection registry):

```graphql
type Query {
  inbox(cursor: String, limit: Int = 25): InboxPage!   # visibility-filtered, newest first
  unreadCount: Int!
}

type Mutation {
  markRead(notifId: ID!): InboxItem!        # applies dismissOnRead
  dismiss(notifId: ID!): InboxItem!
  resolve(notifId: ID!): ResolveResult!     # backup path: sweeps the WHOLE group
  readAll: Int!
  # service-side, IAM-authorized — invoked by the Inbox λ, triggers subscriptions:
  publishInboxEvent(userId: ID!, event: InboxEventInput!): InboxEvent! @aws_iam
}

type Subscription {
  onInboxEvent(userId: ID!): InboxEvent
    @aws_subscribe(mutations: ["publishInboxEvent"])
}

type InboxEvent { kind: InboxEventKind!, item: InboxItem, notifId: ID, unread: Int }
enum InboxEventKind { UPSERT REMOVE BADGE }   # REMOVE reason: resolved | expired
```

- **Reads and mutations use direct JS resolvers to DynamoDB** — no Lambda on the user read path at all. The resolver builds `PK = tenant#user` from the caller's JWT claims (platform OIDC), applies the visibility filter (`expiresAt > now`, not resolved, `state ∈ {unread, read}`), and the conditional state transitions + counter decrement exactly as in v1.3 flow 4.
- **Isolation:** `tenantId`/`userId` come only from validated JWT claims; resolvers construct keys server-side, and subscription auth rejects `onInboxEvent(userId ≠ $ctx.identity)` — a user can only ever subscribe to their own channel. Same guarantee level as the v1.3 `dynamodb:LeadingKeys` posture: enforced below application code.
- **Connections are AWS's problem now.** AppSync owns connection lifecycle, fan-out, and reconnects. Correctness still never depends on the socket: the client runs the `inbox` query on connect/Bell-open; the subscription is a latency optimization.

### 3.5 Config & admin, audit, observability (unchanged mechanics, fewer places)

- **Config store:** `NotificationType`, templates (versioned, draft→shadow→active), bus mappings (notify/resolve, shadow-mode first), org overrides, `TenantAiBudget`. Admin API is internal OIDC, every write versioned + cache-busting, exactly as v1.3 §9.4.
- **Audit:** dual-plane, unchanged from v1.3 §11 — authoritative state written with the side-effect it describes; analytical trail (idempotent `auditId = notifId#stage#attempt`) via Firehose → S3 Parquet + Athena, 7-day hot index; daily reconciliation with drift alarm; PII never in the trail. New v1.4 audit events for the AI gate: `enrich_waiting`, `enrich_retry{reason,attempt}`, `enrich_dlq{reason}`, `redriven`.
- **Observability:** golden dashboard — ingest rate, per-queue oldest-message-age, per-channel success and delivery p99, **all DLQ depths (steady state 0)**, enrichment retry/DLQ rate and budget burn, AppSync connection/publish errors, SES bounce/complaint, top-10 tenants by volume.

---

## 4. AI enrichment — the gate

> **Requirement (v1.4):** an AI-enriched notification **waits in the AI pipeline** and is **not sent to the client** until enrichment completes. There is no baseline-first delivery.

### 4.1 Flow

For types with `aiEnrichment.enabled`, the Intake λ publishes with `stage=enrich`. Only `enrich-q` subscribes to that stage — the message sits there, invisible to every channel, until the Enricher λ succeeds and republishes it with `stage=deliver` plus the enriched fields. Channel λs then render and deliver exactly one version of the content — the enriched one — identically on every channel (no v1/v2, no correction emails, no upgrade window).

Enricher gate order per message:

1. **Signature cache** — `hash(typeId + allow-listed key fields, numerics bucketed)` → cached enriched text at zero model cost (event storms collapse to one call).
2. **Budget gate** — atomic `TenantAiBudget` counter checked **before** every invocation.
3. **Context fetch** — the payload by `payloadRef` plus ≤ 2 allow-listed context views, ≤ 150 ms combined, circuit-broken.
4. **PII masking** — prompt built only from allow-listed `promptFields`; names/emails/free text masked to stable placeholders, re-substituted after generation.
5. **Bedrock** — small model (Haiku-class), temperature 0, no data retention, region-pinned, per-attempt timeout.
6. **Validation** — JSON-schema output + groundedness: every number/date/entity must exist in payload ∪ fetched context.
7. **Republish** `stage=deliver` — the message finally becomes visible to channel queues.

### 4.2 Failure handling — classify, back off, DLQ + alert

Failures are **classified first**, because the right response differs by cause:

| Failure class | Response | Burns budget? |
|---|---|---|
| `budget_denied` (tenant/type cap hit) | **No retry** — parks with an admin alert; delivery resumes when budget/config changes (or an operator decides) | no |
| `throttled` (Bedrock 429) | Backoff retry — a throttle is load, not an error | no |
| `timeout` / model 5xx | Backoff retry | attempt only |
| `validation_failed` (schema/groundedness) | One retry, then DLQ — at temperature 0, identical input rarely fixes itself | yes |

**Retry mechanics:** SQS visibility-timeout exponential backoff with jitter (≈ 30 s → 2 m → 8 m → …), `maxReceiveCount = 5`. This protects both the budget (bounded attempts) and Bedrock (spaced attempts).

**Stuck ⇒ alert, always:** after max attempts the message lands in **`enrich-DLQ`, which alarms at depth ≥ 1** — an undelivered AI notification is an operational event, never a silent one. The audit trail carries `enrich_waiting` → `enrich_retry{reason,attempt}` → `enrich_dlq{reason}` so *why* it failed is one query. **One-click redrive** (safe — every consumer is idempotent on `notifId`) is what finally delivers the message once the cause is fixed; the redrive is audited as `redriven`.

### 4.3 How Bedrock is protected from clogging

Six independent brakes, ordered from structural to tactical:

1. **Reserved concurrency cap on the Enricher λ** (e.g. 5–20). This is the hard ceiling: no matter how deep `enrich-q` gets, at most N model calls run concurrently. The queue absorbs the burst; Bedrock sees a fixed maximum rate. Queue depth can never translate into call pressure.
2. **Signature cache in front of the model.** An event storm is by definition thousands of near-identical events — one signature, one model call, N cache hits. The pathological case is structurally unable to storm the model.
3. **Pre-invoke budget gate.** `TenantAiBudget` is checked before the call, so an over-budget tenant generates zero Bedrock traffic — cost incidents are impossible, not just unlikely.
4. **Client-side token bucket matched to the account's Bedrock RPM/TPM quota**, plus `ThrottlingException` honored with backoff (and it doesn't count as a failure attempt). CNS never pushes past the provisioned quota.
5. **Circuit breaker.** Sustained error/latency threshold opens it → the Enricher stops calling entirely; messages simply keep waiting in `enrich-q` under backoff; a half-open probe restores. A model outage produces zero call storm.
6. **Small model + max-token caps** bound the cost and latency of each individual call; move to Provisioned Throughput when steady volume justifies it.

### 4.4 Config knobs (per type)

```jsonc
"aiEnrichment": {
  "enabled": true,
  "promptFields": ["gapUsd", "dueDate"],
  "contextFetch": [{ "view": "wallet_context", "key": "$.walletId",
                     "fields": ["balance", "currency", "lastTopUps"] }],
  "timeoutMs": 5000,          // per attempt
  "maxAttempts": 5,           // then enrich-DLQ + alarm
  "retryBackoff": "exponential-jitter",   // ~30s → 2m → 8m
  "perTypeTokenCap": 2000000
}
```

(Removed from v1.3: `mode: progressive`, `upgradeWindowMs` — there is no upgrade path anymore.)

### 4.5 AI-assisted event triage

Unchanged from v1.3 §10.6 (T1 design-time author + T2 signature-gated runtime recommended; AI authors *rules*, never messages; shadow mode, vocabulary validation, platform `AiTriageBudget`, kill switch). It inherits the same masking pipeline and the same anti-clog pattern (shape cache = signature cache; budget before invoke; the triage model runs at unique-shape rate, ~20–200 calls/day against 5–14 M events/day).

---

## 5. Data model (delta from v1.3 §8)

| Entity | v1.4 status |
|---|---|
| `NotificationType`, `Template`, `BusMapping`, `OrgOverride`, `TenantAiBudget`, `BudgetUsage` | unchanged (minus `upgradeWindowMs`, plus `maxAttempts`/`retryBackoff` in `aiEnrichment`) |
| Envelope / DeliveryTask (transit) | unchanged, but ride **SNS→SQS** instead of route-q/channel queues; carry `stage` attribute |
| `InboxItem` | unchanged keys (`PK tenant#user`, `SK createdAt#notifId`, GSI on `groupKey`, `expiresAt` TTL) — **`contentVersion`/`enrichedAt` removed**: one version only |
| `ResolutionGroup`, `DeliveryRecord` (outbox), `IdempotencyKey`, `AuditEvent` | unchanged |
| `Connection` (WebSocket registry) | **deleted — AppSync manages connections** |

PII posture unchanged (v1.3 §8.1): payload-by-reference everywhere, tenant-tier CMKs, masked-placeholder-only enrichment cache, HMAC-tokenized recipients in audit, logger schema that cannot carry payloads.

---

## 6. Public surfaces

### 6.1 Producer API — unchanged

```
POST /v1/notifications             idempotent via eventId; 202 {notifRequestId, duplicate:false}
POST /v1/notifications/resolve     202; one call clears the whole group
GET  /v1/notifications/{id}/status per-recipient/channel delivery states
```

`tenantId` is extracted server-side from the JWT/claims — the body field is never the authority. Event-bus notify/resolve mappings (config, not code) are unchanged, including shadow-mode deploys and the episode guard.

### 6.2 In-app — the AppSync API (replaces REST + WS)

Schema in §3.4. Client contract: query `inbox`/`unreadCount` on open, subscribe `onInboxEvent(me)` for live `UPSERT | REMOVE | BADGE` events, call `markRead`/`dismiss`/`resolve` mutations. Lifecycle semantics (TTL never visible, `dismissOnRead`, group resolve sweep, unread counter with conditional decrements + L1 read-time self-heal + weekly L2 sweep) are exactly v1.3 §5.4 — only the transport changed.

### 6.3 Admin API — unchanged

CRUD types/templates/mappings/org-overrides/ai-budgets, template promote (draft→shadow→active), one-click DLQ redrive, 7-day delivery lookup.

---

## 7. Failure modes (updated for v1.4)

| Failure | First signal | User sees | Automatic behavior |
|---|---|---|---|
| Bedrock slow / down | Enricher breaker open; enrich-q oldest-age climbs | AI-type notifications **delayed** (they wait — by requirement); non-AI types unaffected | Backoff retries; breaker stops calls; after `maxAttempts` → enrich-DLQ + **alarm**; redrive delivers |
| Tenant AI budget exhausted | `budget_denied` metric + admin alert | That tenant's AI-type notifications wait | No model calls; parks until budget/config changes or operator redrives |
| RBAC down / slow | breaker metric | nothing ≤ 10 min (stale cache) | API path: 5xx → producer retries (idempotent); bus path: Lambda retry → DLQ + alarm |
| Slack 429 storm | token-bucket saturation | delayed Slack posts | `Retry-After` delayed requeue; other workspaces isolated |
| SES bounce spike | bounce > 2 % alarm | nothing | suppression list auto-grows; per-type auto-pause |
| DDB hot partition | throttle metric | slightly delayed inbox writes | key design spreads load; jittered batch backoff |
| AppSync publish failure | publish-error metric | badge staleness only | client refetches on connect/open — store is the source of truth |
| Poison message | any DLQ > 0 | one notification delayed | maxReceive → DLQ, alarm at 1, idempotent redrive |
| EventBridge mapping drift | shadow diff ≠ expected; hourly canary silent | possibly missing notifications | strict templates render-error → DLQ; canary alarms; archive-replay the gap |

Retry policy per boundary: full-jitter exponential backoff everywhere; DLQ after 5–6 receives; config/template errors are non-retryable → DLQ immediately with reason. (Enricher specifics in §4.2.)

---

## 8. Multi-tenancy, security, DR — kept, summarized

- **Standard tier:** pooled, every key tenant-prefixed, isolation enforced below app code (DynamoDB `LeadingKeys`-conditioned roles; AppSync resolvers build keys from JWT claims only). **Enterprise tier:** same IaC stack stamped as a dedicated cell.
- **PII:** payload-by-reference; only Intake and channel λs ever hold plaintext; Bedrock no-retention + allow-listed masked prompts; logger schema without payload fields + CloudWatch data-protection backstop.
- **DR:** Multi-AZ single region (all components ≥ 2 AZ by construction); IaC redeployable cross-region; config/templates continuously exported; EventBridge archive enables re-ingest of an outage window. Warm-standby / active-active remain priced options with the same triggers as v1.3 §12.

---

## Appendix A — Scale-up evolution (Kafka + EKS)

Kept from v1.3 as the documented evolution path, not an alternative to choose today.

**Shape:** same logical components on a streaming substrate — MSK (3 brokers, RF=3, `acks=all`) as the backbone with topics per stage, long-running consumers on EKS (KEDA on lag, Graviton + Spot), Aurora PostgreSQL for state (day-partitioned inbox, partition-drop TTL), Redis for caches/counters, Glue Schema Registry for contract enforcement. Same envelope, same adapters, same AppSync front — the swap happens *behind* the Intake λ and channel contracts, so producers and clients never notice.

**What it buys:** native 7-day replay (reprocessing = consumer-group offset reset), contractual per-recipient ordering (partition keying), sub-linear cost at high volume.

**What it costs:** a ~$3k/mo broker+DB+cluster floor that never sleeps, ≈ 0.5–1 FTE of platform operations, months instead of weeks to production, and a cluster-per-cell enterprise story.

**Revisit triggers (any one):**
1. Sustained volume > ~60k notifications/min (≈ 4–6× current ceiling — where the cost curves cross);
2. Replay becomes a compliance/product feature;
3. Contractual per-recipient ordering.

Until a trigger fires, the serverless design in this document is the recommendation — at 167–833 req/s the workload sits squarely inside managed-serverless envelopes, and every dollar and hour saved compounds.
