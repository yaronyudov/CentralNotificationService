# CentralNotificationService

One Central Notification Service (CNS) for the whole platform: any backend hands it a notification — explicitly via API or implicitly via the company event bus — and CNS resolves recipients, delivers through the configured channels (in-app Bell inbox, email, Slack), keeps a full audit trail, and optionally enriches the message with AI. Multi-tenant by design.

## 📐 Design documentation

| Document | Status |
|---|---|
| [`docs/CNS-System-Design-v1.4.md`](docs/CNS-System-Design-v1.4.md) | **v1.4 — authoritative** |
| [`Summary.html`](Summary.html) / [`CNS-Design-Reader.jsx`](CNS-Design-Reader.jsx) | Interactive design reader (v1.4) — open `Summary.html` in a browser |
| `CNS-System-Design.docx` | v1.3 — **superseded**, kept for history (two-alternatives study, iteration logs) |
| `תקציר.pdf` | Hebrew summary of v1.3 — historical |

## v1.4 in one paragraph

The pipeline is **one intake Lambda, one SNS topic, and one Lambda per channel**. `NotificationType.channels[]` becomes SNS message attributes and each channel queue subscribes with a filter policy — routing is configuration, not code (no Router λ, no Renderer λ; each channel λ renders its own template). **Six channels ship in v1.4: in-app, email, Slack, webhook, mobile push, and SMS** — each is the same shape (queue + filter + adapter + DLQ), so the three new ones were added with zero pipeline change. The in-app surface is **one AppSync GraphQL API** (queries + mutations + real-time subscriptions; AppSync manages all connections — no WebSocket API, no connection-registry table). AI enrichment is a **gate**: AI-type messages carry `stage=enrich` and wait in the enrich queue — **no channel ever sees them before enrichment succeeds**; failures are classified, retried with backoff, and after max attempts land in an alarmed DLQ (redrive delivers). Bedrock is protected from clogging by a reserved-concurrency ceiling, a signature cache, a pre-invoke budget gate, a quota-matched token bucket, and a circuit breaker. Resolvable notifications get a **delivery-time resolved-check** so that if a group is resolved mid-fan-out, recipients whose wave hasn't been delivered yet are **never notified at all**.

```
Producers (API GW HTTP | EventBridge rules)
   → Intake λ   validate · idempotency · payload→payloadRef · RBAC resolve · resolved-check
   → SNS topic  attrs: channels[] (from type config) + stage
        stage=enrich   → enrich-q ⟵ AI messages WAIT here → Enricher λ → republish stage=deliver
        stage=deliver  ├→ inbox-q   → Inbox λ    (render + DynamoDB + AppSync publish)
                       ├→ email-q   → Email λ    (render + SES)
                       ├→ slack-q   → Slack λ    (render + Slack API)
                       ├→ webhook-q → Webhook λ  (HMAC-sign + SSRF-guard + POST to tenant endpoint)
                       ├→ push-q    → Push λ     (device lookup + APNs / FCM)
                       └→ sms-q     → SMS λ      (consent + segment + SMS provider)
```

## Diagrams (`diagrams/`)

Mermaid sources (`.mmd`) are authoritative; PNGs are rendered from them.

| Source | Shows |
|---|---|
| `v14_components.mmd` | v1.4 component overview (the pipeline above) |
| `f1_explicit.mmd` | Flow 1 — explicit API notification, end to end |
| `f2_fanout.mmd` | Flow 2 — event-bus role fan-out + implicit group resolve |
| `f3_enrichment.mmd` | Flow 3 — AI gate: wait, retries/backoff, DLQ + alert, anti-clog |
| `f4_inbox.mmd` | Flow 4 — inbox read path via AppSync (queries/mutations/subscription) |
| `f5_triage.mmd` | Flow 5 — AI-assisted event triage (rule authoring, shadow mode) |
| `f6_counter.mmd` | Unread-counter lifecycle + reconciliation ladder |
| `f7_webhook.mmd` | Flow 7 — webhook channel A→Z (register/verify, HMAC signing, SSRF guard, breaker, DLQ) |
| `f8_push.mmd` | Flow 8 — mobile/web push (device registry, APNs/FCM, minimal content, token pruning) |
| `f9_sms.mmd` | Flow 9 — SMS (consent/STOP, segmentation, per-tenant budget, delivery receipts) |
| `f10_login_to_inbox.mmd` | Flow 10 — full flow: login → JWT → subscribe → device register → notification → Bell |
| `f11_resolve_race.mmd` | Flow 11 — fan-out resolved mid-flight: user X is **never** notified (unhappy path) |
| `f12_webhook_admin.mmd` | Flow 12 — webhook admin panel: delivery visibility + replay (single / time-frame / filtered) |

These detailed sequence diagrams are embedded directly in `Summary.html` at the relevant sections (Architecture, Channels, AI, Inbox, Webhook-ops, and a per-flow viewer). Open `Summary.html` from the repo root so the `diagrams/*.png` paths resolve.

> The remaining PNGs (`flow_*.png`, `solA_components.png`, `solB_components.png`, `context.png`) are v1.3-era renders kept for the superseded Word document.
