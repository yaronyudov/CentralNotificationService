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

The pipeline is **one intake Lambda, one SNS topic, and one Lambda per channel**. `NotificationType.channels[]` becomes SNS message attributes and each channel queue subscribes with a filter policy — routing is configuration, not code (no Router λ, no Renderer λ; each channel λ renders its own template). The in-app surface is **one AppSync GraphQL API** (queries + mutations + real-time subscriptions; AppSync manages all connections — no WebSocket API, no connection-registry table). AI enrichment is a **gate**: AI-type messages carry `stage=enrich` and wait in the enrich queue — **no channel ever sees them before enrichment succeeds**; failures are classified, retried with backoff, and after max attempts land in an alarmed DLQ (redrive delivers). Bedrock is protected from clogging by a reserved-concurrency ceiling, a signature cache, a pre-invoke budget gate, a quota-matched token bucket, and a circuit breaker.

```
Producers (API GW HTTP | EventBridge rules)
   → Intake λ   validate · idempotency · payload→payloadRef · RBAC resolve · resolved-check
   → SNS topic  attrs: channels[] (from type config) + stage
        stage=enrich   → enrich-q ⟵ AI messages WAIT here → Enricher λ → republish stage=deliver
        stage=deliver  ├→ inbox-q → Inbox λ  (render + DynamoDB + AppSync publish)
                       ├→ email-q → Email λ  (render + SES)
                       └→ slack-q → Slack λ  (render + Slack API)
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

> The remaining PNGs (`flow_*.png`, `solA_components.png`, `solB_components.png`, `context.png`) are v1.3-era renders kept for the superseded Word document.
