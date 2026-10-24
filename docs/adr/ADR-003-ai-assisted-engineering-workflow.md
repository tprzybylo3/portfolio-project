# ADR-003: AI-Assisted Engineering Workflow — Claude Code + Codex + Hooks + Failure Log

## Status
Accepted

## Context

A separate thread from the main data platform architecture (see the original project plan,
`project-summary-for-consultation.md`, not part of this repository, and ADR-002). The goal is
to use modern AI coding agents (Claude Code, Codex) as a real part of a professional
engineering workflow while building this project — not as a separate "AI demo project", but
as a documented way of working on an already planned architecture.

The original proposal (own research + a consultation with ChatGPT) covered a wide set of
elements: Claude Code as the main implementer, Codex as an independent reviewer, a
multi-round adversarial debate between them with a formal `architecture_review.md` document
for every change, a custom MCP server for inspecting Airflow/Snowflake/Iceberg, Claude Code
hooks as guardrails, per-domain Skills (dbt, PySpark, Iceberg), subagents (SQL reviewer,
security reviewer, docs agent), Agent Teams, and AI-driven architecture regression testing.

After the consultation and research (current Claude Code capabilities in 2026, Claude↔Codex
cross-provider review patterns, availability of official MCP servers for Snowflake/Airflow,
signals valued by recruiters and senior DEs in portfolio projects) the scope was deliberately
narrowed.

Key findings from the research:

- **Redaction at the review handoff is critical for real independence.** If the reviewer
  (Codex) sees the implementer's (Claude's) reasoning, the assessment stops being
  independent — the reviewer "freeloads" on someone else's frame of thought and agrees more
  often. A truly independent review requires handing over only the code/diff + requirements,
  without the justification.
- **Official MCP servers for Snowflake and Airflow already exist** (Snowflake-managed MCP
  server, GA; `apache-airflow-providers-common-ai` with native MCP support) — building a
  custom server from scratch for these systems would be reinventing the wheel without
  justification.
- **The signal valued by senior/staff DEs and recruiters in 2026 is documented engineering
  judgement** (real decisions, tested, documented failures, explainable under interview
  pressure) — not the breadth of agent orchestration. An elaborate multi-agent architecture
  as a *core* part of a DE portfolio risks the opposite effect: it signals enthusiasm for AI
  at the expense of DE skills.

## Decision

**Main workflow (part of the main repo):**

```
You → task
Claude Code (Plan Mode: explore → plan → implement) → implementation
Hooks (deterministic guardrails: block destructive SQL, secrets) → allow/block
Deterministic tests (pytest / dbt test / Great Expectations) → validation
Codex (redacted handoff — code/diff only, WITHOUT Claude's reasoning) → independent review
Claude Code → fixes
Failure log (if a real incident happened) → ai/failure-log/NNN-description.md
You → decision → ADR (if it is an architectural decision)
```

1. **Claude Code as the main implementer**, driven by `CLAUDE.md` + Plan Mode
   (explore → plan → implement → test → verify), following the officially recommended
   pattern.
2. **Hooks as deterministic guardrails from the start** — a minimal, concrete set:
   blocking destructive SQL/bash commands (`DROP TABLE`, `rm -rf`, force-push), blocking
   secrets from being written to the repo, running tests after code changes.
3. **Codex as an independent reviewer, with an enforced redacted handoff** — Codex gets only
   the code/diff and the requirements, never Claude's justification/reasoning. One review
   round per significant PR, not a multi-round debate.
4. **Failure log** (`ai/failure-log/NNN-description.md`) — documents *real* incidents (AI
   mistake → how it was detected → root cause → fix → prevention), with a
   `## Detection layer` field (hook / test / Codex / manual), so that over time it shows a
   measurable evolution: more and more mistakes caught deterministically, fewer manually.
   Only real cases, not forced to reach a number of entries (4–6 well-described > 15
   stretched ones).
5. **PASS/FAIL architectural regression with evidence**, tied to the ADRs — in practice a
   well-organised set of tests mapped to ADR requirements (e.g. "idempotent ingestion" →
   `test_incremental_rerun.py`, `test_duplicate_batch.py`), not a separate "AI layer".
6. **AI diagnoses, determinism detects** — in data quality incidents, the LLM gets ready
   signals (deviation %, freshness, duplicates) and formulates a hypothesis + evidence; it
   never decides on its own whether the data is correct.

**A separate experimental folder (`experiments/`), outside the main workflow:**

```
experiments/
├── mcp/              (custom MCP server — only when a concrete gap appears, i.e. the
│                       moment "Claude has no access to information X and therefore
│                       cannot diagnose the problem well"; not built upfront)
├── subagents/
├── agent-teams/
└── adversarial-review/   (multi-round Claude↔Codex debate protocol)
```

## Considered and rejected

- **A multi-round adversarial debate with a formal `architecture_review.md` for every
  change as the core workflow** — rejected for the core repo. For a solo project it is
  process theatre; one well-done review round > a documented corporate-style "debate" for a
  one-person repo. Kept as an experiment.
- **A custom MCP server built upfront** — rejected. Official, fully supported MCP servers
  for Snowflake and Airflow already exist. A custom server makes sense only with a concrete,
  uncovered use case (e.g. cross-referencing Airflow + Iceberg + dbt in a single query).
- **A subagent army / Agent Teams as the foundation** — rejected for the core repo. The value
  grows with team work or a very large repo; the cost is duplicated context and coordination
  without a proportional benefit for a solo project.
- **Separate per-domain Skills (dbt/PySpark/Iceberg) as large documentation dumps** —
  rejected in this form. A short `CLAUDE.md` + hooks where a rule must be enforced
  deterministically covers most needs at this stage.

## Consequences

**Positive:**
- The workflow is learned on a real, working example instead of building scaffolding around
  an empty project
- The failure log gives the narrative "AI makes a mistake → we detect it → we understand
  why → we add a test/guardrail → the same mistake becomes impossible" — a stronger
  engineering signal than a list of AI tools
- Redacting the handoff to Codex gives real, not simulated, review independence
- A clear separation of core vs `experiments/` protects the main project from form
  overtaking substance

**Negative / trade-offs:**
- Giving up the full vision of multi-agent orchestration from the original proposal — some
  of the more ambitious elements (Agent Teams, MCP) are postponed until a concrete
  justification appears
- Requires discipline in writing the failure log — it is easy to skip it "because it takes
  time", and then the strongest element of this layer is lost

## Related

- `project-summary-for-consultation.md` — the original plan of the main data platform
  architecture (not part of this repository)
- ADR-002 — the Bronze/ingestion strategy on which the first Claude Code + hooks + Codex
  review flow will be tested (seed script + incremental extract to S3)

## Next step

The first real test of the workflow: a Faker seed script for the 7 `postgres-source`
tables, written by Claude Code with a hook blocking destructive SQL from the start, followed
by one round of redacted review by Codex.

## Retrospective (added during Phase 4)

**Observation:** the "Next step" above assumed the workflow would start with `seed.py`
(Phase 1). In practice, Phases 1-4 (ingestion, validation, PySpark/Iceberg, Snowflake) were
implemented directly in a conversation with Claude (chat), without the formal Claude
Code/Codex workflow — no Plan Mode, no hooks, no redacted review, no entries in
`ai/failure-log/`. This is a real gap between what ADR-003 declares and what actually
happened, named openly, not hidden.

**Decision:** do not catch up retroactively — the code from Phases 1-4 is already written,
tested and verified on live data; rewriting it only to match the process does not give
proportional value compared to the time better spent on Phases 5-8. The ADR-003 workflow
actually takes effect **from Phase 5 (dbt)** — the first switch to Claude Code
(implementation) + Codex (review) will be explicitly signalled in the project conversation,
with a ready `CLAUDE.md` prepared before the tools are switched.

**Justification, why this is not just an excuse:** this conversation (chat) serves as the
design/decision layer (planning, ADRs, live debugging in the AWS/Snowflake console) — it is
not, and was never meant to be, a substitute for Claude Code. Phases 1-4 were in practice
exploratory work (first contact with Iceberg/Snowflake, many "why" questions), where the
value came from conversational learning, not from automation. Phases 5-8
(dbt/Airflow/CI/CD/Terraform) are a more consolidated implementation of already known
patterns — a naturally better moment for a formal implementation workflow with guardrails.
