# brazil-finance-lakehouse

Batch + streaming lakehouse on Brazilian public financial data (BACEN, CVM). Stack: Python, Databricks Free Edition (Unity Catalog, Delta, Auto Loader, AI/BI dashboards), dbt, Evidence, GitHub Actions. Goal: a public portfolio that shows modern data engineering practice. Quality of process matters as much as the code.

## Hard rules

- **No AI/tool attribution anywhere in this repo.** No `Co-Authored-By`, no session links, no "generated with" lines in commits, PRs, issues, code comments, docs or READMEs. Commits and PRs read as written by the repo owner.
- **No employer material.** Nothing from the owner's workplace: no code, table names, schemas, hostnames or patterns copied. Everything here is built from public data and generic knowledge.
- **No secrets in git.** Tokens come from env vars / GitHub secrets. Keep `.env.example` updated, never `.env`.
- **No raw data in git.** BACEN/CVM data is open (ODbL) but stays out of the repo. Attribute sources in the README.
- **No PII.** Anything customer-like is synthetic and seeded (M4).

## Workflow

- Work is tracked as GitHub issues on the project board. Start from an issue; read its "Done when" list.
- One branch per issue: `feat/<n>-short-name`, `chore/<n>-...`, `fix/<n>-...`.
- Conventional Commits (`feat:`, `fix:`, `chore:`, `docs:`, `test:`, `ci:`). Reference the issue; the PR body says `Closes #<n>`.
- Small PRs, squash merge. CI must be green before merge.
- Only the next milestone gets story-level detail. Do not pre-build later milestones.
- Non-trivial change: give a short plan (a few lines) and wait for a go before writing code. Ask a guiding question when a decision is open; don't pad with options nobody asked for.

## Commands

```
make lint     # ruff check + ruff format --check
make test     # pytest
```
Package manager is `uv` (`uv add`, `uv run`). Don't use `pip install` directly.

## Layout

```
ingestion/    connectors + uploader (run on GitHub Actions; Free Edition has restricted egress)
databricks/   bronze jobs, later Asset Bundle config
dbt/          models, tests, sources (M2)
bi/           Evidence site + AI/BI dashboard (M3)
docs/adr/     architecture decision records
tests/
```

## Conventions

- Python 3.12, type hints on public functions, ruff for lint and format.
- Comments: short and practical, explaining *why*. No docstring blocks unless asked.
- Connectors must be **idempotent** (same window → same output path), retry with bounded backoff, and never swallow errors silently.
- Bronze is append-only and keeps raw payloads untouched in an envelope; dedup and typing happen in silver.
- Every non-obvious decision gets an ADR in `docs/adr/` (Context / Decision / Consequences), numbered `NNNN-title.md`.
- Tests: mock HTTP, no network in unit tests. Cover edge cases (leap years, empty responses, window boundaries).
- Data-source facts (API limits, series codes) must be verified against the official docs, not assumed. BACEN SGS date-range queries are capped at 10 years per request; the connector uses windows of at most 5 years.

## Writing

Invoke the `humanizer` skill (`/humanizer`, embedded mode) on every piece of prose before it leaves your machine, and use the result as the final text. Do not apply its rules from memory. A PreToolUse hook (`.claude/hooks/humanizer-check.py`) blocks commits and PR/issue text that contain dashes or attribution lines, but it cannot run the skill for you. Covers: commit messages (subject and body), PR titles and bodies, issue titles and bodies, comments on issues and PRs, README and other docs, ADRs, and code comments. Write for the reader, in the repo owner's voice, with no AI tells.

- State the point directly. No "not X but Y" contrasts, no one-line closers that repeat the paragraph, no staged openers ("Here's the thing"), no aphorisms.
- Name the specific thing: which file, series code, limit or decision. Cut inflated words (crucial, robust, pivotal, key, highlight) and sales language.
- No em or en dashes. Use a period, comma, colon or parentheses.
- No bold labels on list items, no decorative emoji or arrows, sentence-case headings.
- Keep commit subjects short and plain. Put the why in the body only when it is not obvious from the diff.
- Re-read the final text once more for forced triads and repeated sentence openings before committing, pushing or posting.

## Definition of done (every PR)

- [ ] `make lint test` passes locally and in CI
- [ ] Acceptance criteria in the issue are all met
- [ ] README "What works today" updated if behavior changed
- [ ] ADR added or updated if a decision was made
- [ ] No secrets, raw data or employer-specific content
- [ ] Humanizer pass done on commits, PR text, docs and comments
