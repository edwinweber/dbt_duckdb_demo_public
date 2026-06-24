Analyse the following task, identify ALL agents from the project team in `.claude/agents/` whose expertise is relevant, spawn them, and synthesise their outputs into a single cohesive answer for the user.

## Agent roles

- `investigator` — diagnose broken or unexpected behaviour; read-only, evidence-based
- `architect` — design decisions, trade-offs, hard-to-reverse choices
- `engineer` — implementation: Python, dbt, Dagster, dlt, DuckDB schema, tests
- `writer` — documentation, blog posts, explanations

## How to decide which agents to invoke

Invoke every agent whose lens adds something the others would miss. A task rarely needs only one.

| Task type | Agents |
|-----------|--------|
| Pure debug / something is broken | `investigator` only |
| New feature, no design ambiguity | `engineer` + `writer` (if docs needed) |
| New feature with architectural implications | `architect` → `engineer` → `writer` |
| Design question only | `architect` only |
| Documentation only | `writer` only |
| Broad question (e.g. "how should we approach X") | `architect` + `engineer` (parallel perspectives) |

When in doubt, spawn more rather than fewer — the user asked for the full team.

## Execution order

- **Parallel:** agents with no dependency on each other run simultaneously.
- **Sequential:** `architect` before `engineer` (design before code); `engineer` before `writer` (implementation before docs); `investigator` before anyone else when the root cause is unknown.
- Pass relevant output from earlier agents as context when spawning later ones.

## Output

Synthesise all agent responses into one structured answer. Label each section by the agent that produced it so the user knows which lens each part comes from. Do not just concatenate — integrate where the agents agree, and surface disagreements explicitly.

## Task

$ARGUMENTS
