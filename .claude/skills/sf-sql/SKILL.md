---
name: sf-sql
description: StreamsForge streaming-SQL dialect cheatsheet — grammar, pipeline vs table mode, and the semantic gotchas (-> vs ->>, windowed-subquery rule, LATEST BY). Use when writing or debugging pipeline/table SQL, seeds, or SQL-touching tests.
---

# sf-sql — dialect quick reference

Full user docs: `orleans/docs/index.html` (§ Streaming SQL reference). Engine:
`orleans/src/StreamsForge.Engine` (tokenizer → parser → validator → planner; positioned
diagnostics, never exceptions). Validate cheaply via `POST /api/pipelines/validate` or
`/api/tables/validate` (`{sql}` → diagnostics with line/col).

## Grammar (one screen)

```sql
[WITH name AS (SELECT …), …]                       -- CTEs; earlier-only refs; NO recursion
SELECT expr [AS alias], … | * | alias.*
FROM source | (SELECT …) alias [, UNNEST(expr) AS l …]
  [ [INNER|LEFT|RIGHT|FULL [OUTER]] JOIN source|(SELECT …) alias WITHIN dur ON expr
  | CROSS JOIN source WITHIN dur | [CROSS] JOIN UNNEST(expr) AS l ] …
[WHERE expr]          -- may contain [NOT] IN (SELECT…), [NOT] EXISTS(…), scalar (SELECT agg…)
[GROUP BY expr, … | LATEST BY (col, …)]            -- LATEST BY: tables only
[WINDOW TUMBLING(SIZE d) | HOPPING(SIZE d, ADVANCE BY d) | SESSION(GAP d)]   -- pipelines only
[EMIT CHANGES | EMIT FINAL]
[UNION [ALL|DISTINCT] SELECT …] …    -- top level or FROM (…) alias only; UNION (no ALL): tables only
-- durations: N MILLISECONDS|SECONDS|MINUTES|HOURS · comments: -- · GROUP BY repeats the
-- exact expression (no alias sugar) · aggregates: COUNT/SUM/AVG/MIN/MAX + VAR_SAMP/VAR_POP/
-- STDDEV_SAMP/STDDEV_POP/MEDIAN/PERCENTILE_CONT(p,x)/COUNT(DISTINCT x) (VARIANCE/STDDEV = the _SAMP
-- forms; p must be a constant in [0,1]; all subtractable) · fns: ABS/ROUND/
-- UPPER/LOWER/COALESCE/TO_LONG/TO_DOUBLE/TO_BOOL/TO_TIMESTAMP/TO_STRING/IF(cond,a,b) · searched
-- CASE WHEN cond THEN expr [WHEN …] [ELSE expr] END (sugar for nested IF; all branches must agree
-- on type, ELSE defaults to NULL; no simple `CASE expr WHEN value` form) · CAST(expr AS type)
-- sugar for the TO_* fns (type: STRING/DOUBLE/LONG/BOOL/TIMESTAMP, also TEXT/BIGINT/INT/
-- BOOLEAN/DOUBLE PRECISION — not JSON) · JSON: expr -> 'key' (raw) / expr ->> 'key' (text) / -> 0 (index)
```

## Pipeline vs table mode

| | Pipeline | Table (materialized view) |
|---|---|---|
| Windows / EMIT / WITHIN | yes | **no** — running aggregates, retract/assert deltas |
| Joins | interval (WITHIN, all 5 kinds) | relational equi: INNER/LEFT/RIGHT/FULL OUTER/CROSS over current state (composite keys; CROSS needs Parallelism=1) |
| Inputs | stream sources | streams, PIPELINES and other tables — `FROM <pipeline>` is legal in **table mode only** (the pipeline's compiled output schema is the relation; no replay on a late attach) |
| LATEST BY | ✗ (diagnostic) | ✓ latest row per key by event ts |

## Gotchas that will bite you (all tested, all deliberate)

1. **`SUM(l ->> 'x')` returns NULL** — `->>` is always text, which SUM skips; aggregate the raw node:
   `SUM(l -> 'x')`. (SUM over no non-NULL value is NULL, as in PostgreSQL; COUNT(x) is 0.)
2. **Pipeline-mode subqueries must be windowed** (IN/EXISTS/scalar): value = rolling snapshot
   replaced at each inner window close. Tables have no such restriction.
3. **`NOT IN` is PostgreSQL's three-valued NOT IN**: one NULL in the subquery filters every row that
   did not match; `NULL NOT IN (empty)` is true. Use `NOT EXISTS` or filter the subquery's NULLs if
   that is not what you want.
4. **`LATEST BY`**: older-`_ts` arrivals ignored; upstream retraction drops the key (no fallback
   to prior versions — use row history for trails). Mutually exclusive with GROUP BY/aggregates.
5. **UNNEST**: element binds as a JSON value — address with `l ->> 'field'`, never `l.field`
   (diagnostic). Args reference real sources only (no UNNEST-of-UNNEST). Empty/NULL → zero rows.
6. **Windows-in-windows**: inner emissions carry the inner window's END timestamp outward.
7. **Correlated subqueries**: equality-only (decorrelated to a GROUP-BY join); anything else →
   "rewrite as a JOIN" diagnostic. Recursive CTEs rejected by design (see DESIGN.md §D3).
8. Bare `*`/`alias.*` are rejected with GROUP BY/aggregates; multi-input stars prefix as `t_col`.
9. Reserved row fields: `_ts` (epoch ms), `_source`.
10. **Table outer joins are incremental, not window-deferred**: an unmatched row is NULL-padded the
    instant it arrives and that pad is *retracted* the instant a match shows up (re-asserted if the
    match later disappears) — a consumer on the raw delta stream sees pad → retract → product
    chatter during cold start; only the consolidated state is meaningful. A NULL join key never
    matches: it's padded immediately on the padded side, contributes nothing on the other side, and
    (since the pad carries NULL in the padded columns) a *second* `INNER` join keyed on one of those
    columns drops the row while a second `OUTER` join pads it again. Moving a right-side condition
    from `ON` to `WHERE` silently degrades `LEFT` to `INNER` — the most common outer-join mistake.
11. **`CROSS JOIN` in table mode needs `Parallelism = 1`** — it has no equi-key to hash on; the table
    refuses to start above one partition with a clear error.
12. **`UNION ALL` works everywhere; plain `UNION` (distinct) is tables-only** — pipelines have no
    Z-set weight to dedup with, so `UNION` there gets a diagnostic naming `UNION ALL` as the fix.
    Branches need equal arity and unifiable column kinds (`Long`+`Double` → `Double`, else exact
    match, `JSON`/timestamp only with themselves); output names come from branch 0. Legal only at
    the top level (optionally under `WITH`) and in `FROM (… UNION …) alias` position — never inside
    `IN`/`EXISTS`/scalar subqueries. A set-operation table also needs `Parallelism = 1`.
13. **`TO_*`/`CAST` are total — unconvertible input is NULL, never an error** (`TO_LONG('abc')`,
    `TO_DOUBLE('')`, an overflowing numeric string). `TO_BOOL`'s string rule is permissive, not a
    fixed spelling list: `"true"`/`"false"` case-insensitively and `"0"`/`""` behave as expected, but
    ANY other non-empty string is also `true` (matches `FieldValueCoercion.TryToBool`'s existing
    inbound rule — same canonical implementation, `FieldValueConversion`, backs both). `TO_TIMESTAMP`
    accepts epoch-ms (number or numeric string) or ISO-8601 text. `TO_STRING` only renders ISO-8601
    when its argument is syntactically `TO_TIMESTAMP(...)`/`CAST(x AS TIMESTAMP)` — a bare
    `Timestamp`-kind column has no runtime tag distinguishing it from `Long`, so `TO_STRING(col)`
    prints a plain integer. `SUM(TO_DOUBLE(payload -> 'qty'))` is the fix for gotcha #1 above when the
    producer quoted its numbers. Full fine print: DESIGN.md §D11.

## Working examples (live seeds — copy as templates)

```sql
-- nested CTE + semi-join + windows (seed: "Hot symbol VWAP (nested)")
WITH hot AS (SELECT symbol FROM trades GROUP BY symbol WINDOW TUMBLING(SIZE 10 SECONDS))
SELECT t.symbol, SUM(t.price*t.qty)/SUM(t.qty) AS vwap, COUNT(*) AS trades FROM trades t
WHERE t.symbol IN (SELECT symbol FROM hot) GROUP BY t.symbol WINDOW TUMBLING(SIZE 5 SECONDS)

-- UNNEST over multileg legs (seed table: leg_exposure)
SELECT l ->> 'ccy' AS ccy, SUM(l -> 'notional') AS notional, COUNT(*) AS legs
FROM structures s, UNNEST(s.legs) AS l GROUP BY l ->> 'ccy'

-- LATEST BY current-state view (seed table: order_states)
SELECT order_id, symbol, side, stage, stage_rank, stage_ts, qty, filled_qty, px
FROM order_events LATEST BY (order_id)

-- UNION ALL fan-in (pipeline or table); drop ALL for dedup (tables only)
SELECT symbol, price AS px FROM trades
UNION ALL SELECT symbol, bid AS px FROM quotes
```
