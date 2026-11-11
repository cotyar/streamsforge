# engine-dd - table-mode SQL on differential dataflow

`engine-dd` connects two modules of `java/`:

* `:engine` - port of the StreamsForge streaming-SQL engine. Table mode (`SqlCompiler.compileTable` -> `TablePlan` ->
  `TableExecutor`) is hand-written Z-set / DBSP-style incremental view maintenance.
* `:dataflow` - a general differential dataflow library (lattice times, arrangements, join / reduce / distinct / antijoin /
  iterate).

It does two things: **compile table-mode SQL onto the dataflow library** (`DataflowTableCompiler`), and **use the engine's
`TableExecutor` as an oracle** (and vice versa) in a differential test suite: same random insert/retract stream in,
accumulated output must be identical row for row at every epoch. It also holds one query table mode cannot express at all,
a recursive one (`Reachability`).

```
cd java && ./gradlew :engine-dd:test            # 157 tests, 6 skipped (the @Disabled bug repros), 0 failed
```

## Usage

```java
var outcome = DataflowTableCompiler.compile(
    "SELECT sym, COUNT(*) AS c, SUM(qty) AS q FROM trades GROUP BY sym", streamSchemas, tableSchemas);
// outcome.status(): OK | INVALID (the engine rejected it; diagnostics attached) | UNSUPPORTED (valid, outside the subset; message says what)
DataflowTableQuery q = outcome.plan().newQuery();          // every query owns an independent dataflow graph
q.update("trades", row, +1);  q.update("trades", old, -1); // rows are engine EventRecords; weight -1 = retraction
long epoch = q.commit();                                   // closes the epoch, runs the dataflow to quiescence
q.snapshot();     // Map<canonicalRowKey, RowWeight>  - exactly TableExecutor.snapshot()'s shape
q.deltasAt(epoch) // the netted output changes that epoch produced
```

Tokenizing, parsing, validation and planning are the engine's own (`SqlCompiler.compileTable` -> `CompiledTablePlan`);
the compiler only walks the planned query. WHERE / SELECT / ON / GROUP BY expressions are evaluated by the engine's own
`ExpressionEvaluator`, so scalar semantics (three-valued logic, numeric promotion, JSON access, every scalar function) are
identical by construction. Output rows are `Out` objects whose equality *is* `TableExecutor.canonicalRowKeyOf`.

## Supported subset

| Construct | Supported | Notes |
|---|---|---|
| filter / projection / expressions | yes | any scalar the engine supports |
| INNER / CROSS join | yes | any number of inputs, composite keys (first key joins, rest folded into a residual, as the engine plans it), self-joins |
| LEFT / RIGHT / FULL join | yes | with composite keys and residual predicates |
| `[NOT] IN (subquery)`, `[NOT] EXISTS` | yes | semijoin / antijoin |
| `JOIN UNNEST(expr) AS x` | yes | `flatMap` |
| GROUP BY, global aggregate | yes | `COUNT(*)`, `COUNT(x)`, `COUNT(DISTINCT x)`, `SUM`, `AVG`, `MIN`, `MAX` |
| derived tables, CTEs, aggregate-over-aggregate | yes | recursive compilation of the nested plan |
| `UNION ALL`, `UNION` | yes | concat / concat + `distinct` |
| "SELECT DISTINCT" | n/a | the dialect has no `SELECT DISTINCT`; the idiom `SELECT x FROM t GROUP BY x` and `UNION` are covered |
| `LATEST BY`, scalar subqueries, `VAR_*`/`STDDEV_*`/`MEDIAN`/`PERCENTILE_CONT`/user aggregates | **refused** | `Status.UNSUPPORTED` with a message naming the construct |
| client key-retraction rows (`_retract = true`) | refused at `update()` | feed weight -1 instead |

## How SQL maps to dataflow operators

Rows travel as `Wr` (an engine `WorkingRow` - alias-qualified columns - with value equality). Times are `LongTime` epochs.

| SQL | Dataflow |
|---|---|
| leaf stream/table | one `InputHandle<EventRecord>` per name; `map(WorkingRow.fromEvent(alias, e))` per use (self-joins share the input) |
| `WHERE p` | `filter` (after all joins, as the engine does) |
| projection | `map` to an `Out` (adds `_ts` = max of the contributing row timestamps, `_source`) |
| `a JOIN b ON k` | both sides `map`ped to `(encodedKey, row)` with NULL keys dropped, `Keyed.join`, `WorkingRow.combine`, `filter(residual)` |
| `a LEFT JOIN b` | the inner join above `+ (a antijoin matchedLeft)` null-padded, where `matchedLeft = pairs.map(left).distinct()`; RIGHT symmetric; FULL both. Per-row matched-ness (not per key) makes residual predicates exact |
| `x IN (SELECT ..)` / `EXISTS` | `Keyed.semijoin(keys.distinct())`; `NOT IN` / `NOT EXISTS` -> `antijoin`. Presence, not multiplicity, as in the engine |
| `JOIN UNNEST(e) x` | `flatMap` |
| `GROUP BY g` + aggregates | `map` to `(groupKey, Tup(groupValues, aggArgs))`, `Keyed.reduce`: total weight > 0 -> one output row, aggregates computed from the group's `(value, weight)` multiset |
| `UNION ALL` / `UNION` | `concat` / `concat().distinct()` |
| derived table / CTE | the nested plan's collection is re-wrapped as `WorkingRow`s under the new alias |

The dataflow reduce recomputes a group from its (arranged, consolidated) tuples each time the group changes - O(distinct
tuples in the group) per update where the engine's aggregators are O(1) per update. See throughput.

The aggregate functions (`Aggs`) are deliberately **not** the engine's `IZAggregator`s: they are re-implemented over the
group's value multiset so the differential tests compare two independent aggregate implementations.

## Differential testing

`Diff` generates, per case (fixed seed, `base*100003 + caseIndex`), 8 epochs of 0-6 events over the query's inputs (small
value domains so keys collide; NULLs in keys and values; doubles that are multiples of 0.25 so sums are exact in any order;
35% of events retract a live row, 15% re-insert a live row as a duplicate = weight 2). Events go to
`TableExecutor.onTableDelta` (alternating one call per event / one `onTableDeltaBatch` per source and epoch) and to
`DataflowTableQuery.update`; after every epoch the full output maps (canonical row key -> weight) are compared, and the
dataflow side must also hold no negative weight. `-Ddd.cases=N` changes the case count (default 300).

Coverage - **48 queries**, 300 cases x 8 epochs each:

| Construct | Queries (cases x epochs each) |
|---|---|
| filter / project / null logic / functions | filter-project, select-star, null-logic, functions (4 x 300 x 8) |
| inner joins | inner-join, +residual, +composite key, +WHERE, three-way, self-join, cross-join (7 x 300 x 8) |
| outer joins | left, left+residual, right, full (+composite), left-chain, inner-then-full (6 x 300 x 8) |
| group by / aggregates | count, all five aggregates (long/double/string columns), WHERE, two keys, expression key, aggregate expressions, global aggregate, COUNT(DISTINCT), over join, over LEFT JOIN, the DISTINCT idiom (13 x 300 x 8) |
| unions | UNION ALL, UNION, 3-way UNION, aggregate branches (4 x 300 x 8) |
| semi / anti joins | IN, IN+filter, NOT IN, EXISTS, NOT EXISTS, IN+NOT IN, IN over aggregate subquery, IN under GROUP BY (8 x 300 x 8) |
| derived tables / CTEs | derived, CTE joined back, aggregate-of-aggregate, UNION inside a derived table (4 x 300 x 8) |
| unnest | unnest, unnest+group (2 x 300 x 8) |

Totals per pass: in-order pass **115,200 epochs, 345,193 events (105,067 retractions)**; "load" pass (up to 30 events per
epoch, 60 cases x 5 epochs per query) **14,400 epochs, 216,811 events**; "shuffled" pass (the same events delivered in
random order within an epoch, so a retraction can reach the engine before the assertion it cancels; end-of-epoch state is
compared) **79,200 epochs, 316,667 events** over the 33 queries without value aggregates (the other 15 are the
`@Disabled` order-sensitivity finding below). Three queries also run with one input registered as a materialized table
(`tableSchemas`) instead of a stream (200 cases x 8 epochs each).

Comparing with the engine is only as good as the independence of the two sides. **Shared on purpose** (so not tested by
differential means): the tokenizer/parser/validator/planner, `ExpressionEvaluator`, `TableKeyEncoding` (join/group key
equality), `WorkingRow.combine`. **Independent**: every join / outer-join padding / semi-anti / union / distinct operator
(Z-set index code vs arrangements), group maintenance, and all aggregate functions.

## Discrepancies found and verdicts

Result of running the above: **after one dialect alignment (1) the two sides agree on every in-order case; every remaining
disagreement is a defect of the engine** (the dataflow side is order-insensitive Z-set algebra by construction). Repros are
`@Disabled` tests in `EngineIssuesTest` that assert the *correct* answer; run them with
`./gradlew :engine-dd:test -Djunit.jupiter.conditions.deactivate='org.junit.*DisabledCondition'` to see them fail (5 failures
there + 15 in the shuffled aggregate test).

1. **`SUM` over a group whose argument is NULL in every row returns `0`; ANSI SQL says NULL. Engine wrong (dialect
   deviation, internally inconsistent).** The engine's `AVG`/`MIN`/`MAX` (and `VAR_SAMP`, whose doc says "undefined, not
   zero") return NULL for the same group; only `SumZAggregator` returns `0`. Found as the first mismatch of the
   differential run (the dataflow side followed ANSI). `DataflowTableCompiler.Options(sqlStandardEmptySum)` selects either;
   the default matches the engine so the suites can compare. Repro: `engineSumOfOnlyNullsIsNull` (disabled); current
   behaviour pinned by `engineSumOfNothingIsZeroWhileAvgMinAreNull_pinsCurrentBehaviour`.
2. **GROUP BY value aggregates are not order-insensitive: a retraction that reaches the engine before the assertion it
   cancels corrupts the group. Engine wrong.** `TableOuterJoin`/join/semi/anti/union/unnest survive arbitrary reordering,
   but `TableReduceOp` removes a group's state when its total weight crosses exactly 0 (`groups.remove(key)`) although the
   aggregators then hold a non-empty `{+r2, -r1}` multiset; the later assertion of `r1` starts a fresh group holding only `r1`.
   Repro: retract(r1: qty 5), assert(r2: qty 7), assert(r1) -> `SUM = MAX = 5`, correct `7` (`engineGroupByIsOrderInsensitive`).
   Separately `MinMaxZAggregator.apply` creates a multiset entry only for `weight > 0`, so a retraction of an unseen value is
   lost even when the group stays alive: rows {3, 4} then retract(5), assert(5) -> `MAX = 5`, correct `4`
   (`engineMaxIgnoresRetractionOfUnseenValue`). `COUNT(*)`-only groups are unaffected. This matters in practice: the
   engine's own `unmatchedRetractions` counter and the doc comment on the negative-total case show the authors know
   retractions can arrive first. The parameterized `engineValueAggregatesAreOrderInsensitiveWithinAnEpoch` (disabled) is the
   differential form: 15 queries fail.
3. **`x NOT IN (subquery)` and NULL (ANSI deviation shared by engine and dataflow compiler).** ANSI: a NULL in the subquery
   makes `NOT IN` UNKNOWN for every non-matching x (no rows); `NULL NOT IN (empty set)` is TRUE. `TableSemiAntiOp`
   ignores NULL right keys ("the NOT IN NULL rule" in its comment) and drops every NULL left key even for an empty
   subquery. The compiler copies the engine so the two agree (`notInWithNullInSubquery_pinsCurrentBehaviour`); the ANSI
   answers are the disabled repros `notInWithNullInSubqueryReturnsNothing`, `nullNotInEmptySubqueryIsTrue`. Verdict: engine
   deviates from ANSI; whether that is a deliberate dialect choice is not documented.
4. Not discrepancies, noted while writing queries: the dialect has no `IS NULL`, no `SELECT DISTINCT`; a CTE body cannot
   be a set operation (use `FROM (SELECT .. UNION .. SELECT ..) alias`); canonical row identity does not distinguish
   `0` from `0.0` (`Invariant.toString`), so the SUM result type being history-dependent (sticky `sawDouble`) is not visible
   at the row-identity level.

## Recursive queries (`Reachability`)

Table mode has no `WITH RECURSIVE` (nor can a fixed operator pipeline loop), so reachability / transitive closure is written
directly on `Collection.iterate`, fed by the same event model - an `edges` table of `EventRecord(src, dst)` rows with weight
+1 / -1 - and maintained incrementally, including edge retractions that disconnect nodes and edges on cycles:

```java
roots.iterate((scope, reach) -> {
    var e = scope.enter(edges).<Long, Long>keyed(Pair::first, Pair::second);
    return reach.<Long, Unit>keyed(n -> n, n -> Unit.U).join(e, (src, u, dst) -> dst)
                .concat(scope.enter(roots)).distinct();                       // reachable(m) :- reachable(n), edge(n, m)
});
edges.iterate((scope, paths) -> {                                              // path(a,c) :- path(a,b), edge(b,c)
    var e = scope.enter(edges);
    return paths.<Long, Long>keyed(Pair::second, Pair::first).join(e.<Long, Long>keyed(Pair::first, Pair::second), (b, a, c) -> Pair.of(a, c))
                .concat(e).distinct();
});
```

`ReachabilityTest`: a worked example (cycle, cut and reconnect, duplicate edges needing both copies retracted), 300 random
cases x 8 epochs (7,373 events, 2,401 retractions, graphs of 7 nodes with self loops and duplicate edges) checked against
BFS / naive closure after every epoch, and a test showing the contrast: the engine's 2-hop self-join over the same edge
table yields 5 rows on a 5-cycle where the closure has 25 pairs.

## Throughput (rough)

`ThroughputBenchTest`, Apple M1 Max, JDK 23, 2026-11-11, one JVM, short run after a 10k-event warm-up (so noisy - treat as
order of magnitude, not a benchmark). Stream: 80% trades / 20% quotes, 20% of all events retract a live row, 1000 symbols,
dataflow epoch = 500 events; both sides end in the identical state (asserted). Events/second:

| Query | Engine `TableExecutor` | Dataflow compiler | ratio |
|---|---|---|---|
| filter + project | 1,085,000 | 1,323,000 | 1.2x |
| GROUP BY, 1000 groups, 5 aggregates | 228,000 | 53,000 | 0.23x |
| inner join on a 1000-key column | 47,000 | 42,000 | 0.9x |
| join + GROUP BY | 31,000 | 30,000 | 1.0x |

(200,000 events; at 40,000 events: 0.97M/1.0M, 177k/109k, 145k/174k, 119k/91k.) Stateless and join queries are on par; the
grouped query is where the dataflow side loses at larger sizes, because its reduce recomputes a group from all of its
distinct tuples on every change while the engine's aggregators update in O(1).

## Not done / limits

* The aggregate set is the five core ones; statistical aggregates, `LATEST BY`, scalar subqueries and retention are refused.
* Single worker, single thread (the library's limit). The SQL side's `Parallelism`/shard tiers have no counterpart.
* Output `_ts` follows the engine (max of contributing timestamps; 0 for grouped rows) because it is part of row identity.

## Layout

`DataflowTableCompiler` (entry point, subset check, `Options`) - `DataflowTablePlan` - `GraphBuilder` (plan -> operators) -
`DataflowTableQuery` (runtime: inputs, epochs, snapshot) - `Aggs` - `Out`, `Wr` (data types) - `Reachability`.
Tests: `DifferentialTest`, `CompilerTest`, `EngineIssuesTest`, `ReachabilityTest`, `ThroughputBenchTest` (+ `Diff` harness).
