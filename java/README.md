# StreamsForge on the JVM (Java 21)

A Gradle multi-project that brings the platform's runtime-agnostic core to Java and runs it on three
concurrency models. Bytecode level is 21 (`options.release = 21`) whatever JDK runs Gradle.

```bash
cd java && ./gradlew build          # everything, ~1 min
./gradlew :host:test -Dbench=true --tests '*Benchmark*' -i   # runtime comparison (off by default)
```

| module | what | README |
|---|---|---|
| `dataflow` | Differential dataflow library: lattice times (`LongTime`, `Product`), antichain frontiers, shared arrangements with logical compaction, join / reduce / count / distinct / semijoin / antijoin / `iterate`. No dependencies. Single worker. | [dataflow/README.md](dataflow/README.md) |
| `engine` | Faithful port of `shared/StreamsForge.Engine` (tokenizer, parser, validator, planners, pipeline + table executors, partitioned table dataflow). All 801 in-scope C# test cases ported with unmodified expectations; the other 193 of the C# suite's 994 test AppCore/Host types. | [engine/README.md](engine/README.md) |
| `engine-dd` | Table-mode SQL compiled onto `dataflow`, differential-tested row-for-row against `TableExecutor`; a recursive (`iterate`) reachability example table mode cannot express. | [engine-dd/README.md](engine-dd/README.md) |
| `actors-spi` | The Orleans-grain shape as a Java SPI: keyed virtual actors, non-reentrant async turns, timers, idle collection, lifecycle hooks — plus the conformance kit every runtime passes unmodified. | [actors-spi/README.md](actors-spi/README.md) |
| `actors-futures` | SPI over `CompletableFuture` mailboxes (no thread ever blocks on a turn). | ↑ |
| `actors-loom` | SPI over one JDK 21 virtual thread per activation (turns simply block). | ↑ |
| `actors-pekko` | SPI over Apache Pekko typed actors (Cluster-Sharding-style passivation, stash while an async turn runs). | ↑ |
| `host` | Mini StreamsForge host written once on the SPI: registry, sources, pipelines (watermark timers), tables incl. coordinator mode for parallelism ≥ 2 (epochs, markers, frontier min-combine, exchange), table-over-table late attach. Every scenario test runs on all three runtimes. | [host/README.md](host/README.md) |

## Engine bugs found by `engine-dd` (present in the C# original too)

Details in [engine-dd/README.md](engine-dd/README.md); repros in `EngineIssuesTest`. All three are fixed.

1. **Fixed (C# and Java, 2026-11-11):** table-mode GROUP BY value aggregates (SUM/MIN/MAX/AVG/COUNT(x)/
   COUNT DISTINCT/quantiles) were order-sensitive when a retraction arrived before its assertion —
   `TableReduceOp` dropped a group at total weight exactly 0, and the multiset aggregates ignored a retraction
   of a value they had not seen. Regression tests: `TableReduceOrderInsensitivityTests` in both engines.
2. **Fixed (C# and Java, 2026-11-11):** `SUM` over a group with no non-NULL value yielded 0 (AVG/MIN/MAX: NULL);
   it is NULL now, as in PostgreSQL.
3. **Fixed (C# and Java, 2026-11-11):** `NOT IN` ignored NULLs in the subquery and dropped NULL operands; it is
   PostgreSQL's three-valued `NOT IN` now (a NULL in the subquery filters every unmatched row; `NULL NOT IN (empty)`
   is true). Regression tests for 2 and 3: `PostgresNullSemanticsTests` in both engines.

## What it is not

No persistence, REST, auth, environments, connectors, replay or clustering — the host models the
grain protocol in one process. The closest JVM analogue of Orleans for going distributed is Pekko
Cluster Sharding, which `actors-pekko`'s passivation design already mirrors.
