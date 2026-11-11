# java/engine — Java 21 port of `StreamsForge.Engine`

Faithful port of `shared/StreamsForge.Engine/**` (C#, ~11.4K lines). The engine is pure (no actor/runtime/HTTP types);
its only dependency is nothing (Jackson is allowed by the build but unused). Build/test: `cd java && ./gradlew :engine:test`
(Gradle needs a JDK 21 toolchain on `JAVA_HOME`; bytecode is `--release 21`).

## C# → Java mapping

| C# (`StreamsForge.Engine…`) | Java (`io.streamsforge.engine…`) |
|---|---|
| `PublicApi.cs` | `EventRecord`, `SqlDiagnostic`, `DiagnosticSeverity`, `FieldKind`, `SourceSchema`, `CompileResult`, `TableCompileResult`, `TableDelta`, `TableRetentionPolicy`, `SqlCompiler`, `PipelinePlan`, `PipelineExecutor`, `TablePlan`, `TableExecutor`, `RowWeight` (the `(Row, Weight)` tuple) |
| `Sql/Token, Tokenizer, Parser, Ast` | `sql.Token, Tokenizer, Parser` ; **all AST node types are nested classes of `sql.Ast`** (`import static io.streamsforge.engine.sql.Ast.*`) |
| `Sql/Validator.cs` (+ result types) | `sql.Validator, ValidationResult, JoinBinding, DerivedInfo, UnionDerivedInfo, ResolvedSource, SetOperationValidationResult, SubqueryPredicateInfo, CorrelationKey, ScalarSubqueryInfo, ValidationMode, JoinKeyFolding` |
| `Sql/ReferenceExtractor`, `SqlFunctions` | `sql.ReferenceExtractor`, `sql.SqlFunctions`, `sql.IScalarFunction`, `sql.IAggregateFunction` |
| `Planning/*` | `planning.Planner, TablePlanner, CompiledPlan, CompiledSource, CompiledJoin, CompiledTablePlan, CompiledTableSource, CompiledTableJoin, OutputItem` |
| `Runtime/*` | `runtime.ExpressionEvaluator` (+ nested `Binding`, `EvalContext`, `AggregateLookup`), `SqlValues`, `JsonText`, `TableKeyEncoding`, `WorkingRow`, `ZSetIndex`, `Aggregator`, `ZAggregator`/`IZAggregator`, `StatAggregatorNames` (+ package-private stat/distinct/median aggregators), `AggregateParameters`, `FieldValueConversion`, `ConsolidationLedger`, `IRetentionScope`, `RetentionOrder`, `LedgerRetentionScope`, `TableRetentionSupport` |
| `Runtime/Ops/*` | `runtime.ops.*` (`PipelineJoinOp`, `PipelineWindowOp`, `PipelineFilterProjectOp`, `PipelineSubqueryOp`, `PipelineUnnestOp`, `Table{Ingest,FilterProject,Reduce,Join,SemiAnti,OuterJoin,Unnest,Distinct,LatestBy}Op`, `TableRowDelta`, `ITableOp`, `ITableJoinStage`, `IPipeline*`) |
| `Dataflow/*` | `dataflow.*` (`Epoch, EdgeId, UpstreamId, DeltaBatch, FrontierTracker, EpochBuffer, ExchangeSpec, ExchangeRouter, TableDataflowPlan, TableDataflowBuilder, TableStageExecutors, WorkingRowWireCodec, ArrangementKeySpec, TableStageKindLabel`, enums/records/`ITableStageExecutor`) — all **public** |
| `TablePlan.CreateDataflow` partial | `TablePlan.createDataflow(int)` |

## Public API summary
`SqlCompiler.compile / compileTable / extractReferences` (never throw on bad SQL); `PipelinePlan.createExecutor()` →
`PipelineExecutor.onEvent / advanceWatermark / getWatermark / getLateEvents`; `TablePlan.createExecutor()` →
`TableExecutor.onStreamEvent / onTableDelta / onTableDeltaBatch / snapshot / canonicalRowKey / canonicalRowKeyOf / lastEpoch /
configureRetention`; `TablePlan.supportsRetention / isGlobalAggregate / createDataflow`. Records/classes expose camelCase
accessors (`result.ok()`, `delta.weight()`); compiled-plan / AST / validation-result classes expose public final (or plain
public) fields named like the C# properties in camelCase. Value model: `Long, Double, Boolean, String, null,
Map<String,Object>, List<Object>`.

## Deliberate deviations
* **Visibility:** C# `internal` types are `public` (Java packages cannot share package-private across the mirrored layout; the tests live in another package). Nothing outside `StreamsForge.Engine.Tests` was granted access in C#; treat them as engine-internal.
* **AST** nested in `Ast`; AST/plan nodes keep reference identity (no `equals`), as the Validator/Planner key maps by reference (`IdentityHashMap`).
* **Enums** are UPPER_CASE (`FieldKind.DOUBLE`, `JoinKind.SEMI`, `TableStageKind.SEMI_ANTI`). Validator/Planner messages and `TableStageKindLabel` keep the C# PascalCase text.
* **Exceptions:** `NotSupportedException`→`UnsupportedOperationException`, `InvalidOperationException`→`IllegalStateException`, `ArgumentException/OutOfRange`→`IllegalArgumentException` (messages kept).
* **`TryCoerce`/`TryToTimestamp`** return a `Coercion(ok, value)` record instead of `bool` + `out`.
* **CLR date/numeric types** (only reachable through `FieldValueConversion`): `DateTimeOffset`↔`OffsetDateTime/ZonedDateTime/Instant`, `DateTime`↔`LocalDateTime` (read as UTC, like `Unspecified`), `decimal`↔`BigDecimal`, `ulong`↔`BigInteger`, `int/short/byte/float`↔`Integer/Short/Byte/Float`. `DateTimeOffset.TryParse` is approximated by ISO-8601 (with/without offset or `T`), date-only and `M/d/yyyy [H:mm[:ss]]`; exotic .NET date formats are not accepted.
* **`TimeSpan`**→`java.time.Duration` (ms rounding uses `Math.rint` = .NET banker's rounding where C# used `Math.Round`).
* **Double text** (`runtime.Invariant`) reproduces .NET's `double.ToString(InvariantCulture)` (shortest round-trip; sci notation for exp ≥ 15 / < −4: `1E+20`, `1E-05`). Row identity (`JsonText.serializeCanonicalRow`) is byte-identical.
* **`ROUND`** reproduces `Math.Round(x, digits, AwayFromZero)` including .NET's `digits ≤ 15` limit (throws `IllegalArgumentException` beyond).
* **Dictionary enumeration order:** `runtime.NetDictionary` reproduces .NET `Dictionary` order (free-list reuse after removals); used for window state, group state, join buckets, ledger, LATEST BY. Other maps use `LinkedHashMap`/`HashMap` where the C# order is not observable.
* `ToUpperInvariant/ToLowerInvariant`: per-code-point simple case mapping (no `ß→SS`).
* `ExchangeRouter` UTF-8 encoding substitutes U+FFFD for lone surrogates (as .NET), not `?`.
* A Mod/Abs/etc. `OverflowException` case (`long.MinValue % -1`, `Abs(long.MinValue)`) throws `ArithmeticException`.

## Known gaps
* None of the engine is unported. `Planner`'s `Internal compiler error: <message>` fallback carries Java exception text, which differs from .NET exception text.
* The C# unit-test baseline could not be run in this worktree (the `external/ycs` submodule is not checked out, so `StreamsForge.Connectors.Crdt` fails to build); C# counts below come from reading the test sources.

## Test-porting ledger
Ported: **60 test classes** (of 77 test classes in `orleans/tests/StreamsForge.Engine.Tests`; 17 are out of scope, below) — all 801 C# test cases (`[Fact]` + one per `[Theory]` row) → 801 Java cases, per class equal (plus 3 Java-only `SmokeTest` cases). Gradle: `:engine:test` → 804 tests, 0 failed, 0 skipped. No expectation was modified.

Out of scope (exercise non-Engine types — AppCore/Host/Orleans/Abstractions): `ClusterSmokeTest`, `TableGrainClusterSmokeTest`
(Orleans test cluster); `IngressAdmissionTests`, `IngressEnvelopeBuilderTests`, `IngressRowAcceptanceTests`,
`RetractIngestRowAcceptanceTests`, `RetractConsumerValidationTests`, `RetractConsumerValidationCollectRowIndexesTests`,
`IngestIdempotencyCacheTests`, `DedupTrackerCustomBoundTests`, `SourceIngress{Buffer,BufferDedup,Registry}Tests`,
`FieldValueCoercionTests` (AppCore.Ingest); `ScenarioGeneratorTests`, `ScenarioGeneratorSteppingTests` (Host.Generators);
`TableSearchIndexTests` (Host.Search). `TestHelpers.cs` → `TestHelpers.java`; `PartitionedTableHarness.cs` → `PartitionedTableHarness.java`.
Java-only test helpers: `PortAsserts`, `TableTestSupport`, `DataflowTestRandom` (.NET seeded `System.Random`), `runtime/StatAggregateBridge`.
