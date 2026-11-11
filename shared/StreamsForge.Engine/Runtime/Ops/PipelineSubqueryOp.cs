using StreamsForge.Engine.Sql;

namespace StreamsForge.Engine.Runtime.Ops;

/// <summary>
/// Plan 004 N2/N3/N4's pipeline-mode subquery join stage — semi/anti membership (`[NOT] IN`/`[NOT] EXISTS`,
/// <see cref="JoinKind.Semi"/>/<see cref="JoinKind.Anti"/>) and scalar value lookup (uncorrelated N3 /
/// equality-correlated N4, <see cref="JoinKind.Scalar"/>) share ONE mechanism here, per the task's "same
/// rolling-snapshot rule ... one shared subquery materialization helper if that falls out naturally": both
/// need "the current state of a windowed subquery's most recent emission, indexed by key, tested against at
/// arrival" — membership doesn't care about the row payload (only whether a key's bucket is non-empty),
/// value lookup does (it returns the matched row's fields) — everything else (batch-replace snapshot
/// timing, key-NULL handling) is identical.
///
/// THE ROLLING-SNAPSHOT RULE (plan 004 N2/N3/N4's pipeline "honesty rule", exactly as shipped): the
/// subquery side MUST be a windowed derived query (Validator enforces this — IsTransitivelyWindowed).
/// Its nested nested executor (wired exactly like plan 004 N1's derived-source nesting) is driven by
/// ExecutorImpl the same way any derived role is; the difference is what happens to what it emits — see
/// <see cref="OnRightBatch"/>: instead of feeding each emitted row through the ordinary per-row join path,
/// ExecutorImpl collects the WHOLE emission batch from one OnEvent/AdvanceWatermark call and hands it here
/// as one unit, which WHOLESALE REPLACES this op's snapshot (never merges/accumulates across batches) — "a
/// rolling snapshot replaced on each inner emission batch". An EMPTY batch (the normal steady state between
/// window closes) leaves the snapshot untouched — it keeps serving the most recently closed window's
/// membership/value until the next non-empty batch replaces it. A side-A row is tested/evaluated against
/// WHATEVER snapshot is live AT ITS OWN ARRIVAL (<see cref="OnLeft"/>) — no buffering, no waiting for a
/// "matching" snapshot batch; this is a deliberate, documented approximation (not exactness against some
/// global order) appropriate for a best-effort streaming semi-join/scalar-lookup, not a correctness
/// contract about which specific window a given A-row logically "belongs" to.
///
/// STATE: <see cref="_snapshot"/> — join-key string → the inner query's currently-live emitted row(s) under
/// that key (a list only because a composite N4 correlation key hashes on its FIRST component alone, with
/// any additional components checked via <see cref="_residual"/> at lookup time — see
/// Planner.BuildScalarJoin's doc comment; in every other case it holds at most one entry). Wholesale-
/// replaced, never incrementally mutated, by <see cref="OnRightBatch"/>.
/// </summary>
internal sealed class PipelineSubqueryOp : IPipelineSnapshotJoinStage
{
    private readonly JoinKind _kind; // Semi, Anti, or Scalar
    private readonly Expr _leftKey;
    private readonly Expr _rightKey;
    private readonly Expr? _residual;
    private readonly IReadOnlyDictionary<Expr, (string Alias, string Field)> _bindings;
    private readonly WorkingRow _nullRight;

    private Dictionary<string, List<WorkingRow>> _snapshot = [];

    public PipelineSubqueryOp(
        JoinKind kind, Expr leftKey, Expr rightKey, Expr? residual,
        IReadOnlyDictionary<Expr, (string Alias, string Field)> bindings, (string Alias, SourceSchema Schema) rightSide)
    {
        _kind = kind;
        _leftKey = leftKey;
        _rightKey = rightKey;
        _residual = residual;
        _bindings = bindings;
        _nullRight = WorkingRow.NullSide([rightSide]);
    }

    /// <summary>Whether the current snapshot holds a row whose key is NULL — for NOT IN, PostgreSQL's
    /// "S contains a NULL", which makes <c>x NOT IN (S)</c> NULL (filtered) for every x that did not match.</summary>
    private bool _hasNull;

    /// <summary>Whether the current snapshot holds any row at all (NULL-keyed included) — <c>x NOT IN (S)</c>
    /// is TRUE for an empty S even when x is NULL.</summary>
    private bool _nonEmpty;

    /// <summary>Replaces the whole snapshot from one inner-executor emission batch — see class doc. A
    /// B-row whose key evaluates to NULL is not indexed (it can never match), but it is remembered in
    /// <see cref="_hasNull"/>: NOT IN follows the same PostgreSQL three-valued rule TableSemiAntiOp
    /// documents. For Scalar, a NULL correlation key can never legitimately match an outer row.</summary>
    public void OnRightBatch(IReadOnlyList<WorkingRow> rows)
    {
        if (rows.Count == 0) return; // rolling: an empty batch leaves the prior snapshot live

        var next = new Dictionary<string, List<WorkingRow>>();
        bool hasNull = false;
        foreach (var row in rows)
        {
            var keyVal = ExpressionEvaluator.Eval(_rightKey, new EvalContext(row, _bindings));
            if (keyVal is null) { hasNull = true; continue; }
            var keyStr = TableKeyEncoding.EncodeScalar(keyVal);
            if (!next.TryGetValue(keyStr, out var list)) { list = []; next[keyStr] = list; }
            list.Add(row);
        }
        _snapshot = next;
        _hasNull = hasNull;
        _nonEmpty = true; // rows.Count > 0 here
    }

    public List<WorkingRow> OnLeft(WorkingRow row)
    {
        var keyVal = ExpressionEvaluator.Eval(_leftKey, new EvalContext(row, _bindings));
        if (keyVal is null)
        {
            // Semi: NULL IN (S) is never TRUE (dropped). Anti: NULL NOT IN (S) is TRUE only for an empty S.
            // Scalar: outer correlation key NULL — value is undefined, which IS a legitimate NULL scalar
            // result (not a row filter) — null-pad and keep the row, matching SQL's "scalar subquery with
            // no matching group evaluates to NULL" rule.
            if (_kind == JoinKind.Scalar) return [WorkingRow.Combine(row, _nullRight)];
            return _kind == JoinKind.Anti && !_nonEmpty ? [row] : [];
        }

        var keyStr = TableKeyEncoding.EncodeScalar(keyVal);
        bool present = _snapshot.TryGetValue(keyStr, out var candidates) && candidates.Count > 0;

        if (_kind == JoinKind.Scalar)
        {
            if (present)
            {
                foreach (var candidate in candidates!)
                {
                    var combined = WorkingRow.Combine(row, candidate);
                    if (_residual is null || ExpressionEvaluator.IsTrue(ExpressionEvaluator.Eval(_residual, new EvalContext(combined, _bindings))))
                    {
                        return [combined];
                    }
                }
            }
            return [WorkingRow.Combine(row, _nullRight)];
        }

        // Semi: passes when present. Anti: passes when absent AND S holds no NULL (else the result is NULL).
        bool passes = _kind == JoinKind.Anti ? !present && !_hasNull : present;
        return passes ? [row] : [];
    }

    /// <summary>Never invoked under ExecutorImpl's normal dispatch for a snapshot stage (a derived
    /// subquery role routes its WHOLE emission batch through <see cref="OnRightBatch"/> instead — see
    /// RoleEntry.IsSnapshotJoin) — implemented correctly anyway (as a length-1 batch) so the op is safe to
    /// use even if a caller ever drives it per-row.</summary>
    public List<WorkingRow> OnRight(WorkingRow row)
    {
        OnRightBatch([row]);
        return [];
    }

    /// <summary>No-op — see class doc: there is no left-side buffering to evict (a snapshot stage tests
    /// immediately at arrival, never waits for a match), and the snapshot itself only ever changes via
    /// OnRightBatch, driven by the nested subquery executor's OWN watermark advance (ExecutorImpl drives
    /// that separately — see AdvanceWatermarkCore's _derivedNodes loop).</summary>
    public List<WorkingRow> Evict(long watermark) => [];
}
