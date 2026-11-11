using StreamsForge.Engine.Dataflow;
using StreamsForge.Engine.Sql;

namespace StreamsForge.Engine.Runtime.Ops;

/// <summary>
/// Plan 004 N2's table-mode semi/anti join: `expr [NOT] IN (SELECT ...)` / `[NOT] EXISTS (SELECT ...)`,
/// rewritten by Planner into a Semi (IN/EXISTS) or Anti (NOT IN/NOT EXISTS) <see cref="JoinKind"/> stage.
/// Unlike <see cref="TableJoinOp"/>'s bilinear combine-and-multiply-weight join, this is PRESENCE-gated
/// pass-through of the LEFT (outer) row: a B-side (subquery) row never appears in the output, and B-side
/// row MULTIPLICITY never fans out A-side rows — "duplicates in B must not duplicate A's rows: presence,
/// not join fan-out" (plan 004 N2). What matters is only whether a key's accumulated weight on the B side
/// is currently &gt; 0 ("present") — and, when a key's presence FLIPS (0 → positive or positive → 0), every
/// currently-indexed A-row under that key must be retracted/re-asserted to reflect the flip, even though no
/// NEW A-row arrived (plan 004 N2: "when B's last row for key k retracts, all matching A rows must
/// retract; when first row for k asserts, they assert").
///
/// EXISTS/NOT EXISTS reuses this SAME machinery with a trick at the Planner level, not here: both
/// <see cref="_leftKey"/> and <see cref="_rightKey"/> are compiled as the SAME constant expression (see
/// Planner.BuildSemiAntiJoin), collapsing every row on both sides into one global bucket — "is there any B
/// row at all" becomes "is the constant key present", the exact same presence test IN uses per-key.
///
/// NULL semantics follow PostgreSQL's three-valued logic (a WHERE conjunct keeps a row only when TRUE):
/// <list type="bullet">
/// <item>IN (Semi): <c>x IN (S)</c> is TRUE only on a match; a NULL in S or a NULL x can only make it
/// NULL, never TRUE, so NULL-keyed rows on either side simply never pass.</item>
/// <item>NOT IN (Anti): <c>x NOT IN (S)</c> is TRUE when S is empty (even for a NULL x); otherwise FALSE
/// on a match and NULL (filtered) when x is NULL or S contains a NULL. So one NULL in the subquery result
/// filters every outer row that did not match, and its retraction brings them back.</item>
/// </list>
/// To flip those rows incrementally, NULL-keyed rows are tracked on both sides (<see cref="LeftNulls"/>,
/// <see cref="RightNulls"/>) and the number of present non-NULL B keys is counted, so "S is empty" and "S
/// contains a NULL" are O(1). (Before 2026-11-11 this op ignored NULLs in S — a deliberate plan 004
/// deviation that made <c>'MSFT' NOT IN ('AAPL', NULL)</c> pass.) The B side reaches every partition by
/// Broadcast, so each partition's copy of this global state is complete.
///
/// STATE: <see cref="Left"/> (A-side rows, keyed by their evaluated LeftKey — used to find "which A rows
/// need re-emitting" when a B-side key's presence flips) and <see cref="Right"/> (B-side rows, keyed by
/// their evaluated RightKey — used purely to track each key's accumulated weight/presence; the actual ROW
/// contents on this side are never read, only whether the bucket is non-empty). Both are
/// <see cref="ZSetIndex"/>, exactly TableJoinOp's state shape — this op differs in what it DOES with that
/// state (presence tracking + flip-triggered re-emission), not in the state's shape.
/// </summary>
internal sealed class TableSemiAntiOp(JoinKind kind, Expr leftKey, Expr rightKey, IReadOnlyDictionary<Expr, (string Alias, string Field)> bindings) : ITableJoinStage
{
    private readonly bool _isAnti = kind == JoinKind.Anti;

    /// <summary>A-side (outer row) state: join-key → (row, weight) — indexed so a B-side presence flip can
    /// find every currently-live A-row under that key to re-emit.</summary>
    public ZSetIndex Left { get; } = new();

    /// <summary>B-side (subquery row) state: join-key → (row, weight) — only ever consulted via
    /// <c>.Lookup(key).Any()</c> (is this key currently present with positive weight); the row payload
    /// itself is never read back out.</summary>
    public ZSetIndex Right { get; } = new();

    /// <summary>A-side rows whose key is NULL (single bucket). Only Anti needs them: they pass exactly
    /// while the B side is empty.</summary>
    public ZSetIndex LeftNulls { get; } = new();

    /// <summary>B-side rows whose key is NULL (single bucket) — for Anti, "S contains a NULL".</summary>
    public ZSetIndex RightNulls { get; } = new();

    private const string NullBucket = "";
    private int _presentRightKeys;

    private bool RightHasNull => RightNulls.Lookup(NullBucket).Any();
    private bool RightIsEmpty => _presentRightKeys == 0 && !RightHasNull;

    private bool PassesKeyed(bool present, bool rightHasNull) => _isAnti ? !present && !rightHasNull : present;
    private bool PassesNullKey(bool rightIsEmpty) => _isAnti && rightIsEmpty;

    public IReadOnlyList<TableRowDelta> OnLeftBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input)
    {
        var results = new List<TableRowDelta>();
        foreach (var d in input)
        {
            var keyVal = ExpressionEvaluator.Eval(leftKey, new EvalContext(d.Row, bindings));
            var rowCanonical = JsonText.SerializeCanonicalRow(d.Row.Fields);
            bool passes;
            if (keyVal is null)
            {
                if (!_isAnti) continue; // NULL IN (...) is never TRUE: nothing to remember
                LeftNulls.Apply(NullBucket, rowCanonical, d.Row, d.Weight);
                passes = PassesNullKey(RightIsEmpty);
            }
            else
            {
                var keyStr = TableKeyEncoding.EncodeScalar(keyVal);
                Left.Apply(keyStr, rowCanonical, d.Row, d.Weight);
                passes = PassesKeyed(Right.Lookup(keyStr).Any(), RightHasNull);
            }
            if (passes) results.Add(new TableRowDelta(d.Row, d.Weight));
        }
        return results;
    }

    public IReadOnlyList<TableRowDelta> OnRightBatch(Epoch epoch, IReadOnlyList<TableRowDelta> input)
    {
        var results = new List<TableRowDelta>();
        foreach (var d in input)
        {
            var keyVal = ExpressionEvaluator.Eval(rightKey, new EvalContext(d.Row, bindings));
            var rowCanonical = JsonText.SerializeCanonicalRow(d.Row.Fields);
            bool hadNull = RightHasNull, wasEmpty = RightIsEmpty;

            if (keyVal is null)
            {
                if (!_isAnti) continue; // a NULL in S never makes IN TRUE
                RightNulls.Apply(NullBucket, rowCanonical, d.Row, d.Weight);
                if (RightHasNull != hadNull)
                {
                    // Every keyed A-row whose key is absent from S flips at once.
                    foreach (var k in Left.Keys)
                    {
                        bool present = Right.Lookup(k).Any();
                        EmitFlip(results, Left.Lookup(k), PassesKeyed(present, hadNull), PassesKeyed(present, !hadNull));
                    }
                }
            }
            else
            {
                var keyStr = TableKeyEncoding.EncodeScalar(keyVal);
                bool wasPresent = Right.Lookup(keyStr).Any();
                Right.Apply(keyStr, rowCanonical, d.Row, d.Weight);
                bool isPresentNow = Right.Lookup(keyStr).Any();
                if (wasPresent != isPresentNow)
                {
                    _presentRightKeys += isPresentNow ? 1 : -1;
                    EmitFlip(results, Left.Lookup(keyStr), PassesKeyed(wasPresent, hadNull), PassesKeyed(isPresentNow, hadNull));
                }
            }

            if (_isAnti && RightIsEmpty != wasEmpty)
            {
                EmitFlip(results, LeftNulls.Lookup(NullBucket), PassesNullKey(wasEmpty), PassesNullKey(!wasEmpty));
            }
        }
        return results;
    }

    /// <summary>Re-emits <paramref name="rows"/> when their pass/fail verdict flipped: +weight when they now
    /// pass, −weight when they no longer do.</summary>
    private static void EmitFlip(List<TableRowDelta> results, IEnumerable<(WorkingRow Row, long Weight)> rows, bool passedBefore, bool passesNow)
    {
        if (passedBefore == passesNow) return;
        int sign = passesNow ? 1 : -1;
        foreach (var (row, weight) in rows) results.Add(new TableRowDelta(row, sign * weight));
    }

    /// <summary>Pass-through — see TableJoinOp's identical OnFrontier doc: table mode has no epoch-driven
    /// eviction; this op's state (like TableJoinOp's) is unbounded and consolidated by design.</summary>
    public IReadOnlyList<TableDelta> OnFrontier(Epoch epoch) => [];
}
