package io.streamsforge.engine.runtime.ops;

import io.streamsforge.engine.runtime.WorkingRow;

/**
 * The internal wire type between table-mode ops once a delta has been admitted (see {@link TableIngestOp}): a
 * {@link WorkingRow} (alias-qualified, join/WHERE/GROUP-BY-ready) paired with its Z-set weight. DELIBERATELY not the
 * public {@code TableDelta} for interior edges — join ON clauses and GROUP BY keys are bound against alias-qualified
 * columns, which only WorkingRow carries.
 */
public record TableRowDelta(WorkingRow row, long weight) {}
