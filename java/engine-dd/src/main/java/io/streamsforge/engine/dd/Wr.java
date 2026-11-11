package io.streamsforge.engine.dd;

import io.streamsforge.engine.runtime.WorkingRow;

/** A {@link WorkingRow} (the alias-qualified row the engine's expression evaluator reads) with value equality, so it
 *  can live in a dataflow collection. Equality = timestamp + all fields. */
final class Wr {
    final WorkingRow row;
    private int hash;

    Wr(WorkingRow row) { this.row = row; }

    @Override public boolean equals(Object o) {
        return o instanceof Wr w && w.row.ts() == row.ts() && w.row.fields().equals(row.fields());
    }

    @Override public int hashCode() {
        int h = hash;
        if (h == 0) {
            h = 31 * Long.hashCode(row.ts()) + row.fields().hashCode();
            if (h == 0) h = 1;
            hash = h;
        }
        return h;
    }
}
