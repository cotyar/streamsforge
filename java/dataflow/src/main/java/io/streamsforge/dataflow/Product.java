package io.streamsforge.dataflow;

/**
 * The product partial order: {@code (a,b) <= (c,d)} iff {@code a <= c} and {@code b <= d}. Used for nested
 * iterative scopes, with {@code outer} the enclosing scope's time and {@code inner} the iteration counter.
 */
public record Product<O extends Time<O>, I extends Time<I>>(O outer, I inner) implements Time<Product<O, I>> {
    @Override public boolean lessEqual(Product<O, I> o) { return outer.lessEqual(o.outer) && inner.lessEqual(o.inner); }
    @Override public Product<O, I> join(Product<O, I> o) { return new Product<>(outer.join(o.outer), inner.join(o.inner)); }
    @Override public Product<O, I> meet(Product<O, I> o) { return new Product<>(outer.meet(o.outer), inner.meet(o.inner)); }
    /** Lexicographic: a linear extension of the product order. */
    @Override public int compareTo(Product<O, I> o) {
        int c = outer.compareTo(o.outer);
        return c != 0 ? c : inner.compareTo(o.inner);
    }
    @Override public String toString() { return "(" + outer + "," + inner + ")"; }
}
