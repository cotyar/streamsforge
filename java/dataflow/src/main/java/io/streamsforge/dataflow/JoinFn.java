package io.streamsforge.dataflow;

@FunctionalInterface
public interface JoinFn<K, V1, V2, R> {
    R apply(K key, V1 left, V2 right);
}
