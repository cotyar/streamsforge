package io.streamsforge.engine.runtime;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A map whose ITERATION ORDER reproduces .NET's {@code Dictionary<TKey,TValue>} (and {@code HashSet<T>}):
 * entries live in an index-addressed array, enumeration walks the array by index, and a removed slot goes on a
 * LIFO free list that the NEXT insertion reuses — so "remove A, add B" enumerates B where A used to be, which a
 * {@link java.util.LinkedHashMap} (B last) does not do. Several operators emit rows in enumeration order
 * (window close, group re-emission, join-bucket probes), so the order is part of observable behavior.
 * Null keys are not supported. Not thread-safe.
 */
public final class NetDictionary<K, V> extends AbstractMap<K, V> {
    private final HashMap<K, Integer> index = new HashMap<>();
    private final ArrayList<K> keys = new ArrayList<>();
    private final ArrayList<V> vals = new ArrayList<>();
    private final ArrayList<Boolean> alive = new ArrayList<>();
    private final ArrayList<Integer> free = new ArrayList<>(); // used as a stack

    public NetDictionary() {}

    @Override public int size() { return index.size(); }

    @Override public boolean containsKey(Object key) { return index.containsKey(key); }

    @Override public V get(Object key) {
        Integer i = index.get(key);
        return i == null ? null : vals.get(i);
    }

    @Override public V put(K key, V value) {
        Integer i = index.get(key);
        if (i != null) {
            return vals.set(i, value);
        }
        int slot;
        if (!free.isEmpty()) {
            slot = free.remove(free.size() - 1);
            keys.set(slot, key);
            vals.set(slot, value);
            alive.set(slot, true);
        } else {
            slot = keys.size();
            keys.add(key);
            vals.add(value);
            alive.add(true);
        }
        index.put(key, slot);
        return null;
    }

    @Override public V remove(Object key) {
        Integer i = index.remove(key);
        if (i == null) return null;
        V old = vals.get(i);
        keys.set(i, null);
        vals.set(i, null);
        alive.set(i, false);
        free.add(i);
        return old;
    }

    @Override public void clear() {
        index.clear();
        keys.clear();
        vals.clear();
        alive.clear();
        free.clear();
    }

    @Override public Set<Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            @Override public int size() { return index.size(); }

            @Override public Iterator<Entry<K, V>> iterator() {
                return new Iterator<>() {
                    int next = advance(0);
                    int last = -1;

                    private int advance(int from) {
                        int i = from;
                        while (i < keys.size() && !alive.get(i)) i++;
                        return i;
                    }

                    @Override public boolean hasNext() { return next < keys.size(); }

                    @Override public Entry<K, V> next() {
                        if (!hasNext()) throw new NoSuchElementException();
                        last = next;
                        next = advance(next + 1);
                        final int slot = last;
                        return new Entry<>() {
                            @Override public K getKey() { return keys.get(slot); }
                            @Override public V getValue() { return vals.get(slot); }
                            @Override public V setValue(V v) { return vals.set(slot, v); }
                        };
                    }

                    @Override public void remove() {
                        if (last < 0) throw new IllegalStateException();
                        NetDictionary.this.remove(keys.get(last));
                        last = -1;
                    }
                };
            }
        };
    }
}
