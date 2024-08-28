package com.github.kjetilv.flopp.kernel.segments;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

record TableEntry<T>(
    LineSegment segment,
    int hash,
    T value,
    AtomicReference<String> cachedString
)
    implements Comparable<TableEntry<T>> {

    @SuppressWarnings("unused")
    TableEntry(LineSegment segment, int hash, T value) {
        this(
            Objects.requireNonNull(segment, "segment"),
            hash,
            Objects.requireNonNull(value, "value"),
            new AtomicReference<>()
        );
    }

    TableEntry<T> merge(T value) {
        return new TableEntry<>(segment, hash, value, cachedString);
    }

    String segmentString() {
        return cachedString.updateAndGet(key -> key == null ? segment().asString() : key);
    }

    boolean matches(LineSegment segment, int hash) {
        return hash == this.hash && this.segment.matches(segment);
    }

    @Override
    public int compareTo(TableEntry<T> other) {
        return segment.compareTo(other.segment());
    }
}
