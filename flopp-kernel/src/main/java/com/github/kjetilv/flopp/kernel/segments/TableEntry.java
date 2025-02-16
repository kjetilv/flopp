package com.github.kjetilv.flopp.kernel.segments;

import com.github.kjetilv.flopp.kernel.LineSegment;

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

    @Override
    public int compareTo(TableEntry<T> other) {
        return segment.compareTo(other.segment());
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

    TableEntry<T> freezeTo(LineSegment segment, long offset) {
        return frozen(this.segment.plus(segment, offset));
    }

    private TableEntry<T> frozen(LineSegment copied) {
        return new TableEntry<>(copied, hash(), value, cachedString);
    }
}
