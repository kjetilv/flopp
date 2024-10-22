package com.github.kjetilv.flopp.kernel.segments;

public interface Range {

    static Range of(long start, long end) {
        return new ImmutableRange(start, end);
    }

    default long length() {
        return endIndex() - startIndex();
    }

    default boolean aligned(int alignment) {
        return length() % alignment == 0;
    }

    long startIndex();

    long endIndex();

    record ImmutableRange(long startIndex, long endIndex)
        implements Range {
    }
}
