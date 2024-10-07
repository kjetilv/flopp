package com.github.kjetilv.flopp.kernel;

public interface Range {

    static Range of(long start, long end) {
        return new ImmutableRange(start, end);
    }

    long startIndex();

    long endIndex();

    default long length() {
        return endIndex() - startIndex();
    }

    default boolean aligned(int alignment) {
        return length() % alignment == 0;
    }

    record ImmutableRange(long startIndex, long endIndex)
        implements Range {
    }
}
