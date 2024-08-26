package com.github.kjetilv.flopp.kernel.segments;

public interface Range {

    static Range of(long start, long end) {
        return new Immutable(start, end);
    }

    long startIndex();

    long endIndex();

    default long length() {
        return endIndex() - startIndex();
    }

    default boolean aligned(int alignment) {
        return length() % alignment == 0;
    }

    record Immutable(long startIndex, long endIndex) implements Range {
    }
}
