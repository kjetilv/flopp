package com.github.kjetilv.flopp.kernel;

public interface Range {

    static Range of(long start, long end) {
        return new Immutable(start, end);
    }

    long startIndex();

    long endIndex();

    default long length() {
        return endIndex() - startIndex();
    }

    record Immutable(long startIndex, long endIndex) implements Range {

        public Immutable {
            Non.negative(startIndex, "startIndex");
            Non.negative(endIndex, "endIndex");
            if (endIndex < startIndex) {
                throw new IllegalStateException("Range: " + startIndex + " + >= " + endIndex);
            }
        }
    }
}
