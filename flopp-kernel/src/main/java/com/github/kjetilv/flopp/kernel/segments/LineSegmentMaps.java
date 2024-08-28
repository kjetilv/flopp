package com.github.kjetilv.flopp.kernel.segments;

public final class LineSegmentMaps {

    public static <T> LineSegmentMap<T> create(int size) {
        return create(size, null);
    }

    public static <T> LineSegmentMap<T> create(int size, LineSegmentTraverser.Reusable reusable) {
        return new LineSegmentHashtable<>(size, reusable);
    }

    private LineSegmentMaps() {
    }
}
