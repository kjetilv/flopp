package com.github.kjetilv.flopp.kernel.util;

import com.github.kjetilv.flopp.kernel.bits.BitwiseTraverser;

public final class LineSegmentMaps {

    public static <T> LineSegmentMap<T> create(int size) {
        return create(size, null);
    }

    public static <T> LineSegmentMap<T> create(int size, BitwiseTraverser.Reusable reusable) {
        return new LineSegmentHashtable<>(size, reusable);
    }

    private LineSegmentMaps() {
    }
}
