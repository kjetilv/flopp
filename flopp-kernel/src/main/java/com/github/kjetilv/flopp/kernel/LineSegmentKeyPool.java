package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.BitwiseLongSupplier;

import java.util.HashMap;
import java.util.Map;

public final class LineSegmentKeyPool {

    private final Map<Long, LineSegmentKey> pool = new HashMap<>();

    private BitwiseLongSupplier.Reusable reusable = BitwiseLongSupplier.create();

    public LineSegmentKey resolve(LineSegment lineSegment) {
        reusable = reusable.apply(lineSegment);
        long hash = LineSegments.hashCode(lineSegment, reusable);
        return pool.computeIfAbsent(hash, _ -> LineSegmentKey.create(lineSegment, hash));
    }
}
