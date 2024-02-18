package com.github.kjetilv.flopp.kernel.bits;

import java.lang.foreign.MemorySegment;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public interface Line {

    MemorySegment memorySegment();

    int columns();

    long[] start();

    long[] end();

    default Stream<String> columnStream() {
        return IntStream.range(0, columns()).mapToObj(this::column);
    }

    default Stream<LineSegment> segmentStream() {
        return IntStream.range(0, columns()).mapToObj(this::segment);
    }

    default String column(int column) {
        return segment(column).asString();
    }

    default LineSegment segment(int column) {
        return LineSegment.of(memorySegment(), start()[column], end()[column]);
    }
}
