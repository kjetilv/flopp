package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class MemorySegments {

    public static Stream<Line> lines(Partition partition, MemorySegment memorySegment) {
        return StreamSupport.stream(
            new MemorySegmentPartitionSpliterator(partition, memorySegment),
            false
        );
    }

    private MemorySegments() {
    }

    public interface Line {

        MemorySegment memorySegment();

        long offset();

        int length();

    }
}
