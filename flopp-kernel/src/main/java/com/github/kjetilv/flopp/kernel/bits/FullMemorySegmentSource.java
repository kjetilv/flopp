package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

final class FullMemorySegmentSource extends AbstractFileChannelMemorySegmentSource {

    private final MemorySegment segment;

    FullMemorySegmentSource(Path path, Shape shape) {
        super(path, shape);
        try {
            segment = memorySegment(0L, shape.size());
        } catch (Exception e) {
            throw new IllegalStateException(this + " could not open " + path, e);
        }
    }

    @Override
    protected LineSegment lineSegment(Partition partition, long length) {
        return LineSegments.of(segment, partition.offset(), partition.offset() + length);
    }
}
