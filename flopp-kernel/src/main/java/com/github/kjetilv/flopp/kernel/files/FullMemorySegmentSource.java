package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

@SuppressWarnings("unused")
final class FullMemorySegmentSource extends AbstractFileChannelMemorySegmentSource {

    private final MemorySegment segment;

    FullMemorySegmentSource(Path path, Shape shape) {
        super(path, shape);
        this.segment = memorySegment(0L, shape.size());
    }

    @Override
    protected LineSegment lineSegment(Partition partition, long length) {
        long offset = partition.offset();
        return LineSegments.of(segment, offset, offset + length);
    }
}
