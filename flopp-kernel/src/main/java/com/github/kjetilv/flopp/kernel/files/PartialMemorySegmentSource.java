package com.github.kjetilv.flopp.kernel.files;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

final class PartialMemorySegmentSource extends AbstractFileChannelMemorySegmentSource {

    PartialMemorySegmentSource(Path path, Shape shape) {
        super(path, shape);
    }

    @Override
    protected LineSegment lineSegment(Partition partition, long length) {
        MemorySegment memorySegment = memorySegment(partition.offset(), length);
        return LineSegments.of(memorySegment, 0L, length);
    }
}
