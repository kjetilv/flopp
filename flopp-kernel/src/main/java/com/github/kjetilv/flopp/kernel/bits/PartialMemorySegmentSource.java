package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.LineSegment;
import com.github.kjetilv.flopp.kernel.LineSegments;
import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.formats.Shape;

import java.nio.file.Path;

final class PartialMemorySegmentSource extends AbstractFileChannelMemorySegmentSource {

    PartialMemorySegmentSource(Path path, Shape shape) {
        super(path, shape);
    }

    @Override
    protected LineSegment lineSegment(Partition partition, long length) {
        return LineSegments.of(
            memorySegment(partition.offset(), length),
            0L,
            length
        );
    }
}
