package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class MemorySegments {

    public static Stream<String> lineStrings(Partition partition, Shape shape, MemorySegment memorySegment) {
        return lineBytes(partition, shape, memorySegment).map(String::new);
    }

    public static Stream<byte[]> lineBytes(Partition partition, Shape shape, MemorySegment memorySegment) {
        return lineSegments(partition, shape, memorySegment)
            .map(MemorySegments::toBytes);
    }

    public static Stream<LineSegment> lineSegments(Partition partition, Shape shape, MemorySegment memorySegment) {
        return StreamSupport.stream(
            new VectorPartitionSpliterator(partition, shape, () ->
                new MemorySegmentSource.Segment(memorySegment, 0)),
            false
        );
    }

    public static String toString(LineSegment line) {
        return new String(toBytes(line));
    }

    public static byte[] toBytes(LineSegment line) {
        return line.memorySegment()
            .asSlice(line.offset(), line.length())
            .toArray(ValueLayout.JAVA_BYTE);

    }

    private MemorySegments() {
    }

    public sealed interface LineSegment permits MutableLine, VectorPartitionSpliterator.Line {

        int partitionNo();

        long lineNo();

        MemorySegment memorySegment();

        long offset();

        int length();

        default byte[] asBytes() {
            return toBytes(this);
        }

        default LineSegment validate() {
            if (length() < 0) {
                throw new IllegalStateException(STR."\{this} has negative length");
            }
            return this;
        }
    }
}
