package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.LineSegment;
import jdk.incubator.vector.VectorMask;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

public class VectorPartitionSpliterator
    extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final Mediator<LineSegment> mediator;

    private final long partitionLimit;

    private final MemorySegmentSource source;

    private final MutableLine segmentLine = new MutableLine();

    private final boolean lastPartition;

    private final boolean firstPartition;

    public VectorPartitionSpliterator(
        Partition partition,
        MemorySegmentSource source,
        Mediator<LineSegment> mediator
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);

        this.partition = Objects.requireNonNull(partition, "partition");
        this.mediator = mediator;
        this.partitionLimit = this.partition.count();

        this.firstPartition = partition.first();
        this.lastPartition = partition.last();

        this.source = Objects.requireNonNull(source, "memorySegmentSources");
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        try {
            return process(action);
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to process", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[\{partition}]";
    }

    private boolean process(Consumer<? super LineSegment> action) {
        Consumer<LineSegment> consumer = mediate(action);
        MemorySegmentSource.Segment segment = source.get();
        long segmentOffset = firstLineOffset(segment);
        long lineMarker = segmentOffset;

        int pending = 0;

        while (true) {
            int backshift = 0;
            VectorMask<Byte> mask;
            try {
                if (segmentOffset > segment.maxReadOffset()) {
                    backshift = Math.toIntExact(segmentOffset - segment.maxReadOffset());
                }
                mask = segment.lineMask(segmentOffset - backshift);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            long maskLong = mask.toLong();
            int maskOffset = backshift;
            while (true) {
                int zeroes = maskOffset - backshift + Long.numberOfTrailingZeros(maskLong >>> maskOffset);
                if (zeroes < BYTES_IN_LONG) {
                    int length = pending + zeroes - maskOffset + backshift;
                    LineSegment lineSegment = lineSegment(segment, lineMarker, length);
                    try {
                        LineSegment validated = lineSegment.validate();
                        consumer.accept(validated);
                    } catch (Exception e) {
                        throw new IllegalStateException(
                            STR."\{this} failed to ship \{lineSegment} to \{action}", e);
                    }
                    int advance = length + 1;
                    lineMarker += advance;
                    if (exhausted(segment, lineMarker)) {
                        return false;
                    }
                    maskOffset += advance - pending;
                    pending = 0;
                } else {
                    int length = mask.length();
                    segmentOffset += length - backshift;
                    pending += length - maskOffset;
                    break;
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Consumer<LineSegment> mediate(Consumer<? super LineSegment> action) {
        if (mediator == null) {
            return (Consumer<LineSegment>) action;
        }
        return (Consumer<LineSegment>) mediator.apply(action);
    }

    private long firstLineOffset(MemorySegmentSource.Segment segment) {
        if (firstPartition) {
            return 0;
        }
        long offset = 0;
        while (true) {
            VectorMask<Byte> mask = segment.lineMask(offset);
            int zeroes = trailingZeroes(segment, mask);
            if (zeroes < Long.BYTES) {
                return offset + zeroes + 1 + segment.shift();
            } else {
                offset += Long.BYTES;
            }
        }
    }

    private boolean exhausted(MemorySegmentSource.Segment segment, long offset) {
        long segmentOffset = offset - segment.shift();
        if (segmentOffset == partitionLimit) {
            return lastPartition;
        }
        return segmentOffset >= partitionLimit;
    }

    private LineSegment lineSegment(
        MemorySegmentSource.Segment segment, long offset, int length
    ) {
        segmentLine.memorySegment = segment.memorySegment();
        segmentLine.offset = offset;
        segmentLine.length = length;
        return segmentLine;
    }

    public static final int BYTES_IN_LONG = Long.BYTES * 8;

    private static int trailingZeroes(
        MemorySegmentSource.Segment segment,
        VectorMask<Byte> mask
    ) {
        return Long.numberOfTrailingZeros(mask.toLong() >>> segment.shift());
    }

    private static final class MutableLine implements LineSegment {

        MemorySegment memorySegment;

        long offset;

        long length;

        @Override
        public MemorySegment memorySegment() {
            return memorySegment;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public String toString() {
            return STR."\{getClass().getSimpleName()}[\{offset()}+\{length()}]";
        }
    }
}
