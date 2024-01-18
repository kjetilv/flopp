package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.MemorySegments;
import jdk.incubator.vector.VectorMask;

import java.lang.foreign.MemorySegment;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

public class VectorPartitionSpliterator
    extends Spliterators.AbstractSpliterator<MemorySegments.LineSegment> {

    private final Partition partition;

    private final long partitionLimit;

    private final MemorySegmentSource source;

    private final SurroundConsumer<MemorySegments.LineSegment> lineConsumer;

    private final boolean allocating;

    private final MutableLine segmentLine;

    private final int partitionNo;

    private final boolean lastPartition;

    private final boolean firstPartition;

    public VectorPartitionSpliterator(Partition partition, Shape shape, MemorySegmentSource source) {
        this(partition, shape, source, false);
    }

    public VectorPartitionSpliterator(
        Partition partition,
        Shape shape,
        MemorySegmentSource source,
        boolean allocating
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);

        this.partition = Objects.requireNonNull(partition, "partition");
        this.partitionLimit = this.partition.count();
        this.partitionNo = partition.partitionNo();

        this.firstPartition = partition.first();
        this.lastPartition = partition.last();

        this.source = Objects.requireNonNull(source, "memorySegmentSources");

        this.allocating = allocating || shape != null && shape.footer() > 0;
        if (this.allocating) {
            this.segmentLine = null;
        } else {
            this.segmentLine = new MutableLine();
            this.segmentLine.partitionNo = partition.partitionNo();
        }
        this.lineConsumer = SurroundConsumers.surround(
            this.partition.first() && shape != null && shape.header() > 0 ? shape.header() : 0,
            this.partition.last() && shape != null && shape.footer() > 0 ? shape.footer() : 0
        );
    }

    @Override
    public boolean tryAdvance(Consumer<? super MemorySegments.LineSegment> action) {
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

    private boolean process(Consumer<? super MemorySegments.LineSegment> action) {
        MemorySegmentSource.Segment segment = source.get();
        long segmentOffset = firstLineOffset(segment);
        long lineMarker = segmentOffset;

        int pending = 0;

        long lines = 0;
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
                    MemorySegments.LineSegment lineSegment = lineSegment(segment, lineMarker, length, lines);
                    try {
                        MemorySegments.LineSegment validated = lineSegment.validate();
                        lineConsumer.accept(action, validated);
                    } catch (Exception e) {
                        throw new IllegalStateException(
                            STR."\{this} failed to ship \{lineSegment} to \{lineConsumer}", e);
                    }
                    lines++;
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

    private MemorySegments.LineSegment lineSegment(
        MemorySegmentSource.Segment segment, long offset, int length, long lineNo
    ) {
        if (allocating) {
            return new Line(
                partitionNo,
                lineNo,
                segment.memorySegment(),
                offset,
                length
            );
        }
        segmentLine.memorySegment = segment.memorySegment();
        segmentLine.lineNo = lineNo;
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

    record Line(
        int partitionNo,
        long lineNo,
        MemorySegment memorySegment,
        long offset,
        int length
    ) implements MemorySegments.LineSegment {

        @Override
        public String toString() {
            return STR."\{getClass().getSimpleName()}[\{lineNo()}/\{partitionNo()}: \{offset()}+\{length()}]";
        }
    }

    @SuppressWarnings("PackageVisibleField")
    public static final class MutableLine implements MemorySegments.LineSegment {

        int partitionNo;

        long lineNo;

        MemorySegment memorySegment;

        long offset;

        int length;

        @Override
        public int partitionNo() {
            return partitionNo;
        }

        @Override
        public long lineNo() {
            return lineNo;
        }

        @Override
        public MemorySegment memorySegment() {
            return memorySegment;
        }

        @Override
        public long offset() {
            return offset;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public String toString() {
            return STR."\{getClass().getSimpleName()}[\{lineNo()}/\{partitionNo()}: \{offset()}+\{length()}]";
        }
    }}
