package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

public class BitwisePartitionSpliterator
    extends Spliterators.AbstractSpliterator<MemorySegments.LineSegment> {

    private final Partition partition;

    private final int partitionLimit;

    private final MemorySegmentSource source;

    private final SurroundConsumer<MemorySegments.LineSegment> lineConsumer;

    private final boolean allocating;

    private final MutableLine segmentLine;

    private final int partitionNo;

    private final boolean lastPartition;

    private final boolean firstPartition;

    private MemorySegmentSource.Segment segment;

    private long limit;

    /**
     * Current byte.
     */
    private long current;

    /**
     * How much of {@link #current} is processed
     */
    private long localByteOffset;

    /**
     * How much of {@link #partitionLimit} is processed
     */
    private long byteOffset;

    /**
     * Position of byte starting current line
     */
    private long previousLineStartByte;

    /**
     * Line no of upcoming line, 0-indexed
     */
    private long lineNo;

    public BitwisePartitionSpliterator(Partition partition, Shape shape, MemorySegmentSource source) {
        this(partition, shape, source, false);
    }

    public BitwisePartitionSpliterator(
        Partition partition,
        Shape shape,
        MemorySegmentSource source,
        boolean allocating
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);

        if (!(partition.last() || partition.isAligned(BYTES_IN_LONG))) {
            throw new IllegalArgumentException(
                STR."Not a valid partition, should be aligned @\{BYTES_IN_LONG}: \{partition}"
            );
        }

        this.partition = Objects.requireNonNull(partition, "partition");
        this.partitionLimit = this.partition.count();
        this.partitionNo = partition.partitionNo();

        this.firstPartition = partition.first();
        this.lastPartition = partition.last();

        this.source = Objects.requireNonNull(source, "memorySegmentSources");

        this.byteOffset = partition.offset();

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
        segment = source.get();
        limit = segment.limit(ValueLayout.JAVA_LONG);
        long lineNo = 0;
        if (!firstPartition) {
            jumpToLineByte(segment);
        }
        while (byteOffset < limit) {
            if (ln()) {
                long length = byteOffset - previousLineStartByte;
                MemorySegments.LineSegment lineSegment = lineSegment(
                    segment,
                    previousLineStartByte,
                    length,
                    lineNo
                );
                action.accept(lineSegment);
                previousLineStartByte += length + 1;
                lineNo++;
                if (done()) {
                    return false;
                }
            }
            advance();
        }
        return true;
    }

    private boolean done() {
        return byteOffset > partitionLimit;
    }

    private boolean ln() {
        return (current & '\n') == '\n';
    }

    private void jumpToLineByte(MemorySegmentSource.Segment segment) {
        current = segment.longAt(0);
        localByteOffset = 0;
        while (true) {
            for (int i = 0; i < 16; i++) {
                if (ln()) {
                    previousLineStartByte = byteOffset + 1;
                    advance();
                    return;
                }
                advance();
            }
        }
    }

    private void advance() {
        byteOffset++;
        if (localByteOffset == 7) {
            localByteOffset = 0L;
            current = segment.longAt(byteOffset);
        } else {
            localByteOffset++;
            current >>= 8;
        }
    }

    private MemorySegments.LineSegment lineSegment(
        MemorySegmentSource.Segment segment, long offset, long length, long lineNo
    ) {
        if (allocating) {
            return new Line(
                partitionNo,
                lineNo,
                segment.memorySegment(),
                offset,
                Math.toIntExact(length)
            );
        }
        segmentLine.memorySegment = segment.memorySegment();
        segmentLine.lineNo = lineNo;
        segmentLine.offset = offset;
        segmentLine.length = Math.toIntExact(length);
        return segmentLine;
    }

    public static final long BYTES_IN_LONG = ValueLayout.JAVA_LONG.byteSize() * 2;

    record Line(
        int partitionNo,
        long lineNo,
        MemorySegment memorySegment,
        long offset,
        int length
    ) implements MemorySegments.LineSegment {

        @Override
        public String toString() {
            return STR."\{getClass().getSimpleName()}[\{lineNo()}/\{partitionNo()}: \{offset()}-\{length()}]";
        }
    }
}
