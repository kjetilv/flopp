package com.github.kjetilv.flopp.kernel;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

public class BitwisePartitionSpliterator
    extends Spliterators.AbstractSpliterator<MemorySegments.LineSegment> {

    private final Partition partition;

    private final long partitionLimit;

    private final MemorySegmentSource source;

    private final SurroundConsumer<MemorySegments.LineSegment> publisher;

    private final boolean allocating;

    private final MutableLine segmentLine;

    private final int partitionNo;

    private final boolean firstPartition;

    private MemorySegmentSource.Segment segment;

    /**
     * Current byte.
     */
    private long current;

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
        this.partitionLimit = this.partition.offset() + this.partition.count();
        this.partitionNo = partition.partitionNo();

        this.firstPartition = partition.first();
        boolean lastPartition = partition.last();

        this.source = Objects.requireNonNull(source, "memorySegmentSources");

        this.byteOffset = partition.offset();

        this.allocating = allocating || shape != null && shape.footer() > 0;
        if (this.allocating) {
            this.segmentLine = null;
        } else {
            this.segmentLine = new MutableLine();
            this.segmentLine.partitionNo = partition.partitionNo();
        }
        this.publisher = SurroundConsumers.surround(
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
        current = segment.longAt(0);
        if (!firstPartition) {
            jumpToLine();
        }
        while (true) {
            if (ln()) {
                long length = byteOffset - previousLineStartByte;
                publisher.accept(action, lineSegment(length));
                if (done()) {
                    return false;
                }
                newLine(length);
            }
            advance();
        }
    }

    private void jumpToLine() {
        while (true) {
            if (ln()) {
                advance();
                previousLineStartByte = byteOffset;
                return;
            }
            advance();
        }
    }

    private void advance() {
        byteOffset++;
        if (byteOffset % 8 == 0) {
            current = segment.longAt(byteOffset);
        } else {
            current >>= 8;
        }
    }

    private boolean ln() {
        return (current & '\n') == '\n';
    }

    private boolean done() {
        return byteOffset > partitionLimit;
    }

    private void newLine(long length) {
        previousLineStartByte += length + 1;
        lineNo++;
    }

    private MemorySegments.LineSegment lineSegment(long length) {
        MemorySegment memorySegment = segment.memorySegment();
        if (allocating) {
            return new Line(
                partitionNo,
                lineNo,
                memorySegment,
                previousLineStartByte,
                Math.toIntExact(length)
            );
        }
        segmentLine.memorySegment = memorySegment;
        segmentLine.lineNo = lineNo;
        segmentLine.offset = previousLineStartByte;
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
            return STR."\{getClass().getSimpleName()}[\{lineNo()}/\{partitionNo()}: \{offset()}+\{length()}]";
        }
    }
}
