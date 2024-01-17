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

    private final MemorySegment memorySegment;

    private final SurroundConsumer<MemorySegments.LineSegment> publisher;

    private final boolean allocating;

    private final MutableLine segmentLine;

    private final int partitionNo;

    private final boolean firstPartition;

    private final boolean lastPartition;

    private final int trail;

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

    public BitwisePartitionSpliterator(
        Partition partition,
        Shape shape,
        MemorySegment memorySegment
    ) {
        this(
            partition,
            shape,
            memorySegment,
            false
        );
    }

    public BitwisePartitionSpliterator(
        Partition partition,
        Shape shape,
        MemorySegment memorySegment,
        boolean allocating
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);

        if (!(partition.last() || partition.isAligned(BYTES_IN_LONG))) {
            throw new IllegalArgumentException(
                STR."Not a valid partition, should be aligned @\{BYTES_IN_LONG}: \{partition}"
            );
        }
        this.partition = Objects.requireNonNull(partition, "partition");

        this.trail = Math.toIntExact(this.partition.count() % partition.alignment());
        this.partitionLimit = this.partition.count() - this.trail;
        this.partitionNo = this.partition.partitionNo();
        this.firstPartition = this.partition.first();
        this.lastPartition = this.partition.last();
        this.memorySegment = Objects.requireNonNull(memorySegment, "memorySegmentSources");

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
        current = memorySegment.get(ValueLayout.JAVA_LONG, 0);
        if (!firstPartition) {
            jumpToLine();
        }
        if (partition.last() && trail > 0) {
            boolean trailing = false;
            while (true) {
                if (ln()) {
                    long length = shipLine(action);
                    if (lastDone()) {
                        return false;
                    }
                    newLine(length);
                }
                trailing = trailing | lastAdvance();
                if (trailing && lastDone()) {
                    return false;
                }
            }
        } else {
            while (true) {
                if (ln()) {
                    long length = shipLine(action);
                    if (done()) {
                        return false;
                    }
                    newLine(length);
                }
                advance();
            }
        }
    }

    private long shipLine(Consumer<? super MemorySegments.LineSegment> action) {
        long length = byteOffset - previousLineStartByte;
        MemorySegments.LineSegment lineSegment = lineSegment(length);
        publisher.accept(action, lineSegment);
        return length;
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
            current = memorySegment.get(ValueLayout.JAVA_LONG, byteOffset);
        } else {
            current >>= 8;
        }
    }

    private boolean lastAdvance() {
        byteOffset++;
        if (byteOffset == partitionLimit) {
            current = 0L;
            for (int i = trail - 1; i >= 0; i--) {
                current = (current << 8) + (long) memorySegment.get(ValueLayout.JAVA_BYTE, byteOffset + i);
            }
            return true;
        }
        if (byteOffset % 8 == 0) {
            current = memorySegment.get(ValueLayout.JAVA_LONG, byteOffset);
        } else {
            current >>= 8;
        }
        return false;
    }

    private boolean ln() {
        return (current & '\n') == '\n';
    }

    private boolean done() {
        return byteOffset > partitionLimit;
    }

    private boolean lastDone() {
        return byteOffset == partitionLimit + trail;
    }

    private void newLine(long length) {
        previousLineStartByte += length + 1;
        lineNo++;
    }

    private MemorySegments.LineSegment lineSegment(long length) {
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

    public static final long BYTES_IN_LONG = ValueLayout.JAVA_LONG.byteSize();

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
