package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;
import com.github.kjetilv.flopp.kernel.Shape;
import com.github.kjetilv.flopp.kernel.SurroundConsumer;
import com.github.kjetilv.flopp.kernel.SurroundConsumers;

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

    public BitwisePartitionSpliterator(Partition partition, Shape shape, MemorySegment memorySegment) {
        this(partition, shape, memorySegment, false);
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
        return STR."\{getClass().getSimpleName()}[\{lineNo}, offset \{byteOffset} in \{partition}]";
    }

    private boolean process(Consumer<? super MemorySegments.LineSegment> action) {
        current = next();
        if (!firstPartition) {
            jumpToLine();
        }
        if (lastPartition) {
            tailSensitiveProcessing(action);
        } else {
            normalProcessing(action);
        }
        return false;
    }

    private void normalProcessing(Consumer<? super MemorySegments.LineSegment> action) {
        while (true) {
            if (ln()) {
                long length = shipLine(action);
                if (done()) {
                    return;
                }
                newLine(length);
            }
            advance();
        }
    }

    @SuppressWarnings("SameReturnValue")
    private void tailSensitiveProcessing(Consumer<? super MemorySegments.LineSegment> action) {
        boolean trailing = false;
        while (true) {
            if (ln()) {
                long length = shipLine(action);
                if (lastDone()) {
                    return;
                }
                newLine(length);
            }
            trailing = trailing | advanceLast();
            if (trailing && lastDone()) {
                return;
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
        slideCurrent();
    }

    private boolean advanceLast() {
        byteOffset++;
        if (byteOffset == partitionLimit) {
            current = 0L;
            for (int i = trail - 1; i >= 0; i--) {
                current = (current << 8) + (long) memorySegment.get(ValueLayout.JAVA_BYTE, byteOffset + i);
            }
            return true;
        }
        slideCurrent();
        return false;
    }

    private void slideCurrent() {
        if (byteOffset % 8 == 0) {
            current = next();
        } else {
            current >>= 8;
        }
    }

    private long next() {
        try {
            return memorySegment.get(ValueLayout.JAVA_LONG, byteOffset);
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to advance from \{byteOffset} in \{memorySegment}", e);
        }
    }

    private boolean ln() {
        return (current & 0xFF) == '\n';
    }

    private boolean done() {
        return byteOffset >= partitionLimit;
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
