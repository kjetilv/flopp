package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

public class BitwisePartitionSpliterator
    extends Spliterators.AbstractSpliterator<MemorySegments.LineSegment> {

    private final Partition partition;

    private final long partitionLimit;

    private final MemorySegment ms;

    private final MutableLine ml;

    private final boolean firstPartition;

    private final boolean lastPartition;

    private final int trail;

    private final long lastLimit;

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

    public BitwisePartitionSpliterator(Partition partition, MemorySegment ms) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);

        if (!(partition.last() || partition.isAligned(BYTES_IN_LONG))) {
            throw new IllegalArgumentException(
                STR."Not a valid partition, should be aligned @\{BYTES_IN_LONG}: \{partition}"
            );
        }
        this.partition = Objects.requireNonNull(partition, "partition");

        this.trail = Math.toIntExact(this.partition.count() % partition.alignment());
        this.partitionLimit = this.partition.count() - this.trail;
        this.lastLimit = partitionLimit + trail;

        this.firstPartition = this.partition.first();
        this.lastPartition = this.partition.last();

        this.ms = Objects.requireNonNull(ms, "memorySegmentSources");
        this.ml = new MutableLine();
        this.ml.partitionNo = partition.partitionNo();
        this.ml.memorySegment = ms;
    }

    @Override
    public boolean tryAdvance(Consumer<? super MemorySegments.LineSegment> action) {
        try {
            return process(action);
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to process", e);
        }
    }

    private boolean process(Consumer<? super MemorySegments.LineSegment> action) {
        current = next();
        if (!firstPartition) {
            jumpToLine();
        }
        if (lastPartition) {
            tailSensitive(action);
        } else {
            fullSpeed(action);
        }
        return false;
    }

    private void fullSpeed(Consumer<? super MemorySegments.LineSegment> action) {
        while (true) {
            if (ln()) {
                shipLine(action);
                if (byteOffset >= partitionLimit) {
                    return;
                }
            }
            advance();
        }
    }

    @SuppressWarnings("SameReturnValue")
    private void tailSensitive(Consumer<? super MemorySegments.LineSegment> action) {
        if (processToEnd(action)) {
            return;
        }
        while (true) {
            if (ln()) {
                shipLine(action);
                return;
            }
            advance();
        }
    }

    private boolean processToEnd(Consumer<? super MemorySegments.LineSegment> action) {
        while (true) {
            if (ln()) {
                shipLine(action);
                if (byteOffset == lastLimit) {
                    return true;
                }
            }
            if (foundTail()) {
                return false;
            }
        }
    }

    private void shipLine(Consumer<? super MemorySegments.LineSegment> action) {
        long length = byteOffset - previousLineStartByte;
        ml.offset = previousLineStartByte;
        ml.length = length;
        action.accept(ml);
        previousLineStartByte += length + 1;
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
        advanceCurrent();
    }

    private boolean foundTail() {
        byteOffset++;
        if (byteOffset == partitionLimit) {
            current = 0L;
            for (int i = trail - 1; i >= 0; i--) {
                current = (current << 8) + ms.get(ValueLayout.JAVA_BYTE, byteOffset + i);
            }
            return true;
        }
        advanceCurrent();
        return false;
    }

    private void advanceCurrent() {
        if (byteOffset % 8 == 0) {
            current = next();
        } else {
            current >>= 8;
        }
    }

    private long next() {
        try {
            return ms.get(ValueLayout.JAVA_LONG, byteOffset);
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to advance from \{byteOffset} in \{ms}", e);
        }
    }

    private boolean ln() {
        return (current & 0xFF) == '\n';
    }

    public static final long BYTES_IN_LONG = ValueLayout.JAVA_LONG.byteSize();

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[offset:\{byteOffset} in \{partition}]";
    }
}
