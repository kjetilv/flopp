package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

@SuppressWarnings("ProtectedField")
abstract sealed class AbstractBitwisePartitionSpliterator
    extends Spliterators.AbstractSpliterator<MemorySegments.LineSegment>
    permits BitwiseAlignedPartitionSpliterator, BitwiseTrailingPartitionSpliterator {

    protected final Partition partition;

    protected final MemorySegment ms;

    protected final MutableLine ml;

    protected long partitionLimit;

    /**
     * Current byte.
     */
    protected long current;

    /**
     * How much of {@link #partitionLimit} is processed
     */
    protected long byteOffset;

    /**
     * Position of byte starting current line
     */
    private long previousLineStartByte;

    AbstractBitwisePartitionSpliterator(Partition partition, MemorySegment ms) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);
        this.ms = Objects.requireNonNull(ms, "memorySegmentSources");

        this.partition = Objects.requireNonNull(partition, "partition");

        this.ml = new MutableLine();
        this.ml.partitionNo = partition.partitionNo();
        this.ml.memorySegment = ms;
    }

    @Override
    public final boolean tryAdvance(Consumer<? super MemorySegments.LineSegment> action) {
        try {
            return advance(action);
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to process", e);
        }
    }

    abstract boolean advance(Consumer<? super MemorySegments.LineSegment> action);

    final boolean cycleDone(Consumer<? super MemorySegments.LineSegment> action) {
        if (ln()) {
            shipLine(action);
            if (byteOffset >= partitionLimit) {
                return true;
            }
        }
        byteOffset++;
        advanceCurrent();
        return false;
    }

    final void shipLine(Consumer<? super MemorySegments.LineSegment> action) {
        ml.offset = previousLineStartByte;
        ml.length = byteOffset - previousLineStartByte;
        action.accept(ml);
        previousLineStartByte += ml.length + 1;
    }

    final void advanceCurrent() {
        if (byteOffset % 8 == 0) {
            current = next();
        } else {
            current >>= 8;
        }
    }

    final boolean ln() {
        return (current & 0xFF) == '\n';
    }

    final void jumpToLine() {
        while (true) {
            byteOffset++;
            if (ln()) {
                advanceCurrent();
                previousLineStartByte = byteOffset;
                return;
            }
            advanceCurrent();
        }
    }

    final long next() {
        try {
            return ms.get(ValueLayout.JAVA_LONG, byteOffset);
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to advance from \{byteOffset} in \{ms}", e);
        }
    }

    protected void processToTail(Consumer<? super MemorySegments.LineSegment> action, int tail) {
        while (true) {
            if (ln()) {
                shipLine(action);
            }
            byteOffset++;
            if (byteOffset == partitionLimit) {
                loadTail(tail);
                return;
            }
            advanceCurrent();
        }
    }

    private void loadTail(int tail) {
        current = 0L;
        for (int i = tail - 1; i >= 0; i--) {
            current = (current << 8) + ms.get(ValueLayout.JAVA_BYTE, byteOffset + i);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[offset:\{byteOffset} in \{partition}]";
    }
}
