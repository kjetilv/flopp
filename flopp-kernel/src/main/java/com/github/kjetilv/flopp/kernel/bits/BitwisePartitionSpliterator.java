package com.github.kjetilv.flopp.kernel.bits;

import com.github.kjetilv.flopp.kernel.Mediator;
import com.github.kjetilv.flopp.kernel.Partition;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;
import java.util.Spliterators;
import java.util.function.Consumer;

@SuppressWarnings("preview")
public class BitwisePartitionSpliterator
    extends Spliterators.AbstractSpliterator<LineSegment> {

    private final Partition partition;

    private final long partitionLimit;

    private final MutableLine ml = new MutableLine();

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

    private final Mediator<LineSegment> mediator;

    public BitwisePartitionSpliterator(
        Partition partition,
        MemorySegment memorySegment,
        Mediator<LineSegment> mediator
    ) {
        super(Long.MAX_VALUE, IMMUTABLE | SIZED);

        if (!(partition.last() || partition.isAligned())) {
            throw new IllegalArgumentException(
                STR."Not a valid partition, should be aligned @\{BYTES_IN_LONG}: \{partition}"
            );
        }
        this.partition = Objects.requireNonNull(partition, "partition");
        this.ml.memorySegment = memorySegment.asReadOnly();
        this.mediator = mediator;

        this.trail = Math.toIntExact(this.partition.count() % partition.alignment());
        this.partitionLimit = this.partition.count() - this.trail;
        this.lastLimit = partitionLimit + trail;

        this.firstPartition = this.partition.first();
        this.lastPartition = this.partition.last();
    }

    @Override
    public boolean tryAdvance(Consumer<? super LineSegment> action) {
        try {
            return process(mediate(action));
        } catch (Exception e) {
            throw new IllegalStateException(STR."\{this} failed to process", e);
        }
    }

    @Override
    public String toString() {
        return STR."\{getClass().getSimpleName()}[offset:\{byteOffset} in \{partition}]";
    }

    @SuppressWarnings("unchecked")
    private Consumer<LineSegment> mediate(Consumer<? super LineSegment> action) {
        if (mediator == null) {
            return (Consumer<LineSegment>) action;
        }
        return (Consumer<LineSegment>) mediator.apply(action);
    }

    @SuppressWarnings({"StatementWithEmptyBody", "SameReturnValue"})
    private boolean process(Consumer<LineSegment> action) {
        current = next();
        if (!firstPartition) {
            jumpToLine();
        }
        if (lastPartition) {
            for (long i = 0; i < partitionLimit / BYTES_IN_LONG && !cycleDone(action); i++) ;
            if (processedToEnd(action)) {
                return false;
            }
            while (true) {
                if (cycleDone(action)) {
                    return false;
                }
            }
        }
        while (true) {
            if (cycleDone(action)) {
                return false;
            }
        }
    }

    private boolean cycleDone(Consumer<LineSegment> action) {
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

    private boolean processedToEnd(Consumer<LineSegment> action) {
        while (true) {
            if (ln()) {
                shipLine(action);
                if (byteOffset == lastLimit) {
                    return true;
                }
            }
            byteOffset++;
            if (tailRemains()) {
                return false;
            }
            advanceCurrent();
        }
    }

    private void shipLine(Consumer<LineSegment> action) {
        long length = byteOffset - previousLineStartByte;
        ml.offset = previousLineStartByte;
        ml.length = length;
        action.accept(ml);
        previousLineStartByte += length + 1;
    }

    private void jumpToLine() {
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

    private boolean tailRemains() {
        if (byteOffset == partitionLimit) {
            current = 0L;
            for (int i = trail - 1; i >= 0; i--) {
                current = (current << 8) + ml.memorySegment().get(ValueLayout.JAVA_BYTE, byteOffset + i);
            }
            return true;
        }
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
            return ml.memorySegment().get(ValueLayout.JAVA_LONG, byteOffset);
        } catch (Exception e) {
            throw new IllegalStateException(
                STR."\{this} failed to advance from \{byteOffset} in \{ml.memorySegment()}",
                e
            );
        }
    }

    private boolean ln() {
        return (current & 0xFF) == '\n';
    }

    public static final long BYTES_IN_LONG = ValueLayout.JAVA_LONG.byteSize();
}
