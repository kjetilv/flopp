package com.github.kjetilv.flopp.kernel.segments;

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static com.github.kjetilv.flopp.kernel.segments.LineSegments.nextHash;

public interface LineSegmentTraverser extends Function<LineSegment, LineSegmentTraverser.Reusable> {

    static Reusable create() {
        return new AbstractLineSegmentTraverser.MultiModeSuppler().blank();
    }

    static Reusable createAligned() {
        return new AbstractLineSegmentTraverser.AlignedTraverser().blank();
    }

    static Reusable create(boolean align) {
        return (align
            ? new AbstractLineSegmentTraverser.AlignedTraverser()
            : new AbstractLineSegmentTraverser.MultiModeSuppler()).blank();
    }

    static Reusable create(LineSegment segment) {
        return create(segment, false);
    }

    static Reusable create(LineSegment segment, boolean align) {
        return (align
            ? new AbstractLineSegmentTraverser.AlignedTraverser()
            : new AbstractLineSegmentTraverser.MultiModeSuppler()
        ).apply(segment);
    }

    sealed interface Reusable extends LongSupplier, Function<LineSegment, Reusable>
        permits AbstractLineSegmentTraverser.ReusableBase {

        default int toHashCode(LineSegment lineSegment) {
            return apply(lineSegment).toHashCode();
        }

        default int toHashCode() {
            int hash = 17;
            long size = size();
            for (int i = 0; i < size; i++) {
                hash = nextHash(hash, getAsLong());
            }
            return hash;
        }

        long size();

        void forEach(IndexedLongConsumer consumer);
    }

    @FunctionalInterface
    interface IndexedLongConsumer extends LongConsumer {

        default void accept(long value) {
            accept(-1, value);
        }

        void accept(int index, long value);
    }
}
