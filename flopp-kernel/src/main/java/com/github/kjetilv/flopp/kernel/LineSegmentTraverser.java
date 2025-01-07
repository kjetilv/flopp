package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.segments.AbstractLineSegmentTraverser;
import com.github.kjetilv.flopp.kernel.segments.AbstractLineSegmentTraverser.AlignedTraverser;
import com.github.kjetilv.flopp.kernel.segments.AbstractLineSegmentTraverser.MultiModeSuppler;

import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static com.github.kjetilv.flopp.kernel.LineSegments.nextHash;

@SuppressWarnings("unused")
public interface LineSegmentTraverser extends Function<LineSegment, LineSegmentTraverser.Reusable> {

    static Reusable create() {
        return new MultiModeSuppler().initial();
    }

    static Reusable createAligned() {
        return new AlignedTraverser().initial();
    }

    static Reusable create(boolean align) {
        AbstractLineSegmentTraverser traverser = align
            ? new AlignedTraverser()
            : new MultiModeSuppler();
        return traverser.initial();
    }

    static Reusable create(LineSegment segment) {
        return create(segment, false);
    }

    static Reusable create(LineSegment segment, boolean aligned) {
        return (aligned
            ? new AlignedTraverser()
            : new MultiModeSuppler()
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
