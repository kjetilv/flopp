package com.github.kjetilv.flopp.kernel.io;

import java.util.Spliterators;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static com.github.kjetilv.flopp.kernel.segments.MemorySegments.ALIGNMENT_POW;

public final class SuppliedLongSpliterator extends Spliterators.AbstractLongSpliterator {

    private final long length;

    private final LongSupplier longSupplier;

    public SuppliedLongSpliterator(LongSupplier supplier, long length) {
        super(length >> ALIGNMENT_POW + 2, IMMUTABLE | ORDERED);
        this.length = length;
        longSupplier = supplier;
    }

    @Override
    public boolean tryAdvance(LongConsumer action) {
        for (int i = 0; i < length; i++) {
            action.accept(longSupplier.getAsLong());
        }
        return false;
    }
}
