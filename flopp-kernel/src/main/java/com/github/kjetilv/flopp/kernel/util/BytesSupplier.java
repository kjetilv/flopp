package com.github.kjetilv.flopp.kernel.util;

import com.github.kjetilv.flopp.kernel.segments.MemorySegments;

import java.util.Objects;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

@SuppressWarnings("unused")
public final class BytesSupplier implements IntSupplier {

    private final LongSupplier longSupplier;

    private int index;

    private long data;

    public BytesSupplier(LongSupplier longSupplier) {
        this.longSupplier = Objects.requireNonNull(longSupplier, "longSupplier");
        this.data = this.longSupplier.getAsLong();
    }

    @Override
    public int getAsInt() {
        try {
            return Bits.getByte(data, index);
        } finally {
            index++;
            if (index == MemorySegments.ALIGNMENT_INT) {
                data = longSupplier.getAsLong();
                index = 0;
            }
        }
    }
}
