package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;
import com.github.kjetilv.flopp.kernel.bits.BitwiseTraverser;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;

public record LineSegmentKey(int hash, long[] data, int length)
    implements Comparable<LineSegmentKey>,
    Supplier<String>,
    Function<Charset, String> {

    public static Function<LineSegment, LineSegmentKey> factory() {
        BitwiseTraverser.Reusable reusable = BitwiseTraverser.create();
        return segment -> {
            BitwiseTraverser.Reusable current = reusable.apply(segment);
            return create(segment, current, (int) current.size());
        };
    }

    public static LineSegmentKey create(LineSegment lineSegment) {
        BitwiseTraverser.Reusable current = BitwiseTraverser.create(lineSegment);
        return create(lineSegment, current, (int) current.size());
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LineSegmentKey lineSegmentKey && Arrays.equals(data, lineSegmentKey.data);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        return new String(Bits.toBytes(data, length), UTF_8);
    }

    @Override
    public int compareTo(LineSegmentKey o) {
        return toString().compareTo(o.toString());
    }

    @Override
    public String get() {
        return apply(UTF_8);
    }

    @Override
    public String apply(Charset charset) {
        return new String(Bits.toBytes(data, length), charset);
    }

    private static LineSegmentKey create(
        LineSegment segment,
        LongSupplier current,
        int count
    ) {
        long[] data = new long[count];
        int hash = 0;
        for (int i = 0; i < count; i++) {
            long l = current.getAsLong();
            hash += 31 * hash + Long.hashCode(l);
            data[i] = l;
        }
        return new LineSegmentKey(hash, data, (int) segment.length());
    }
}
