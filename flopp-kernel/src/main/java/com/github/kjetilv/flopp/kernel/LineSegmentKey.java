package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;
import com.github.kjetilv.flopp.kernel.bits.BitwiseTraverser;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static com.github.kjetilv.flopp.kernel.LineSegments.nextHash;
import static java.nio.charset.StandardCharsets.UTF_8;

public record LineSegmentKey(int hash, long[] data, int length)
    implements Comparable<LineSegmentKey>,
    Supplier<String>,
    Function<Charset, String> {

    public static Function<LineSegment, LineSegmentKey> factory() {
        BitwiseTraverser.Reusable reusable = BitwiseTraverser.create();
        return segment -> {
            BitwiseTraverser.Reusable current = reusable.reset(segment);
            return create(current, (int) current.size(), (int) segment.length());
        };
    }

    public static Function<LineSegment, LineSegmentKey> pool() {
        Map<Integer, LineSegmentKey> pool = new HashMap<>();
        BitwiseTraverser.Reusable reusable = BitwiseTraverser.create();
        return segment ->
            pool.computeIfAbsent(reusable.reset(segment).toHashCode(), hash -> {
                BitwiseTraverser.Reusable reset = reusable.reset(segment);
                int count = (int) reset.size();
                long[] buffer = reset.fill(new long[count]);
                return new LineSegmentKey(hash, buffer, (int) segment.length());
            });
    }

    public static LineSegmentKey create(LineSegment lineSegment) {
        BitwiseTraverser.Reusable current = BitwiseTraverser.create(lineSegment);
        return create(current, (int) current.size(), (int) lineSegment.length());
    }

    public LineSegmentKey(long hash, long[] data, int length) {
        this(Long.hashCode(hash), data, length);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LineSegmentKey key && Arrays.equals(data, key.data);
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
    public int compareTo(LineSegmentKey other) {
        return Arrays.compare(data, other.data);
    }

    @Override
    public String get() {
        return apply(UTF_8);
    }

    @Override
    public String apply(Charset charset) {
        return new String(Bits.toBytes(data, length), charset);
    }

    private static LineSegmentKey create(LongSupplier current, int count, int length) {
        long[] data = new long[count];
        int hash = 17;
        for (int i = 0; i < count; i++) {
            long l = current.getAsLong();
            hash = nextHash(hash, l);
            data[i] = l;
        }
        return new LineSegmentKey(hash, data, length);
    }
}
