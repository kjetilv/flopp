package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;

public record LineSegmentKey(int hash, long[] data, int length)
    implements Comparable<LineSegmentKey>,
    Supplier<String>,
    Function<Charset, String> {

    public static LineSegmentKey create(LineSegment lineSegment) {
        long[] data = LineSegments.asLongs(lineSegment);
        int hash = Arrays.hashCode(data);
        return new LineSegmentKey(hash, data, (int) lineSegment.length());
    }

    public static LineSegmentKey create(LineSegment lineSegment, long hash) {
        long[] data = LineSegments.asLongs(lineSegment);
        return new LineSegmentKey((int) hash, data, (int) lineSegment.length());
    }

    public static LineSegmentKey create(LineSegment lineSegment, long[] buffer, long hash) {
        long[] data = LineSegments.asLongs(lineSegment, buffer);
        return new LineSegmentKey((int) hash, data, (int) lineSegment.length());
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
}
