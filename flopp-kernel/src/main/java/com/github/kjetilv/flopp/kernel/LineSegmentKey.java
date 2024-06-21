package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

public record LineSegmentKey(int hash, long[] data, int length) implements Comparable<LineSegmentKey> {

    public static LineSegmentKey create(LineSegment lineSegment) {
        long[] data = LineSegments.asLongs(lineSegment);
        int hash = Arrays.hashCode(data);
        return new LineSegmentKey(hash, data, (int) lineSegment.length());
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
}
