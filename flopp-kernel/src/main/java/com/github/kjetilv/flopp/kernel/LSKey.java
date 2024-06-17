package com.github.kjetilv.flopp.kernel;

import com.github.kjetilv.flopp.kernel.bits.Bits;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

public record LSKey(int hash, long[] data, int length) implements Comparable<LSKey> {

    public static LSKey create(LineSegment lineSegment) {
        long[] data = LineSegments.asLongs(lineSegment);
        int hash = Arrays.hashCode(data);
        return new LSKey(hash, data, (int) lineSegment.length());
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LSKey lsKey && Arrays.equals(lsKey.data, data);
    }

    @Override
    public int hashCode() {
        return hash;
    }

    @Override
    public String toString() {
        byte[] bytes = Bits.toBytes(data, length);
        String s = new String(bytes, UTF_8);
        return s;
    }

    @Override
    public int compareTo(LSKey o) {
        return toString().compareTo(o.toString());
    }
}
