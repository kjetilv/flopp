package com.github.kjetilv.lopp;

import com.github.kjetilv.lopp.utils.Non;

record Slice(int offset, int length, int total) {

    public static Slice first(long sliceSize, long total) {
        return new Slice(0, Math.min(sliceSize, total), total);
    }

    Slice(long offset, long length, long total) {
        this(Math.toIntExact(offset), Math.toIntExact(length), Math.toIntExact(total));
    }

    Slice {
        Non.negative(offset, "offset");
        Non.negativeOrZero(length, "length");
        Non.negativeOrZero(total, "total");
    }

    public Slice next() {
        int nextOffset = offset + length;
        if (nextOffset == total) {
            return null;
        }
        int nextLength = Math.min(total - nextOffset, length);
        return new Slice(nextOffset, nextLength, total);
    }

}
