package com.github.kjetilv.flopp.kernel;

record Slice(int offset, int length, int total) {

    static Slice first(long size, long total) {
        return new Slice(0, Math.min(size, total), total);
    }

    Slice(long offset, long length, long total) {
        this(Math.toIntExact(offset), Math.toIntExact(length), Math.toIntExact(total));
    }

    Slice {
        Non.negative(offset, "offset");
        Non.negative(length, "length");
        Non.negative(total, "total");
    }

    Slice bump(long total) {
        return new Slice(offset, length, total);
    }

    Slice next() {
        int nextOffset = offset + length;
        int nextLength = Math.min(total - nextOffset, length);
        return new Slice(nextOffset, nextLength, total);
    }
}
