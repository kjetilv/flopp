package com.github.kjetilv.flopp.kernel;

/**
 * @param offset Start position of slice
 * @param length Length of slice
 */
public record Slice(long offset, int length) {

    /**
     * First slice.
     *
     * @param length Length of slice
     * @param total  Total available
     * @return Slice of {@link #offset} 0
     */
    public static Slice first(long length, long total) {
        return new Slice(0, Math.toIntExact(Math.min(length, total)));
    }

    public Slice {
        Non.negative(offset, "offset");
        Non.negative(length, "length");
    }

    boolean last(long limit) {
        return limit <= offset + length;
    }

    Slice next(long limit) {
        long nextOffset = offset + length;
        int nextLength = Math.toIntExact(Math.min(limit - (offset + length), length));
        return new Slice(nextOffset, nextLength);
    }
}
