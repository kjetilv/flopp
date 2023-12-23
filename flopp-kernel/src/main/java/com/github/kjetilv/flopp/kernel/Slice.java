package com.github.kjetilv.flopp.kernel;

/**
 *
 * @param offset Start position of slice
 * @param length Lenght of slice
 * @param total Total available
 */
record Slice(long offset, long length, long total) {

    /**
     * First slice.
     *
     * @param length Length of slice
     * @param total Total available
     * @return Slice of {@link #offset} 0
     */
    static Slice first(long length, long total) {
        return new Slice(0, Math.min(length, total), total);
    }

    Slice {
        Non.negative(offset, "offset");
        Non.negative(length, "length");
        Non.negative(total, "total");
    }

    public boolean done() {
        return length() == 0;
    }

    boolean last() {
        return next().done();
    }

    /**
     * Adjusts the known {@link #total)}.
     *
     * @param newTotal New total
     * @return New slice with updated {@link #total)}
     */
    Slice withTotal(long newTotal) {
        return new Slice(offset, length, newTotal);
    }

    /**
     * The next slice following this
     *
     * @return Next slice
     */
    Slice next() {
        return next(total);
    }

    Slice next(long total) {
        long nextOffset = offset + length;
        long nextLength = Math.min(total - nextOffset, length);
        return new Slice(nextOffset, nextLength, total);
    }
}
