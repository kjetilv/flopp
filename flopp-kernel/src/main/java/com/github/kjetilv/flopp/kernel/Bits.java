package com.github.kjetilv.flopp.kernel;

import java.nio.charset.Charset;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class Bits {

    public static String toString(long l) {
        return toString(l, -1, null);
    }

    public static String toString(long l, Charset charset) {
        return toString(l, -1, charset);
    }

    public static String toString(long l, int len) {
        return toString(l, len, null);
    }

    public static String toString(long l, int len, Charset charset) {
        Charset cs = charset == null ? UTF_8 : charset;
        return len < 0
            ? new String(toBytes(l), cs)
            : new String(toBytes(l), 0, len, cs);
    }

    public static byte[] toBytes(long l) {
        return new byte[] {
            (byte) (l & 0xFF),
            (byte) (l >> 8L & 0xFF),
            (byte) (l >> 16L & 0xFF),
            (byte) (l >> 24L & 0xFF),
            (byte) (l >> 32L & 0xFF),
            (byte) (l >> 40L & 0xFF),
            (byte) (l >> 48L & 0xFF),
            (byte) (l >> 56L & 0xFF)
        };
    }

    /**
     * @param c Char
     * @return A counter for finding the number of chars in a long
     */
    public static Counter counter(char c) {
        long mask = maskFor(c);
        return new Counter(mask);
    }

    /**
     * @param c Char
     * @return Finder for cycling through the occurrences in a long
     */
    public static Finder finder(char c) {
        return new Finder(maskFor(c));
    }

    private Bits() {
    }

    private static final int ALIGNMENT = 8;

    private static final long[] CLEARED = {
        0xFFFFFFFFFFFFFF00L,
        0xFFFFFFFFFFFF0000L,
        0xFFFFFFFFFF000000L,
        0xFFFFFFFF00000000L,
        0xFFFFFF0000000000L,
        0xFFFF000000000000L,
        0xFF00000000000000L,
        0x0000000000000000L
    };

    private static int count(long bytes, long mask) {
        long find = find(bytes, mask);
        int count = 0;
        while (find != 0) {
            int dist = dist(find);
            find &= CLEARED[dist];
            count++;
        }
        return count;
    }

    private static int dist(long bytes) {
        return Long.numberOfTrailingZeros(bytes) / ALIGNMENT;
    }

    private static long find(long bytes, long mask) {
        long masked = bytes ^ mask;
        long underflown = masked - 0x0101010101010101L;
        long clearedHighBits = underflown & ~masked;
        return clearedHighBits & 0x8080808080808080L;
    }

    private static long maskFor(char s) {
        long mask = s;
        for (int i = 0; i < ALIGNMENT; i++) {
            mask = (mask << ALIGNMENT) + s;
        }
        return mask;
    }

    /**
     * Returns occurrences of a byte in a long.
     */
    public static final class Finder {

        private long find;

        private final long mask;

        private Finder(long mask) {
            this.mask = mask;
        }

        /**
         * Set the given long, and return the first occurrence.  Mutates this finder.
         *
         * @param bytes Long
         * @return First occurrence
         */
        public int next(long bytes) {
            find = find(bytes, mask);
            return next();
        }

        /**
         * Retuns the next occurrence.  Mutates this finder.
         *
         * @return Next occurrence, or the number of bytes in a long (ie. 8) if done
         */
        public int next() {
            if (find == 0L) {
                return ALIGNMENT;
            }
            int dist = dist(find);
            find &= CLEARED[dist];
            return dist;
        }

        public boolean hasNext() {
            return find != 0;
        }
    }

    /**
     * Counts occurrences of a byte in a long
     */
    public static final class Counter {

        private final long mask;

        private Counter(long mask) {
            this.mask = mask;
        }

        public int count(long bytes) {
            return Bits.count(bytes, mask);
        }
    }
}
