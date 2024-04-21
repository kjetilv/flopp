package com.github.kjetilv.flopp.kernel.bits;

import java.nio.charset.Charset;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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

    public static String hex(long mask) {
        return hex(mask, false);
    }

    public static String hex(long mask, boolean dotted) {
        if (mask == 0) {
            return "0x0";
        }
        String hexString = String.format("%08X", mask);
        String padded = zeroPad(hexString, 16);
        return "0x" + (dotted ? dot(padded, 2) : hexString);
    }

    public static String bin(long mask) {
        return bin(mask, false);
    }

    public static String bin(long mask, boolean dotted) {
        if (mask == 0) {
            return "0x0";
        }
        String padded = zeroPad(Long.toBinaryString(mask), 64);
        return dotted ? dot(padded, 8) : padded;
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
        return new SimpleCounter(c);
    }

    /**
     * @param c Char
     * @return Finder for cycling through the occurrences in a long
     */
    public static Finder finder(char c) {
        return new CyclingFinder(c);
    }

    public static Finder cyclingFinder(char c) {
        return new CyclingFinder(c);
    }

    public static Finder swarFinder(char c) {
        return new SwarFinder(c);
    }

    private Bits() {
    }

    public static final long EIGHTIES = 0x8080808080808080L;

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

    private static final long[] CHECK = {
        0x00000000000000FFL,
        0x000000000000FF00L,
        0x0000000000FF0000L,
        0x00000000FF000000L,
        0x000000FF00000000L,
        0x0000FF0000000000L,
        0x00FF000000000000L,
        0xFF00000000000000L
    };

    private static final long ZION_80 = 0x8080808080808080L;

    private static final long UNDERTAKER = 0x0101010101010101L;

    private static final long SEPARATOR_XOR_MASK = 0X3B3B3B3B3B3B3B3BL;

    private static final long ONES = 0x0101010101010101L;

    public static final long SEVEN_EFFS = 0x7F7F7F7F7F7F7F7FL;

    private static String dot(String s, int interval) {
        int len = s.length();
        if (len % interval == 0) {
            return IntStream.range(0, len / interval)
                .mapToObj(i -> s.substring(i * interval, i * interval + interval))
                .collect(Collectors.joining("."));
        }
        throw new IllegalArgumentException("Not an x" + interval + " string (" + len + "): " + s);
    }

    private static String zeroPad(String binaryString, int l) {
        int length = binaryString.length();
        if (length < l) {
            String padding = IntStream.range(0, l - length).mapToObj(_ -> "0")
                .collect(Collectors.joining());
            return padding + binaryString;
        }
        return binaryString;
    }

    private static int count(long bytes, long mask) {
        long find = findInstances(bytes, mask);
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

    private static boolean zero(int position, long bytes) {
        return (bytes & CHECK[position]) == 0x00;
    }

    private static long findInstances(long bytes, long mask) {
        long masked = bytes ^ mask;
        long underflown = masked - ONES;
        long clearedHighBits = underflown & ~masked;
        return clearedHighBits & EIGHTIES;
    }

    private static boolean hasZero(long bytes) {
        return ~((bytes & SEVEN_EFFS) + SEVEN_EFFS | bytes | SEVEN_EFFS) != 0x0L;
    }

    /**
     * Returns occurrences of a byte in a long.
     */
    private static final class SwarFinder implements Finder {

        private long dists;

        private final long mask;

        private final char c;

        private SwarFinder(char c) {
            this.c = c;
            this.mask = ONES * this.c;
        }

        /**
         * Set the given long, and return the first occurrence.  Mutates this finder.
         *
         * @param bytes Long
         * @return First occurrence
         */
        @Override
        public int next(long bytes) {
            dists = findInstances(bytes, mask);
            return next();
        }

        /**
         * Retuns the next occurrence.  Mutates this finder.
         *
         * @return Next occurrence, or the number of bytes in a long (ie. 8) if done
         */
        @Override
        public int next() {
            if (dists == 0L) {
                return ALIGNMENT;
            }
            int dist = dist(dists);
            dists &= CLEARED[dist];
            return dist;
        }

        @Override
        public boolean hasNext() {
            return dists != 0;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "['" + c + "'/" + hex(mask) + " " + hex(dists) + "]";
        }
    }

    /**
     * Returns occurrences of a byte in a long.
     */
    private static final class CyclingFinder implements Finder {

        private long dists;

        private int offset;

        private final long mask;

        private final char c;

        private CyclingFinder(char c) {
            this.c = c;
            this.mask = ONES * this.c;
        }

        /**
         * Set the given long, and return the first occurrence.  Mutates this finder.
         *
         * @param bytes Long
         * @return First occurrence
         */
        @Override
        public int next(long bytes) {
            offset = 0;
            dists = bytes ^ mask;
            return hasZero(dists)
                ? next()
                : (offset = ALIGNMENT);
        }


        /**
         * Retuns the next occurrence.  Mutates this finder.
         *
         * @return Next occurrence, or the number of bytes in a long (ie. 8) if done
         */
        @Override
        public int next() {
            while (offset < ALIGNMENT) {
                try {
                    if (zero(offset, dists)) {
                        return offset;
                    }
                } finally {
                    offset++;
                }
            }
            return offset;
        }

        @Override
        public boolean hasNext() {
            while (offset < ALIGNMENT) {
                if (zero(offset, dists)) {
                    return true;
                } else {
                    offset++;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "['" + c + "'/" + hex(mask) + " " + hex(dists) + "]";
        }
    }

    /**
     * Counts occurrences of a byte in a long
     */
    private static final class SimpleCounter implements Counter {

        private final long mask;

        private final char c;

        private SimpleCounter(char c) {
            this.c = c;
            this.mask = ONES * this.c;
        }

        @Override
        public int count(long bytes) {
            return Bits.count(bytes, mask);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + c + "]";
        }
    }

    public interface Finder {

        int next(long bytes);

        int next();

        boolean hasNext();
    }

    public interface Counter {

        int count(long bytes);
    }
}
