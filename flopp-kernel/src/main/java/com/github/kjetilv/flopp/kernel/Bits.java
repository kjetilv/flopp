package com.github.kjetilv.flopp.kernel;

public final class Bits {

    public static Counter counter(char c) {
        long mask = createMask(c);
        return bytes ->
            count(bytes, mask);
    }

    public static Finder finder(long bytes, char c) {
        return new ByteFinder(bytes, createMask(c));
    }

    public static int countOccurrences(long bytes, char c) {
        return count(bytes, createMask(c));
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
        long occurrences = occurrences(bytes, mask);
        int count = 0;
        while (occurrences != 0) {
            int dist = dist(occurrences);
            occurrences &= CLEARED[dist];
            count++;
        }
        return count;
    }

    private static int dist(long bytes) {
        return Long.numberOfTrailingZeros(bytes) / ALIGNMENT;
    }

    private static long occurrences(long bytes, long mask) {
        long masked = bytes ^ mask;
        long underflown = masked - 0x0101010101010101L;
        long clearedHighBits = underflown & ~masked;
        return clearedHighBits & 0x8080808080808080L;
    }

    private static long createMask(char s) {
        long mask = s;
        for (int i = 0; i < ALIGNMENT; i++) {
            mask = (mask << ALIGNMENT) + s;
        }
        return mask;
    }

    private static final class ByteFinder implements Finder {

        private long occurrences;

        private ByteFinder(long bytes, long mask) {
            occurrences = occurrences(bytes, mask);
        }

        @Override
        public int next() {
            int dist = dist(occurrences);
            occurrences &= CLEARED[dist];
            return dist;
        }

        @Override
        public boolean hasNext() {
            return occurrences != 0;
        }
    }

    public interface Counter {

        int count(long bytes);
    }

    public interface Finder {

        int next();

        boolean hasNext();
    }
}
