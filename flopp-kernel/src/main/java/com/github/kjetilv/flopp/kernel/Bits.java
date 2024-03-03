package com.github.kjetilv.flopp.kernel;

public final class Bits {

    public static Counter counter(char c) {
        long mask = createMask(c);
        return new BytesCounter(mask);
    }

    public static Finder finder(char c) {
        return new ByteFinder(c);
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

    private static long createMask(char s) {
        long mask = s;
        for (int i = 0; i < ALIGNMENT; i++) {
            mask = (mask << ALIGNMENT) + s;
        }
        return mask;
    }

    private static final class ByteFinder implements Finder {

        private long find;

        private final long mask;

        private ByteFinder(char c) {
            this.mask = createMask(c);
        }

        @Override
        public int next(long bytes) {
            find = find(bytes, mask);
            return next();
        }

        @Override
        public int next() {
            if (find == 0L) {
                return ALIGNMENT;
            }
            int dist = dist(find);
            find &= CLEARED[dist];
            return dist;
        }

        @Override
        public boolean hasNext() {
            return find != 0;
        }
    }

    private static final class BytesCounter implements Counter {

        private final long mask;

        private BytesCounter(long mask) {
            this.mask = mask;
        }

        @Override
        public int count(long bytes) {
            return Bits.count(bytes, mask);
        }
    }

    public sealed interface Counter {

        int count(long bytes);
    }

    public sealed interface Finder {

        int next(long bytes);

        int next();

        boolean hasNext();
    }
}
