package com.github.kjetilv.flopp.kernel.bits;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_POW;

@SuppressWarnings("unused")
public final class Bits {

    public static String toString(long l, Charset charset) {
        return toString(l, -1, charset);
    }

    public static String toString(long l, int len, Charset charset) {
        return len < 0
            ? new String(toBytes(l), charset)
            : new String(toBytes(l), 0, len, charset);
    }

    public static String hex(long mask) {
        return hex(mask, false);
    }

    public static String hxD(long mask) {
        return hex(mask, true);
    }

    public static String hex(long mask, boolean dotted) {
        String hexString = String.format("%08X", mask);
        String paddedHexString = zeroPad(hexString, 16);
        return "0x" + (dotted ? dot(paddedHexString, 2) : hexString);
    }

    public static String bin(long mask) {
        return bin(mask, false);
    }

    public static String bnD(long mask) {
        return bin(mask, true);
    }

    public static String bin(long mask, boolean dotted) {
        if (mask == 0L) {
            return "0x0";
        }
        String padded = zeroPad(Long.toBinaryString(mask), 64);
        return dotted ? dot(padded, 8) : padded;
    }

    public static byte[] toBytes(long data) {
        return new byte[] {
            (byte) (data & 0xFF),
            (byte) (data >> 8L & 0xFF),
            (byte) (data >> 16L & 0xFF),
            (byte) (data >> 24L & 0xFF),
            (byte) (data >> 32L & 0xFF),
            (byte) (data >> 40L & 0xFF),
            (byte) (data >> 48L & 0xFF),
            (byte) (data >> 56L)
        };
    }

    @SuppressWarnings("DuplicatedCode")
    public static void transferMultipleDataTo(long[] data, int offset, byte[] target) {
        int firstLong;
        int longCount;
        int headStart = offset % ALIGNMENT_INT;
        int headLen = ALIGNMENT_INT - headStart;
        int position = 0;
        int length = target.length;
        if (offset > 0) {
            transferLimitedDataTo(data[0], 0, headLen, target);
            firstLong = 1;
            longCount = length - headLen >> ALIGNMENT_POW;
            position = headLen;
        } else {
            firstLong = 0;
            longCount = length >> ALIGNMENT_POW;
        }
        for (int l = firstLong; l < longCount; l++) {
            transferDataTo(data[l], position, target);
            position += ALIGNMENT_INT;
        }
        int remainder = length - position;
        if (remainder > 0) {
            transferLimitedDataTo(data[longCount], position, remainder, target);
        }
    }

    public static void transferDataTo(long data, int offset, byte[] target) {
        target[offset] = (byte) (data & 0xFF);
        target[offset + 1] = (byte) (data >> 8 & 0xFF);
        target[offset + 2] = (byte) (data >> 16 & 0xFF);
        target[offset + 3] = (byte) (data >> 24 & 0xFF);
        target[offset + 4] = (byte) (data >> 32 & 0xFF);
        target[offset + 5] = (byte) (data >> 40 & 0xFF);
        target[offset + 6] = (byte) (data >> 48 & 0xFF);
        target[offset + 7] = (byte) (data >> 56);
    }

    /**
     * @param data   Source long
     * @param offset Start position in target array
     * @param length How many bytes to move from long into array
     * @param target Array
     */
    public static void transferLimitedDataTo(long data, int offset, int length, byte[] target) {
        for (int i = 0; i < length; i++) {
            target[offset + i] = (byte) (data >> ALIGNMENT_INT * i & 0xFF);
        }
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
        return finder(c, false);
    }

    /**
     * @param c Char
     * @return Finder for cycling through the occurrences in a long
     */
    public static Finder finder(char c, boolean fast) {
        return fast ? swarFinder(c) : cyclingFinder(c);
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

    public static final long SEVEN_EFFS = 0x7F7F7F7F7F7F7F7FL;

    private static final long ONES = 0x0101010101010101L;

    private static final long[] CLEAR = {
        0xFFFFFFFFFFFFFF00L,
        0xFFFFFFFFFFFF0000L,
        0xFFFFFFFFFF000000L,
        0xFFFFFFFF00000000L,
        0xFFFFFF0000000000L,
        0xFFFF000000000000L,
        0xFF00000000000000L,
        0x0000000000000000L
    };

    private static final long[] ZERO_CHECK = {
        0x00000000000000FFL,
        0x000000000000FF00L,
        0x0000000000FF0000L,
        0x00000000FF000000L,
        0x000000FF00000000L,
        0x0000FF0000000000L,
        0x00FF000000000000L,
        0xFF00000000000000L
    };

    private static String dot(String hexString, int interval) {
        int len = hexString.length();
        if (len % interval == 0) {
            return IntStream.range(0, len / interval)
                .mapToObj(i -> hexString.substring(i * interval, i * interval + interval))
                .collect(Collectors.joining("."));
        }
        throw new IllegalArgumentException("Not an x" + interval + " string (" + len + "): " + hexString);
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

    private static int trailingBytes(long data) {
        return Long.numberOfTrailingZeros(data) >> ALIGNMENT_POW;
    }

    private static boolean zero(int position, long bytes) {
        return (bytes & ZERO_CHECK[position]) == 0x00;
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

        private final long mask;

        private long dists;

        private SwarFinder(char c) {
            this.mask = ONES * c;
        }

        /**
         * Set the given long, and return the first occurrence.  Mutates this finder.
         *
         * @param data Long
         * @return First occurrence
         */
        @Override
        public int next(long data) {
            dists = findInstances(data, mask);
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
                return ALIGNMENT_INT;
            }
            int trail = trailingBytes(dists);
            dists &= CLEAR[trail];
            return trail;
        }

        @Override
        public boolean hasNext() {
            return dists != 0;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "['" + (char) (mask & 0xFF) + "' / " + hex(dists) + "]";
        }
    }

    private static final class DevSwarFinder implements Finder {

        private final long mask;

        private long dists;

        private long data;

        private int trail;

        private DevSwarFinder(char c) {
            this.mask = ONES * c;
        }

        /**
         * Set the given long, and return the first occurrence.  Mutates this finder.
         *
         * @param data Long
         * @return First occurrence
         */
        @Override
        public int next(long data) {
            if (this.dists == 0L) {
                this.data = data;
                this.dists = findInstances(data, mask);
                return next();
            }
            throw new IllegalStateException(this + " not empty: " + Bits.hxD(data));
        }

        /**
         * Retuns the next occurrence.  Mutates this finder.
         *
         * @return Next occurrence, or the number of bytes in a long (ie. 8) if done
         */
        @Override
        public int next() {
            if (dists == 0L) {
                return ALIGNMENT_INT;
            }
            trail = trailingBytes(dists);
            dists &= CLEAR[trail];
            return trail;
        }

        @Override
        public boolean hasNext() {
            return dists != 0;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "['" + (char) (mask & 0xFF) + "' / " +
                   trail + "@" + hxD(data) + "/'" + Bits.toString(data, StandardCharsets.UTF_8) + "' : " +
                   hxD(dists) + "]";
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
         * @param data Long
         * @return First occurrence
         */
        @Override
        public int next(long data) {
            offset = 0;
            dists = data ^ mask;
            return hasZero(dists)
                ? next()
                : (offset = ALIGNMENT_INT);
        }

        /**
         * Retuns the next occurrence.  Mutates this finder.
         *
         * @return Next occurrence, or the number of bytes in a long (ie. 8) if done
         */
        @Override
        public int next() {
            while (offset < ALIGNMENT_INT) {
                try {
                    if (zero(offset, dists)) {
                        return offset;
                    }
                } finally {
                    offset++;
                }
            }
            return ALIGNMENT_INT;
        }

        @Override
        public boolean hasNext() {
            while (offset < ALIGNMENT_INT) {
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
            int count = 0;
            long find = findInstances(bytes, mask);
            while (find != 0) {
                find &= CLEAR[trailingBytes(find)];
                count++;
            }
            return count;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + c + "]";
        }
    }

    public interface Finder {

        int next(long data);

        int next();

        boolean hasNext();
    }

    public interface Counter {

        int count(long bytes);
    }
}
