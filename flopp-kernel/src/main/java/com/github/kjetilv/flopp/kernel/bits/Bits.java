package com.github.kjetilv.flopp.kernel.bits;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;

import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_INT;
import static com.github.kjetilv.flopp.kernel.bits.MemorySegments.ALIGNMENT_POW;

@SuppressWarnings("unused")
public final class Bits {

    /**
     * @param l Eight bytes
     * @return Eight-byte string in {@link Charset#defaultCharset() default charset}
     */
    public static String toString(long l) {
        return toString(l, Charset.defaultCharset());
    }

    /**
     * @param l       Eight bytes
     * @param charset Charset
     * @return Eight-byte string in charset
     */
    public static String toString(long l, Charset charset) {
        return toString(l, -1, charset);
    }

    /**
     * @param l       Eight bytes
     * @param n       Length n
     * @param charset Charset
     * @return n-byte string in charset
     */
    public static String toString(long l, int n, Charset charset) {
        return n < 0
            ? new String(toBytes(l), charset)
            : new String(toBytes(l), 0, n, charset);
    }

    /**
     * @param l long
     * @return Byte-wise hex string, dotted: {@code 0x01.23.45.67.89.AB.CD.EF}
     */
    public static String hxD(long l) {
        return hex(l, true);
    }

    /**
     * @param l long
     * @return Byte-wise hex string, possibly dotted: {@code 0x01.23.45.67.89.AB.CD.EF}
     */
    public static String hex(long l, boolean dotted) {
        String hexString = hex(l);
        String paddedHexString = zeroPad(hexString, 16);
        return "0x" + (dotted ? dot(paddedHexString, 2) : hexString);
    }

    /**
     * @param l long
     * @return Byte-wise hex string: {@code 0x0123456789ABCDEF}
     */
    public static String hex(long l) {
        return String.format("%08X", l);
    }

    /**
     * @param l Long
     * @return 64-char string with 0's and 1's
     */
    public static String bin(long l) {
        return bin(l, false);
    }

    /**
     * @param l Long
     * @return 71-char string with 0's and 1's, with comely dots between every eight bits
     */
    public static String bnD(long l) {
        return bin(l, true);
    }

    /**
     * @param l Long
     * @return String with 0's and 1's, possibly dotted
     * @see #bnD(long)
     * @see #bin(long)
     */
    public static String bin(long l, boolean dotted) {
        if (l == 0L) {
            return "0x0";
        }
        String padded = zeroPad(Long.toBinaryString(l), 64);
        return dotted ? dot(padded, 8) : padded;
    }

    /**
     * @param data long array
     * @param n    Length of byte array
     * @return n-length byte array
     */
    public static byte[] toBytes(long[] data, int n) {
        byte[] bytes = new byte[n];
        transferMultipleDataTo(data, 0, bytes);
        return bytes;
    }

    /**
     * @param l     Long
     * @param index Index, 0-7
     * @return Byte at index
     */
    public static byte getByte(long l, int index) {
        return (byte) (l >> ALIGNMENT_INT * index & 0xFF);
    }

    /**
     * @param l Long
     * @return Long as eigth-byte array
     */
    public static byte[] toBytes(long l) {
        return new byte[] {
            (byte) (l & 0xFF),
            (byte) (l >> 8L & 0xFF),
            (byte) (l >> 16L & 0xFF),
            (byte) (l >> 24L & 0xFF),
            (byte) (l >> 32L & 0xFF),
            (byte) (l >> 40L & 0xFF),
            (byte) (l >> 48L & 0xFF),
            (byte) (l >> 56L)
        };
    }

    /**
     * Copies long into byte array from a given offset
     *
     * @param l      Long
     * @param offset Offset in array
     * @param target Target array
     */
    public static void transferDataTo(long l, int offset, byte[] target) {
        target[offset] = (byte) (l & 0xFF);
        target[offset + 1] = (byte) (l >> 8 & 0xFF);
        target[offset + 2] = (byte) (l >> 16 & 0xFF);
        target[offset + 3] = (byte) (l >> 24 & 0xFF);
        target[offset + 4] = (byte) (l >> 32 & 0xFF);
        target[offset + 5] = (byte) (l >> 40 & 0xFF);
        target[offset + 6] = (byte) (l >> 48 & 0xFF);
        target[offset + 7] = (byte) (l >> 56);
    }

    /**
     * Copies a number of bytes from long into byte array, from a given offset
     *
     * @param l      Long
     * @param offset Start position in target array
     * @param length How many bytes to move from long into array
     * @param target Target array
     */
    public static void transferLimitedDataTo(long l, int offset, int length, byte[] target) {
        for (int i = 0; i < length; i++) {
            target[offset + i] = (byte) (l >> ALIGNMENT_INT * i & 0xFF);
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

    /**
     * @param l      Long
     * @param length How many bytes to retain in the long
     * @return Truncated long
     */
    public static long truncate(long l, long length) {
        return truncate(l, (int) length);
    }

    /**
     * @param l      Long
     * @param length How many bytes to retain in the long
     * @return Truncated long
     */
    public static long truncate(long l, int length) {
        return l & KEEP[length];
    }

    private Bits() {
    }

    private static final long EIGHTIES = 0x8080808080808080L;

    private static final long SEVEN_EFFS = 0x7F7F7F7F7F7F7F7FL;

    private static final long ONES = 0x0101010101010101L;

    private static final long[] CLEAR = {
        0xFFFFFFFFFFFFFF00L,
        0xFFFFFFFFFFFF0000L,
        0xFFFFFFFFFF000000L,
        0xFFFFFFFF00000000L,
        0xFFFFFF0000000000L,
        0xFFFF000000000000L,
        0xFF00000000000000L,
        0x0000000000000000L,
        0x0000000000000000L
    };

    private static final long[] KEEP = {
        0x0000000000000000L,
        0x00000000000000FFL,
        0x000000000000FFFFL,
        0x0000000000FFFFFFL,
        0x00000000FFFFFFFFL,
        0x000000FFFFFFFFFFL,
        0x0000FFFFFFFFFFFFL,
        0x00FFFFFFFFFFFFFFL,
        0xFFFFFFFFFFFFFFFFL
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

    private static void transferMultipleDataTo(long[] data, int offset, byte[] target) {
        int headStart = offset % ALIGNMENT_INT;
        int headLen = ALIGNMENT_INT - headStart;
        int length = target.length;
        int firstLong;
        int longCount;
        int position = 0;
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

    private static String dot(String hex, int interval) {
        int len = hex.length();
        if (len % interval == 0) {
            StringJoiner joiner = new StringJoiner(".");
            for (int i = 0; i < len / interval; i++) {
                joiner.add(hex.substring(i * interval, i * interval + interval));
            }
            return joiner.toString();
        }
        throw new IllegalArgumentException("Not an x" + interval + " string (" + len + "): " + hex);
    }

    private static String zeroPad(String binaryString, int paddedLength) {
        int length = binaryString.length();
        if (length >= paddedLength) {
            return binaryString;
        }
        return "0".repeat(paddedLength - length) + binaryString;
    }

    private static int trailingBytes(long l) {
        int trailingZeros = Long.numberOfTrailingZeros(l);
        return trailingZeros >> ALIGNMENT_POW;
    }

    private static boolean zero(long l, int position) {
        return (l & ZERO_CHECK[position]) == 0x00;
    }

    private static long findInstances(long data, long mask) {
        long masked = data ^ mask;
        long underflown = masked - ONES;
        long clearedHighBits = underflown & ~masked;
        return clearedHighBits & EIGHTIES;
    }

    private static boolean hasZero(long l) {
        return ~((l & SEVEN_EFFS) + SEVEN_EFFS | l | SEVEN_EFFS) != 0x0L;
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
            throw new IllegalStateException(this + " not empty: " + hxD(data));
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

        private CyclingFinder(char c) {
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
                    if (zero(dists, offset)) {
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
                if (zero(dists, offset)) {
                    return true;
                } else {
                    offset++;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "['" + (char) (mask & 0xFF) + "'/" + hex(mask) + " " + hex(dists) + "]";
        }
    }

    /**
     * Counts occurrences of a byte in a long
     */
    private static final class SimpleCounter implements Counter {

        private final long mask;

        private SimpleCounter(char c) {
            this.mask = ONES * c;
        }

        @Override
        public int count(long data) {
            int count = 0;
            long find = findInstances(data, mask);
            while (find != 0) {
                find &= CLEAR[trailingBytes(find)];
                count++;
            }
            return count;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + (char) (mask & 0xFF) + "]";
        }
    }

    public interface Finder {

        int next(long data);

        int next();

        boolean hasNext();
    }

    public interface Counter {

        int count(long data);
    }
}
