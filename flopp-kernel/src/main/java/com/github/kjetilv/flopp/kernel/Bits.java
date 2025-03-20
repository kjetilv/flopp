package com.github.kjetilv.flopp.kernel;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.StringJoiner;

import static com.github.kjetilv.flopp.kernel.MemorySegments.ALIGNMENT_INT;
import static com.github.kjetilv.flopp.kernel.MemorySegments.ALIGNMENT_POW;

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
    public static String toString(long l, long n, Charset charset) {
        return n < 0
            ? new String(toBytes(l), charset)
            : new String(toBytes(l), 0, (int) n, charset);
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
        transferMultipleDataTo(data, bytes);
        return bytes;
    }

    /**
     * @param data long array
     * @param n    Length of byte array
     * @return n-length byte array
     */
    public static char[] toChars(long[] data, int n) {
        char[] chars = new char[n];
        transferMultipleDataTo(data, chars);
        return chars;
    }

    /**
     * @param l     Long
     * @param index Index, [0-8)
     * @return Byte at index
     */
    public static int getByte(long l, int index) {
        return ffi(l >> ALIGNMENT_INT * index);
    }

    /**
     * @param l     Long
     * @param index Index, [0-8)
     * @return Byte at index
     */
    public static int getByte(long l, long index) {
        return ffi(l >> ALIGNMENT_INT * index);
    }

    /**
     * Index of byte in long. -1 if not present.
     *
     * @param b    byte
     * @param data long
     */
    public static int indexOf(int b, long data) {
        long l = data;
        for (int i = 0; i < ALIGNMENT_INT; i++) {
            if (ffl(l) == b) {
                return i;
            }
            l >>= ALIGNMENT_INT;
        }
        return -1;
    }

    /**
     * @param l Long
     * @return Long as eight-byte array
     */
    public static byte[] toBytes(long l) {
        return new byte[] {
            ffb(l), ffb(l >> 8L), ffb(l >> 16L), ffb(l >> 24L), ffb(l >> 32L), ffb(l >> 40L), ffb(l >> 48L),
            (byte) (l >> 56L)
        };
    }

    /**
     * @param l Long
     * @return Long as eight-byte array
     */
    public static char[] toChars(long l) {
        return new char[] {
            ffc(l), ffc(l >> 8L), ffc(l >> 16L), ffc(l >> 24L), ffc(l >> 32L), ffc(l >> 40L), ffc(l >> 48L),
            (char) (l >> 56L)
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
        target[offset] = ffb(l);
        target[offset + 1] = ffb(l >> 8);
        target[offset + 2] = ffb(l >> 16);
        target[offset + 3] = ffb(l >> 24);
        target[offset + 4] = ffb(l >> 32);
        target[offset + 5] = ffb(l >> 40);
        target[offset + 6] = ffb(l >> 48);
        target[offset + 7] = (byte) (l >> 56);
    }

    /**
     * Copies long into byte array from a given offset
     *
     * @param l      Long
     * @param offset Offset in array
     * @param target Target array
     */
    public static void transferDataTo(long l, int offset, char[] target) {
        target[offset] = ffc(l);
        target[offset + 1] = ffc(l >> 8);
        target[offset + 2] = ffc(l >> 16);
        target[offset + 3] = ffc(l >> 24);
        target[offset + 4] = ffc(l >> 32);
        target[offset + 5] = ffc(l >> 40);
        target[offset + 6] = ffc(l >> 48);
        target[offset + 7] = (char) (l >> 56);
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
            target[offset + i] = ffb(l >> ALIGNMENT_INT * i);
        }
    }

    /**
     * Copies a number of bytes from long into byte array, from a given offset
     *
     * @param l      Long
     * @param offset Start position in target array
     * @param length How many bytes to move from long into array
     * @param target Target array
     */
    public static void transferLimitedDataTo(long l, int offset, int length, char[] target) {
        for (int i = 0; i < length; i++) {
            target[offset + i] = ffc(l >> ALIGNMENT_INT * i);
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
        return finder((byte) c);
    }

    /**
     * @param c Char
     * @return Finder for cycling through the occurrences in a long
     */
    public static Finder finder(byte c) {
        return finder(c, false);
    }

    /**
     * @param c Char
     * @return Finder for cycling through the occurrences in a long
     */
    public static Finder finder(char c, boolean fast) {
        return finder((byte) c, fast);
    }

    public static Finder finder(byte c, boolean fast) {
        return fast ? swarFinder(c) : cyclingFinder(c);
    }

    public static Finder cyclingFinder(byte c) {
        return new CyclingFinder(c);
    }

    public static Finder swarFinder(byte c) {
        return new SwarFinder(c);
    }

    /**
     * @param l      Long
     * @param length How many bytes to retain in the long
     * @return Truncated long
     */
    public static long clearHigh(long l, int length) {
        return l & KEEP_0[length];
    }

    public static long clearLow(long l, int bytes) {
        return l & CLEAR_0[bytes];
    }

    public static long toLong(byte[] bytes) {
        long l = 0L;
        for (int i = 0; i < bytes.length; i++) {
            l |= ffl(bytes[i]) << ALIGNMENT_INT * i;
        }
        return l;
    }

    public static long repeatByte(int i) {
        return
            (ffl(i) << 56L) +
            (ffl(i) << 48L) +
            (ffl(i) << 40L) +
            (ffl(i) << 32L) +
            (ffl(i) << 24L) +
            (ffl(i) << 16L) +
            (ffl(i) << 8L) +
            ffl(i);
    }

    private Bits() {
    }

    private static final long EIGHTIES = 0x8080808080808080L;

    private static final long F7 = 0x7F7F7F7F7F7F7F7FL;

    private static final long ONES = 0x0101010101010101L;

    private static final long[] CLEAR_0 = {
        0xFFFFFFFFFFFFFFFFL,
        0xFFFFFFFFFFFFFF00L,
        0xFFFFFFFFFFFF0000L,
        0xFFFFFFFFFF000000L,
        0xFFFFFFFF00000000L,
        0xFFFFFF0000000000L,
        0xFFFF000000000000L,
        0xFF00000000000000L,
        0x0000000000000000L,
    };

    private static final long[] CLEAR_1 = {
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

    private static final long[] KEEP_0 = {
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

    private static char ffc(long l) {
        return (char) ffl(l);
    }

    private static int ffi(long l) {
        return (int) ffl(l);
    }

    private static byte ffb(long l) {
        return (byte) ffl(l);
    }

    private static long ffl(long l) {
        return l & 0xFFL;
    }

    @SuppressWarnings("DuplicatedCode")
    private static void transferMultipleDataTo(long[] data, byte[] target) {
        int length = target.length;
        int position = 0;
        int longCount = length >> ALIGNMENT_POW;
        for (int l = 0; l < longCount; l++) {
            transferDataTo(data[l], position, target);
            position += ALIGNMENT_INT;
        }
        int remainder = length - position;
        if (remainder > 0) {
            transferLimitedDataTo(data[longCount], position, remainder, target);
        }
    }

    @SuppressWarnings("DuplicatedCode")
    private static void transferMultipleDataTo(long[] data, char[] target) {
        int length = target.length;
        int position = 0;
        int longCount = length >> ALIGNMENT_POW;
        for (int l = 0; l < longCount; l++) {
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
        return Long.numberOfTrailingZeros(l) >> ALIGNMENT_POW;
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

    /**
     * Returns occurrences of a byte in a long.
     */
    private static final class SwarFinder implements Finder {

        private final long mask;

        private long dists;

        private SwarFinder(byte c) {
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
         * Returns the next occurrence.  Mutates this finder.
         *
         * @return Next occurrence, or the number of bytes in a long (ie. 8) if done
         */
        @Override
        public int next() {
            int trail = trailingBytes(dists);
            dists &= CLEAR_1[trail];
            return trail;
        }

        @Override
        public boolean hasNext() {
            return dists != 0;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "['" + ffc(mask) + "' / " + hex(dists) + "]";
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
         * Returns the next occurrence.  Mutates this finder.
         *
         * @return Next occurrence, or the number of bytes in a long (ie. 8) if done
         */
        @Override
        public int next() {
            if (dists == 0L) {
                return ALIGNMENT_INT;
            }
            trail = trailingBytes(dists);
            dists &= CLEAR_1[trail];
            return trail;
        }

        @Override
        public boolean hasNext() {
            return dists != 0;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "['" + ffc(mask) + "' / " +
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

        private CyclingFinder(byte c) {
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
            if (hasZero()) {
                return next();
            }
            return ALIGNMENT_INT;
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

        private boolean hasZero() {
            return ~((dists & F7) + F7 | dists | F7) != 0x0L;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "['" + ffc(mask) + "'/" +
                   hex(mask) + " " +
                   hex(dists) +
                   "]";
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
                find &= CLEAR_1[trailingBytes(find)];
                count++;
            }
            return count;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[" + ffc(mask) + "]";
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
